/*
 * Copyright (c) 2024 General Motors GTO LLC
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * SPDX-FileType: SOURCE
 * SPDX-FileCopyrightText: 2024 General Motors GTO LLC
 * SPDX-License-Identifier: Apache-2.0
 */

package org.eclipse.uprotocol.core.udiscovery;

import static org.eclipse.uprotocol.common.util.UStatusUtils.checkArgument;
import static org.eclipse.uprotocol.common.util.UStatusUtils.isOk;
import static org.eclipse.uprotocol.common.util.UStatusUtils.toStatus;
import static org.eclipse.uprotocol.common.util.log.Formatter.join;
import static org.eclipse.uprotocol.common.util.log.Formatter.stringify;
import static org.eclipse.uprotocol.common.util.log.Formatter.tag;
import static org.eclipse.uprotocol.core.udiscovery.internal.Utils.logStatus;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_ADD_NODES;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_DELETE_NODES;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_FIND_NODES;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_FIND_NODE_PROPERTIES;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_LOOKUP_URI;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_REGISTER_FOR_NOTIFICATIONS;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_UNREGISTER_FOR_NOTIFICATIONS;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_UPDATE_NODE;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_UPDATE_PROPERTY;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.SERVICE;
import static org.eclipse.uprotocol.transport.builder.UPayloadBuilder.packToAny;

import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.Service;
import android.content.Intent;
import android.os.Binder;
import android.os.IBinder;
import android.util.Log;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.VisibleForTesting;

import org.eclipse.uprotocol.UPClient;
import org.eclipse.uprotocol.common.UStatusException;
import org.eclipse.uprotocol.common.util.log.Key;
import org.eclipse.uprotocol.core.udiscovery.db.DiscoveryManager;
import org.eclipse.uprotocol.core.udiscovery.v3.FindNodePropertiesResponse;
import org.eclipse.uprotocol.core.udiscovery.v3.FindNodesResponse;
import org.eclipse.uprotocol.core.udiscovery.v3.LookupUriResponse;
import org.eclipse.uprotocol.transport.UListener;
import org.eclipse.uprotocol.transport.builder.UAttributesBuilder;
import org.eclipse.uprotocol.uri.factory.UResourceBuilder;
import org.eclipse.uprotocol.v1.UCode;
import org.eclipse.uprotocol.v1.UMessage;
import org.eclipse.uprotocol.v1.UPayload;
import org.eclipse.uprotocol.v1.UStatus;
import org.eclipse.uprotocol.v1.UUri;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * This service is responsible for handling various methods related to the UProtocol discovery process.
 * It manages the lifecycle of the UPClient and handles RPC requests.
 * <p>
 * The service is also responsible for managing the database initialization, registering and unregistering methods,
 * and handling request events. It also manages the lifecycle of the UPClient and handles the connection status.
 * <p>
 * The service is started in the foreground to ensure it keeps running even when the app is not in the foreground.
 * <p>
 * This class is part of the UProtocol core discovery package.
 */
@SuppressWarnings({"java:S1200", "java:S3008", "java:S1134"})
public class UDiscoveryService extends Service {
    public static final String TAG = tag(SERVICE.getName());
    private static final CharSequence NOTIFICATION_CHANNEL_NAME = TAG;
    private static final int NOTIFICATION_ID = 1;
    private static final String DATABASE_NOT_INITIALIZED = "Database not initialized";
    private static final String NOTIFICATION_CHANNEL_ID = TAG;
    protected static boolean VERBOSE = Log.isLoggable(TAG, Log.VERBOSE);
    protected static boolean DEBUG = Log.isLoggable(TAG, Log.DEBUG);
    private final ScheduledExecutorService mExecutor = Executors.newScheduledThreadPool(1);
    private final Map<UUri, Consumer<UMessage>> mMethodHandlers = new HashMap<>();
    private final UListener mRequestListener = this::handleRequest;
    private final Binder mBinder = new Binder() {
    };
    private final AtomicBoolean mDatabaseInitialized = new AtomicBoolean(false);
    private RPCHandler mRpcHandler;
    private ResourceLoader mResourceLoader;
    private UPClient mUpClient;

    // This constructor is for service initialization
    // without this constructor service won't start.
    @SuppressWarnings("unused")
    public UDiscoveryService() {
    }

    @VisibleForTesting
    UDiscoveryService(RPCHandler rpcHandler, UPClient upClient, ResourceLoader resourceLoader) {
        mRpcHandler = rpcHandler;
        mUpClient = upClient;
        mResourceLoader = resourceLoader;
        init().join();
    }

    @Override
    public @Nullable IBinder onBind(@NonNull Intent intent) {
        if (DEBUG) {
            Log.d(TAG, join(Key.METHOD, "onBind"));
        }
        return mBinder;
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public void onCreate() {
        super.onCreate();
        if (DEBUG) {
            Log.d(TAG, join(Key.METHOD, "onCreate"));
        }
        startForegroundService();
        mUpClient = UPClient.create(getApplicationContext(), SERVICE, mExecutor, (client, ready) -> {
            if (ready) {
                Log.i(TAG, join(Key.EVENT, "uPClient connected"));
            } else {
                Log.w(TAG, join(Key.EVENT, "uPClient unexpectedly disconnected"));
            }
        });
        ObserverManager observerManager = new ObserverManager(this);
        Notifier notifier = new Notifier(observerManager, mUpClient);
        DiscoveryManager discoveryManager = new DiscoveryManager(notifier);
        AssetManager assetManager = new AssetManager();
        mResourceLoader = new ResourceLoader(this, assetManager, discoveryManager);
        mRpcHandler = new RPCHandler(this, assetManager, discoveryManager, observerManager);
        init();
    }

    private void startForegroundService() {
        if (DEBUG) {
            Log.d(TAG, join(Key.METHOD, "startForegroundService"));
        }
        final NotificationChannel channel = new NotificationChannel(NOTIFICATION_CHANNEL_ID,
                NOTIFICATION_CHANNEL_NAME,
                NotificationManager.IMPORTANCE_NONE);
        final NotificationManager manager =
                (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        manager.createNotificationChannel(channel);
        android.app.Notification.Builder notificationBuilder = new android.app.Notification.Builder(this,
                NOTIFICATION_CHANNEL_ID);
        android.app.Notification notification = notificationBuilder.setOngoing(true)
                .setSmallIcon(R.mipmap.ic_launcher)
                .setContentTitle(getApplicationContext().getResources().getString(
                        R.string.notification_title))
                .setCategory(android.app.Notification.CATEGORY_SERVICE)
                .build();
        startForeground(NOTIFICATION_ID, notification);
    }

    private synchronized CompletableFuture<Void> init() {
        return mUpClient.connect()
                .thenCompose(status -> {
                    logStatus(TAG, "connect", status);
                    return isOk(status) ?
                            CompletableFuture.completedFuture(status) :
                            CompletableFuture.failedFuture(new UStatusException(status));
                }).thenRunAsync(() -> {
                    ResourceLoader.InitLDSCode code = mResourceLoader.initializeLDS();
                    boolean isInitialized = (code != ResourceLoader.InitLDSCode.FAILURE);
                    mDatabaseInitialized.set(isInitialized);
                    if (mUpClient.isConnected()) {
                        registerAllMethods();
                    }
                }).toCompletableFuture();
    }

    private void registerAllMethods() {
        CompletableFuture.allOf(
                        registerMethod(METHOD_LOOKUP_URI, this::lookupUri),
                        registerMethod(METHOD_FIND_NODES, this::findNodes),
                        registerMethod(METHOD_UPDATE_NODE, this::updateNode),
                        registerMethod(METHOD_FIND_NODE_PROPERTIES, this::findNodesProperty),
                        registerMethod(METHOD_ADD_NODES, this::addNodes),
                        registerMethod(METHOD_DELETE_NODES, this::deleteNodes),
                        registerMethod(METHOD_UPDATE_PROPERTY, this::updateProperty),
                        registerMethod(METHOD_REGISTER_FOR_NOTIFICATIONS, this::registerNotification),
                        registerMethod(METHOD_UNREGISTER_FOR_NOTIFICATIONS, this::unregisterNotification))
                .exceptionally(e -> {
                    logStatus(TAG, "registerAllMethods", toStatus(e));
                    return null;
                });
    }

    private void lookupUri(@NonNull UMessage requestMessage) {
        try {
            checkArgument(mDatabaseInitialized.get(), UCode.FAILED_PRECONDITION, DATABASE_NOT_INITIALIZED);
            sendResponse(requestMessage, mRpcHandler.processLookupUriFromLDS(requestMessage));
        } catch (Exception e) {
            final UStatus status = logStatus(TAG, METHOD_LOOKUP_URI, toStatus(e));
            sendResponse(requestMessage, packToAny(LookupUriResponse.newBuilder().setStatus(status).build()));
        }
    }

    private void findNodes(@NonNull UMessage requestMessage) {
        try {
            checkArgument(mDatabaseInitialized.get(), UCode.FAILED_PRECONDITION, DATABASE_NOT_INITIALIZED);
            sendResponse(requestMessage, mRpcHandler.processFindNodesFromLDS(requestMessage));
        } catch (Exception e) {
            final UStatus status = logStatus(TAG, METHOD_FIND_NODES, toStatus(e));
            sendResponse(requestMessage, packToAny(FindNodesResponse.newBuilder().setStatus(status).build()));
        }
    }

    private void updateNode(@NonNull UMessage requestMessage) {
        try {
            checkArgument(mDatabaseInitialized.get(), UCode.FAILED_PRECONDITION, DATABASE_NOT_INITIALIZED);
            sendResponse(requestMessage, mRpcHandler.processLDSUpdateNode(requestMessage));
        } catch (Exception e) {
            final UStatus status = logStatus(TAG, METHOD_UPDATE_NODE, toStatus(e));
            sendResponse(requestMessage, packToAny(status));
        }
    }

    private void findNodesProperty(@NonNull UMessage requestMessage) {
        try {
            checkArgument(mDatabaseInitialized.get(), UCode.FAILED_PRECONDITION, DATABASE_NOT_INITIALIZED);
            sendResponse(requestMessage, mRpcHandler.processFindNodeProperties(requestMessage));
        } catch (Exception e) {
            final UStatus status = logStatus(TAG, METHOD_FIND_NODE_PROPERTIES, toStatus(e));
            sendResponse(requestMessage, packToAny(FindNodePropertiesResponse.newBuilder().setStatus(status).build()));
        }
    }

    private void addNodes(@NonNull UMessage requestMessage) {
        try {
            checkArgument(mDatabaseInitialized.get(), UCode.FAILED_PRECONDITION, DATABASE_NOT_INITIALIZED);
            sendResponse(requestMessage, mRpcHandler.processAddNodesLDS(requestMessage));
        } catch (Exception e) {
            final UStatus status = logStatus(TAG, METHOD_ADD_NODES, toStatus(e));
            sendResponse(requestMessage, packToAny(status));
        }
    }

    private void deleteNodes(@NonNull UMessage requestMessage) {
        try {
            checkArgument(mDatabaseInitialized.get(), UCode.FAILED_PRECONDITION, DATABASE_NOT_INITIALIZED);
            sendResponse(requestMessage, mRpcHandler.processDeleteNodes(requestMessage));
        } catch (Exception e) {
            final UStatus status = logStatus(TAG, METHOD_DELETE_NODES, toStatus(e));
            sendResponse(requestMessage, packToAny(status));
        }
    }

    private void updateProperty(@NonNull UMessage requestMessage) {
        try {
            checkArgument(mDatabaseInitialized.get(), UCode.FAILED_PRECONDITION, DATABASE_NOT_INITIALIZED);
            sendResponse(requestMessage, mRpcHandler.processLDSUpdateProperty(requestMessage));
        } catch (Exception e) {
            final UStatus status = logStatus(TAG, METHOD_UPDATE_PROPERTY, toStatus(e));
            sendResponse(requestMessage, packToAny(status));
        }
    }

    private void registerNotification(@NonNull UMessage requestMessage) {
        try {
            checkArgument(mDatabaseInitialized.get(), UCode.FAILED_PRECONDITION, DATABASE_NOT_INITIALIZED);
            sendResponse(requestMessage, mRpcHandler.processRegisterNotifications(requestMessage));
        } catch (Exception e) {
            final UStatus status = logStatus(TAG, METHOD_REGISTER_FOR_NOTIFICATIONS, toStatus(e));
            sendResponse(requestMessage, packToAny(status));
        }
    }

    private void unregisterNotification(@NonNull UMessage requestMessage) {
        try {
            checkArgument(mDatabaseInitialized.get(), UCode.FAILED_PRECONDITION, DATABASE_NOT_INITIALIZED);
            sendResponse(requestMessage, mRpcHandler.processUnregisterNotifications(requestMessage));
        } catch (Exception e) {
            final UStatus status = logStatus(TAG, METHOD_UNREGISTER_FOR_NOTIFICATIONS, toStatus(e));
            sendResponse(requestMessage, packToAny(status));
        }
    }

    private static @NonNull UUri buildMethodUri(@NonNull String name) {
        return UUri.newBuilder()
                .setEntity(SERVICE)
                .setResource(UResourceBuilder.forRpcRequest(name))
                .build();
    }

    private CompletableFuture<UStatus> registerMethod(@NonNull String methodName, @NonNull Consumer<UMessage> handler) {
        final UUri methodUri = buildMethodUri(methodName);
        return CompletableFuture.supplyAsync(() -> {
            final UStatus status = mUpClient.registerListener(methodUri, mRequestListener);
            if (isOk(status)) {
                mMethodHandlers.put(methodUri, handler);
            }
            return logStatus(TAG, "registerMethod", status, Key.URI, stringify(methodUri));
        });
    }

    private CompletableFuture<UStatus> unregisterMethod(@NonNull String methodName) {
        final UUri methodUri = buildMethodUri(methodName);
        return CompletableFuture.supplyAsync(() -> {
            final UStatus status = mUpClient.unregisterListener(methodUri, mRequestListener);
            mMethodHandlers.remove(methodUri);
            return logStatus(TAG, "unregisterMethod", status, Key.URI, stringify(methodUri));
        });
    }

    private void sendResponse(@NonNull UMessage requestMessage, @NonNull UPayload responsePayload) {
        final UMessage responseMessage = UMessage.newBuilder()
                .setAttributes(UAttributesBuilder.response(requestMessage.getAttributes()).build())
                .setPayload(responsePayload)
                .build();
        mUpClient.send(responseMessage);
    }

    @Override
    public void onDestroy() {
        if (DEBUG) {
            Log.d(TAG, join(Key.METHOD, "onDestroy"));
        }
        CompletableFuture.allOf(
                        unregisterMethod(METHOD_LOOKUP_URI),
                        unregisterMethod(METHOD_FIND_NODES),
                        unregisterMethod(METHOD_UPDATE_NODE),
                        unregisterMethod(METHOD_FIND_NODE_PROPERTIES),
                        unregisterMethod(METHOD_ADD_NODES),
                        unregisterMethod(METHOD_DELETE_NODES),
                        unregisterMethod(METHOD_UPDATE_PROPERTY),
                        unregisterMethod(METHOD_REGISTER_FOR_NOTIFICATIONS),
                        unregisterMethod(METHOD_UNREGISTER_FOR_NOTIFICATIONS))
                .thenCompose(it -> mUpClient.disconnect())
                .whenComplete((status, exception) -> logStatus(TAG, "disconnect", status));
        mRpcHandler.shutdown();
        super.onDestroy();
    }

    private void handleRequest(@NonNull UMessage requestMessage) {
        if (DEBUG) {
            Log.d(TAG, join(Key.METHOD, "handleRequest", Key.MESSAGE, stringify(requestMessage)));
        }
        final UUri methodUri = requestMessage.getAttributes().getSink();
        final Consumer<UMessage> handler = mMethodHandlers.get(methodUri);
        if (handler != null) {
            handler.accept(requestMessage);
        }
    }
}
