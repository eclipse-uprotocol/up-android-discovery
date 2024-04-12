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

import static android.util.Log.DEBUG;
import static android.util.Log.INFO;

import static junit.framework.TestCase.assertNotNull;

import static org.eclipse.uprotocol.common.util.UStatusUtils.STATUS_OK;
import static org.eclipse.uprotocol.common.util.UStatusUtils.buildStatus;
import static org.eclipse.uprotocol.core.udiscovery.db.JsonNodeTest.REGISTRY_JSON;
import static org.eclipse.uprotocol.core.udiscovery.internal.Utils.toLongUri;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_ADD_NODES;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_DELETE_NODES;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_FIND_NODES;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_FIND_NODE_PROPERTIES;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_LOOKUP_URI;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_REGISTER_FOR_NOTIFICATIONS;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_UNREGISTER_FOR_NOTIFICATIONS;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_UPDATE_NODE;
import static org.eclipse.uprotocol.core.udiscovery.v3.UDiscovery.METHOD_UPDATE_PROPERTY;
import static org.eclipse.uprotocol.transport.builder.UPayloadBuilder.packToAny;
import static org.eclipse.uprotocol.transport.builder.UPayloadBuilder.unpack;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.robolectric.Robolectric.setupService;

import android.content.Intent;
import android.util.Log;

import com.google.protobuf.Any;

import org.eclipse.uprotocol.UPClient;
import org.eclipse.uprotocol.core.udiscovery.v3.AddNodesRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.DeleteNodesRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.FindNodePropertiesRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.FindNodePropertiesResponse;
import org.eclipse.uprotocol.core.udiscovery.v3.FindNodesRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.FindNodesResponse;
import org.eclipse.uprotocol.core.udiscovery.v3.LookupUriResponse;
import org.eclipse.uprotocol.core.udiscovery.v3.NotificationsRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.PropertyValue;
import org.eclipse.uprotocol.core.udiscovery.v3.UpdateNodeRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.UpdatePropertyRequest;
import org.eclipse.uprotocol.transport.UListener;
import org.eclipse.uprotocol.transport.builder.UAttributesBuilder;
import org.eclipse.uprotocol.uri.factory.UResourceBuilder;
import org.eclipse.uprotocol.v1.UCode;
import org.eclipse.uprotocol.v1.UEntity;
import org.eclipse.uprotocol.v1.UMessage;
import org.eclipse.uprotocol.v1.UPayload;
import org.eclipse.uprotocol.v1.UPriority;
import org.eclipse.uprotocol.v1.UStatus;
import org.eclipse.uprotocol.v1.UUri;
import org.eclipse.uprotocol.v1.UUriBatch;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.shadows.ShadowLog;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings({"java:S1200", "java:S3008", "java:S1134", "java:S2925", "java:S3415",
        "java:S5845"})
@RunWith(RobolectricTestRunner.class)
public class UDiscoveryServiceTest extends TestBase {
    private static final UStatus FAILED_STATUS = buildStatus(UCode.FAILED_PRECONDITION);
    private static final UStatus NOT_FOUND_STATUS = buildStatus(UCode.NOT_FOUND);
    private static final UMessage LOOKUP_URI_REQUEST_MESSAGE =
            buildRequestMessage(METHOD_LOOKUP_URI, packToAny(UUri.getDefaultInstance()));
    private static final UMessage FIND_NODES_REQUEST_MESSAGE =
            buildRequestMessage(METHOD_FIND_NODES, packToAny(FindNodesRequest.getDefaultInstance()));
    private static final UMessage FIND_NODE_PROPERTIES_REQUEST_MESSAGE =
            buildRequestMessage(METHOD_FIND_NODE_PROPERTIES, packToAny(FindNodePropertiesRequest.getDefaultInstance()));
    private static final UMessage UPDATE_NODE_REQUEST_MESSAGE =
            buildRequestMessage(METHOD_UPDATE_NODE, packToAny(UpdateNodeRequest.getDefaultInstance()));
    private static final UMessage ADD_NODES_REQUEST_MESSAGE =
            buildRequestMessage(METHOD_ADD_NODES, packToAny(AddNodesRequest.getDefaultInstance()));
    private static final UMessage DELETE_NODES_REQUEST_MESSAGE =
            buildRequestMessage(METHOD_DELETE_NODES, packToAny(DeleteNodesRequest.getDefaultInstance()));
    private static final UMessage REGISTER_FOR_NOTIFICATIONS_REQUEST_MESSAGE =
            buildRequestMessage(METHOD_REGISTER_FOR_NOTIFICATIONS, packToAny(NotificationsRequest.getDefaultInstance()));
    private static final UMessage UNREGISTER_FOR_NOTIFICATIONS_REQUEST_MESSAGE =
            buildRequestMessage(METHOD_UNREGISTER_FOR_NOTIFICATIONS, packToAny(NotificationsRequest.getDefaultInstance()));
    private static final UMessage UPDATE_PROPERTIES_REQUEST_MESSAGE =
            buildRequestMessage(METHOD_UPDATE_PROPERTY, packToAny(UpdatePropertyRequest.getDefaultInstance()));
    private static final LookupUriResponse LOOKUP_URI_RESPONSE = LookupUriResponse.newBuilder()
            .setUris(UUriBatch.newBuilder()
                    .addUris(UUri.newBuilder()
                            .setAuthority(TEST_AUTHORITY)
                            .setEntity(UEntity.newBuilder()
                                    .setName("body.cabin_climate/1")
                                    .build())
                            .build())
                    .addUris(UUri.newBuilder()
                            .setAuthority(TEST_AUTHORITY)
                            .setEntity(UEntity.newBuilder()
                                    .setName("body.cabin_climate/2")
                                    .build())
                            .build())
                    .addUris(UUri.newBuilder()
                            .setAuthority(TEST_AUTHORITY)
                            .setEntity(UEntity.newBuilder()
                                    .setName("body.cabin_climate/3")
                                    .build())
                            .build()))
            .build();
    private static final FindNodesResponse FIND_NODES_RESPONSE = FindNodesResponse.newBuilder()
            .addNodes(jsonToNode(REGISTRY_JSON))
            .setStatus(buildStatus(UCode.OK))
            .build();
    private static final FindNodePropertiesResponse FIND_NODE_PROPERTIES_RESPONSE = FindNodePropertiesResponse.newBuilder()
            .putProperties("message", PropertyValue.newBuilder().setUString("hello world").build())
            .putProperties("year", PropertyValue.newBuilder().setUInteger(2024).build())
            .putProperties("enabled", PropertyValue.newBuilder().setUBoolean(true).build())
            .build();
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();
    private UDiscoveryService mService;
    private UListener mListener;
    @Mock
    private UPClient mUpClient;
    @Mock
    private RPCHandler mRpcHandler;
    @Mock
    private ResourceLoader mResourceLoader;


    private static void setLogLevel(int level) {
        UDiscoveryService.VERBOSE = (level <= Log.VERBOSE);
        UDiscoveryService.DEBUG = (level <= DEBUG);
    }

    private static UMessage buildRequestMessage(String methodName, UPayload payload) {
        UUri methodUri = UUri.newBuilder()
                .setEntity(SERVICE)
                .setResource(UResourceBuilder.forRpcRequest(methodName))
                .build();
        return UMessage.newBuilder()
                .setAttributes(UAttributesBuilder.request(methodUri, methodUri, UPriority.UPRIORITY_CS4, TTL).build())
                .setPayload(payload)
                .build();
    }

    @Before
    public void setUp() {
        ShadowLog.stream = System.out;

        CompletableFuture<UStatus> response = CompletableFuture.completedFuture(STATUS_OK);
        when(mUpClient.connect()).thenReturn(response);
        when(mResourceLoader.initializeLDS()).thenReturn(ResourceLoader.InitLDSCode.SUCCESS);
        when(mUpClient.isConnected()).thenReturn(true);
        when(mUpClient.registerListener(any(), any())).thenReturn(STATUS_OK);

        mService = new UDiscoveryService(mRpcHandler, mUpClient, mResourceLoader);

        // sleep to ensure registerAllMethods completes in the async thread before the verify
        // registerEventListener call below
        sleep(DELAY_MS);

        // capture the request listener to inject rpc request messages
        ArgumentCaptor<UListener> captor = ArgumentCaptor.forClass(UListener.class);
        verify(mUpClient, atLeastOnce()).registerListener(any(), captor.capture());
        mListener = captor.getValue();
    }

    @Test
    public void initialization_test() {
        assertTrue(true);
    }


    @Test
    public void negative_upClient_connect_exception() {
        setLogLevel(Log.VERBOSE);
        CompletableFuture<UStatus> connectionFuture = CompletableFuture.completedFuture(FAILED_STATUS);
        UPClient upClient = mock(UPClient.class);
        when(upClient.connect()).thenReturn(connectionFuture);
        assertThrows(Exception.class, () -> new UDiscoveryService(mRpcHandler, upClient, mResourceLoader));
    }

    @Test
    public void negative_upClient_isConnected_false() {
        UPClient upClient = mock(UPClient.class);
        CompletableFuture<UStatus> connectionFuture = CompletableFuture.completedFuture(STATUS_OK);

        when(upClient.connect()).thenReturn(connectionFuture);
        when(upClient.isConnected()).thenReturn(false);

        new UDiscoveryService(mRpcHandler, upClient, mResourceLoader);
        verify(upClient, never()).registerListener(any(), any());
    }

    @Test
    public void negative_handler_uninitialized_exception() {
        setLogLevel(DEBUG);
        ResourceLoader mockLoader = mock(ResourceLoader.class);
        when(mockLoader.initializeLDS()).thenReturn(ResourceLoader.InitLDSCode.FAILURE);
        new UDiscoveryService(mRpcHandler, mUpClient, mockLoader);

        // sleep to ensure registerAllMethods completes in the async thread before the verify
        // registerEventListener call below
        sleep(DELAY_MS);

        // capture the request listener to inject rpc request messages
        ArgumentCaptor<UListener> listenerCaptor = ArgumentCaptor.forClass(UListener.class);
        verify(mUpClient, atLeastOnce()).registerListener(any(), listenerCaptor.capture());
        UListener listener = listenerCaptor.getValue();

        List<UMessage> requestMessages = List.of(
                LOOKUP_URI_REQUEST_MESSAGE,
                FIND_NODES_REQUEST_MESSAGE,
                ADD_NODES_REQUEST_MESSAGE,
                UPDATE_NODE_REQUEST_MESSAGE,
                DELETE_NODES_REQUEST_MESSAGE,
                UPDATE_PROPERTIES_REQUEST_MESSAGE,
                FIND_NODE_PROPERTIES_REQUEST_MESSAGE,
                REGISTER_FOR_NOTIFICATIONS_REQUEST_MESSAGE,
                UNREGISTER_FOR_NOTIFICATIONS_REQUEST_MESSAGE);
        for (UMessage requestMessage : requestMessages) {
            reset(mUpClient);
            ArgumentCaptor<UMessage> messageCaptor = ArgumentCaptor.forClass(UMessage.class);
            listener.onReceive(requestMessage);
            verify(mUpClient, timeout(DELAY_MS)).send(messageCaptor.capture());
            UMessage responseMessage = messageCaptor.getValue();
            UStatus status;
            String method = requestMessage.getAttributes().getSource().getResource().getInstance();
            status = switch (method) {
                case METHOD_LOOKUP_URI -> unpack(responseMessage.getPayload(), LookupUriResponse.class)
                        .orElseThrow(() -> new RuntimeException("Unexpected payload")).getStatus();
                case METHOD_FIND_NODES -> unpack(responseMessage.getPayload(), FindNodesResponse.class)
                        .orElseThrow(() -> new RuntimeException("Unexpected payload")).getStatus();
                case METHOD_FIND_NODE_PROPERTIES -> unpack(responseMessage.getPayload(),
                        FindNodePropertiesResponse.class)
                        .orElseThrow(() -> new RuntimeException("Unexpected payload")).getStatus();
                default -> unpack(responseMessage.getPayload(), UStatus.class)
                        .orElseThrow(() -> new RuntimeException("Unexpected payload"));
            };
            assertEquals(UCode.FAILED_PRECONDITION, status.getCode());
        }
    }

    @Test
    public void negative_upClient_registerMethod_exception() {
        setLogLevel(Log.VERBOSE);
        CompletableFuture<UStatus> connectionFuture = CompletableFuture.completedFuture(STATUS_OK);
        when(mUpClient.connect()).thenReturn(connectionFuture);

        when(mUpClient.registerListener(any(), any())).thenReturn(FAILED_STATUS);

        new UDiscoveryService(mRpcHandler, mUpClient, mResourceLoader);
        // wait for register async tasks to complete
        sleep(DELAY_MS);
        verify(mUpClient, atLeastOnce()).registerListener(any(), any());
    }

    @Test
    public void negative_upClient_unregisterMethod_exception() {
        setLogLevel(DEBUG);
        when(mUpClient.unregisterListener(any(), any())).thenReturn(FAILED_STATUS);

        mService.onDestroy();
        // wait for unregister async tasks to complete
        sleep(DELAY_MS);
        verify(mUpClient, atLeastOnce()).unregisterListener(any(), any());
    }

    @Test
    public void negative_withoutSink_handleRequest() {
        UUri uri = UUri.newBuilder().setEntity(TEST_ENTITY)
                .setResource(UResourceBuilder.forRpcRequest("fakeMethod"))
                .build();
        UMessage requestMessage = buildRequestMessage(toLongUri(uri), packToAny(Any.getDefaultInstance()));
        mListener.onReceive(requestMessage);
        sleep(DELAY_MS);
        verifyNoInteractions(mRpcHandler);
    }

    private void assertResponseSent(UPayload payload) {
        ArgumentCaptor<UMessage> messageCaptor = ArgumentCaptor.forClass(UMessage.class);
        verify(mUpClient, timeout(DELAY_MS)).send(messageCaptor.capture());
        UMessage responseMessage = messageCaptor.getValue();
        assertEquals(payload, responseMessage.getPayload());
    }

    @Test
    public void positive_LookupUri_LDS() {
        setLogLevel(INFO);
        UPayload responsePayload = packToAny(LOOKUP_URI_RESPONSE);
        when(mRpcHandler.processLookupUriFromLDS(any())).thenReturn(responsePayload);
        
        mListener.onReceive(LOOKUP_URI_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_LookupUri_LDS_not_found() {
        setLogLevel(DEBUG);
        UPayload responsePayload = packToAny(NOT_FOUND_STATUS);
        when(mRpcHandler.processLookupUriFromLDS(any())).thenReturn(responsePayload);

        mListener.onReceive(LOOKUP_URI_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_FindNodes_LDS() {
        setLogLevel(DEBUG);
        UPayload responsePayload = packToAny(FIND_NODES_RESPONSE);
        when(mRpcHandler.processFindNodesFromLDS(any())).thenReturn(responsePayload);

        mListener.onReceive(FIND_NODES_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_FindNodes_LDS_not_found() {
        setLogLevel(INFO);
        UPayload responsePayload = packToAny(FIND_NODES_RESPONSE);
        when(mRpcHandler.processFindNodesFromLDS(any())).thenReturn(responsePayload);

        mListener.onReceive(FIND_NODES_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_UpdateNode() {
        setLogLevel(DEBUG);
        UPayload responsePayload = packToAny(STATUS_OK);
        when(mRpcHandler.processLDSUpdateNode(any())).thenReturn(responsePayload);

        mListener.onReceive(UPDATE_NODE_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_FindNodesProperty() {
        UPayload responsePayload = packToAny(FIND_NODE_PROPERTIES_RESPONSE);
        when(mRpcHandler.processFindNodeProperties(any())).thenReturn(responsePayload);

        mListener.onReceive(FIND_NODE_PROPERTIES_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_AddNodes() {
        setLogLevel(DEBUG);
        UPayload responsePayload = packToAny(STATUS_OK);
        when(mRpcHandler.processAddNodesLDS(any())).thenReturn(responsePayload);

        mListener.onReceive(ADD_NODES_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_DeleteNodes() {
        UPayload responsePayload = packToAny(STATUS_OK);
        when(mRpcHandler.processDeleteNodes(any())).thenReturn(responsePayload);

        mListener.onReceive(DELETE_NODES_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_UpdateProperty() {
        UPayload responsePayload = packToAny(STATUS_OK);
        when(mRpcHandler.processLDSUpdateProperty(any())).thenReturn(responsePayload);

        mListener.onReceive(UPDATE_PROPERTIES_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_RegisterNotification() {
        UPayload responsePayload = packToAny(STATUS_OK);
        when(mRpcHandler.processRegisterNotifications(any()))
                .thenReturn(responsePayload);

        mListener.onReceive(REGISTER_FOR_NOTIFICATIONS_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_UnRegisterNotification() {
        UPayload responsePayload = packToAny(STATUS_OK);
        when(mRpcHandler.processUnregisterNotifications(any())).thenReturn(responsePayload);

        mListener.onReceive(UNREGISTER_FOR_NOTIFICATIONS_REQUEST_MESSAGE);
        assertResponseSent(responsePayload);
    }

    @Test
    public void positive_OnCreate() {
        setLogLevel(DEBUG);
        UDiscoveryService service = setupService(UDiscoveryService.class);
        assertNotEquals(mService, service);
        setLogLevel(INFO);
        service = setupService(UDiscoveryService.class);
        assertNotEquals(mService, service);
    }

    @Test
    public void positive_onBind() {
        setLogLevel(DEBUG);
        assertNotNull(mService.onBind(new Intent()));
        setLogLevel(INFO);
        assertNotNull(mService.onBind(new Intent()));
    }

    @Test
    public void positive_onDestroy() {
        when(mUpClient.unregisterListener(any(), any())).thenReturn(STATUS_OK);
        when(mUpClient.isConnected()).thenReturn(true);
        when(mUpClient.unregisterListener(any(), any())).thenReturn(STATUS_OK);
        mService.onDestroy();
        // wait for unregister async tasks to complete
        sleep(DELAY_MS);
        verify(mUpClient, atLeastOnce()).unregisterListener(any(), any());
    }
}
