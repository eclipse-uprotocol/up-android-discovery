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

import static org.eclipse.uprotocol.common.util.UStatusUtils.STATUS_OK;
import static org.eclipse.uprotocol.core.udiscovery.common.Constants.LDS_DB_FILENAME;
import static org.eclipse.uprotocol.core.udiscovery.common.Constants.UNEXPECTED_PAYLOAD;
import static org.eclipse.uprotocol.core.udiscovery.db.JsonNodeTest.REGISTRY_JSON;
import static org.eclipse.uprotocol.core.udiscovery.internal.Utils.toLongUri;
import static org.eclipse.uprotocol.transport.builder.UPayloadBuilder.packToAny;
import static org.eclipse.uprotocol.transport.builder.UPayloadBuilder.unpack;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import android.content.Context;
import android.util.Log;

import androidx.test.platform.app.InstrumentationRegistry;

import org.eclipse.uprotocol.common.UStatusException;
import org.eclipse.uprotocol.core.udiscovery.db.DiscoveryManager;
import org.eclipse.uprotocol.core.udiscovery.v3.AddNodesRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.DeleteNodesRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.FindNodePropertiesRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.FindNodePropertiesResponse;
import org.eclipse.uprotocol.core.udiscovery.v3.FindNodesRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.FindNodesResponse;
import org.eclipse.uprotocol.core.udiscovery.v3.LookupUriResponse;
import org.eclipse.uprotocol.core.udiscovery.v3.Node;
import org.eclipse.uprotocol.core.udiscovery.v3.NotificationsRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.ObserverInfo;
import org.eclipse.uprotocol.core.udiscovery.v3.PropertyValue;
import org.eclipse.uprotocol.core.udiscovery.v3.UpdateNodeRequest;
import org.eclipse.uprotocol.core.udiscovery.v3.UpdatePropertyRequest;
import org.eclipse.uprotocol.v1.UCode;
import org.eclipse.uprotocol.v1.UEntity;
import org.eclipse.uprotocol.v1.UMessage;
import org.eclipse.uprotocol.v1.UPayload;
import org.eclipse.uprotocol.v1.UStatus;
import org.eclipse.uprotocol.v1.UUri;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.shadows.ShadowLog;

import java.io.IOException;
import java.util.List;

@RunWith(RobolectricTestRunner.class)
public class RPCHandlerTest extends TestBase {
    private static final UUri TEST_AUTHORITY_URI = UUri.newBuilder()
            .setAuthority(TEST_AUTHORITY)
            .build();

    private static final UUri TEST_ENTITY_URI =  UUri.newBuilder()
            .setAuthority(TEST_AUTHORITY)
            .setEntity(UEntity.newBuilder()
                    .setName(TEST_ENTITY_NAME)
                    .build())
            .build();

    public Context mContext;
    @Mock
    public Notifier mNotifier;
    @Mock
    public AssetManager mAssetManager;
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();
    public RPCHandler mRPCHandler;
    public DiscoveryManager mDiscoveryManager;
    @Mock
    private ObserverManager mObserverManager;

    private static void setLogLevel(int level) {
        RPCHandler.DEBUG = (level <= Log.DEBUG);
        RPCHandler.VERBOSE = (level <= Log.VERBOSE);
    }

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        ShadowLog.stream = System.out;
        setLogLevel(Log.INFO);
        mContext = InstrumentationRegistry.getInstrumentation().getContext();
        // initialize Discovery Manager
        initializeDb();
        mObserverManager = mock(ObserverManager.class);
        mRPCHandler = new RPCHandler(mContext, mAssetManager, mDiscoveryManager,
                mObserverManager);
    }

    @After
    public void shutdown() {
        mRPCHandler.shutdown();
    }

    public void initializeDb() {
        mDiscoveryManager = spy(new DiscoveryManager(mNotifier));
        mDiscoveryManager.init(TEST_AUTHORITY);
        Node node = jsonToNode(REGISTRY_JSON);
        UStatus status = mDiscoveryManager.updateNode(node, -1);
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void positive_persist() {
        String payload = mDiscoveryManager.export();
        mRPCHandler.persist(payload);
        verify(mAssetManager).writeFileToInternalStorage(mContext, LDS_DB_FILENAME, payload);
    }

    @Test
    public void positive_processLookupUri() {
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(TEST_ENTITY_URI)).build();

        UPayload responsePayload = mRPCHandler.processLookupUriFromLDS(requestMessage);
        LookupUriResponse response = unpack(responsePayload, LookupUriResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).lookupUri(TEST_ENTITY_URI);
        assertEquals(UCode.OK, response.getStatus().getCode());
    }

    @Test
    public void positive_processLookupUri_debug() {
        setLogLevel(Log.DEBUG);
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(TEST_ENTITY_URI)).build();

        UPayload responsePayload = mRPCHandler.processLookupUriFromLDS(requestMessage);
        LookupUriResponse response = unpack(responsePayload, LookupUriResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).lookupUri(TEST_ENTITY_URI);
        assertEquals(UCode.OK, response.getStatus().getCode());
    }

    @Test
    public void negative_processLookupUri_exception() {
        // pack incorrect protobuf message type
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(UStatus.getDefaultInstance())).build();

        UPayload responsePayload = mRPCHandler.processLookupUriFromLDS(requestMessage);
        LookupUriResponse response = unpack(responsePayload, LookupUriResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager, never()).lookupUri(any());
        assertEquals(UCode.INVALID_ARGUMENT, response.getStatus().getCode());
    }

    @Test
    public void positive_processFindNodes() {
        FindNodesRequest request = FindNodesRequest.newBuilder().setUri(toLongUri(TEST_AUTHORITY_URI)).build();

        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processFindNodesFromLDS(requestMessage);
        FindNodesResponse response = unpack(responsePayload, FindNodesResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).findNode(TEST_AUTHORITY_URI, -1);
        assertEquals(UCode.OK, response.getStatus().getCode());
    }

    @Test
    public void positive_processFindNodes_debug() {
        setLogLevel(Log.DEBUG);
        FindNodesRequest request = FindNodesRequest.newBuilder().setUri(toLongUri(TEST_AUTHORITY_URI)).setDepth(0).build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processFindNodesFromLDS(requestMessage);
        FindNodesResponse response = unpack(responsePayload, FindNodesResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).findNode(TEST_AUTHORITY_URI, 0);
        assertEquals(UCode.OK, response.getStatus().getCode());
    }

    @Test
    public void negative_processFindNodes_exception() {
        // pack incorrect protobuf message type
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(UStatus.getDefaultInstance())).build();

        UPayload responsePayload = mRPCHandler.processFindNodesFromLDS(requestMessage);
        FindNodesResponse response = unpack(responsePayload, FindNodesResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        assertEquals(UCode.INVALID_ARGUMENT, response.getStatus().getCode());
    }

    @Test
    public void positive_processFindNodeProperties() {
        FindNodePropertiesRequest request = FindNodePropertiesRequest.newBuilder()
                .setUri(toLongUri(TEST_ENTITY_URI))
                .addProperties(TEST_PROPERTY1)
                .addProperties(TEST_PROPERTY2)
                .build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processFindNodeProperties(requestMessage);
        FindNodePropertiesResponse response = unpack(responsePayload, FindNodePropertiesResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).findNodeProperties(TEST_ENTITY_URI, request.getPropertiesList());
        assertEquals(UCode.OK, response.getStatus().getCode());
    }

    @Test
    public void positive_processFindNodeProperties_debug() {
        setLogLevel(Log.DEBUG);

        FindNodePropertiesRequest request = FindNodePropertiesRequest.newBuilder()
                .setUri(toLongUri(TEST_ENTITY_URI))
                .addProperties(TEST_PROPERTY1)
                .addProperties(TEST_PROPERTY2)
                .build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processFindNodeProperties(requestMessage);
        FindNodePropertiesResponse response = unpack(responsePayload, FindNodePropertiesResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).findNodeProperties(TEST_ENTITY_URI, request.getPropertiesList());
        assertEquals(UCode.OK, response.getStatus().getCode());
    }

    @Test
    public void negative_processFindNodeProperties_exception() {
        // pack incorrect protobuf message type
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(UStatus.getDefaultInstance())).build();

        UPayload responsePayload = mRPCHandler.processFindNodeProperties(requestMessage);
        FindNodePropertiesResponse response = unpack(responsePayload, FindNodePropertiesResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verifyNoInteractions(mAssetManager);
        assertEquals(UCode.INVALID_ARGUMENT, response.getStatus().getCode());
    }

    @Test
    public void positive_processUpdateNode() {
        Node node = Node.newBuilder().setUri(toLongUri(TEST_ENTITY_URI)).setType(Node.Type.ENTITY).build();
        UpdateNodeRequest request = UpdateNodeRequest.newBuilder().setNode(node).build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processLDSUpdateNode(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mAssetManager).writeFileToInternalStorage(eq(mContext), eq(LDS_DB_FILENAME), anyString());
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void positive_processUpdateNode_debug() {
        setLogLevel(Log.DEBUG);

        Node node = Node.newBuilder().setUri(toLongUri(TEST_ENTITY_URI)).setType(Node.Type.ENTITY).build();
        UpdateNodeRequest request = UpdateNodeRequest.newBuilder().setNode(node).setTtl(0).build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processLDSUpdateNode(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mAssetManager).writeFileToInternalStorage(eq(mContext), eq(LDS_DB_FILENAME), anyString());
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void negative_processUpdateNode_exception() {
        // pack incorrect protobuf message type
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(UStatus.getDefaultInstance())).build();

        UPayload responsePayload = mRPCHandler.processLDSUpdateNode(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verifyNoInteractions(mAssetManager);
        assertEquals(UCode.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    public void positive_processUpdateProperty() {
        PropertyValue propertyValue = PropertyValue.newBuilder().setUInteger(0).build();
        UpdatePropertyRequest request = UpdatePropertyRequest.newBuilder()
                .setUri(toLongUri(TEST_ENTITY_URI))
                .setProperty(TEST_PROPERTY1)
                .setValue(propertyValue)
                .build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processLDSUpdateProperty(requestMessage);
        UStatus sts = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).updateProperty(TEST_PROPERTY1, propertyValue, TEST_ENTITY_URI);
        verify(mAssetManager).writeFileToInternalStorage(eq(mContext), eq(LDS_DB_FILENAME), anyString());
        assertEquals(UCode.OK, sts.getCode());
    }

    @Test
    public void positive_processUpdateProperty_debug() {
        setLogLevel(Log.DEBUG);

        PropertyValue propertyValue = PropertyValue.newBuilder().setUInteger(0).build();
        UpdatePropertyRequest request = UpdatePropertyRequest.newBuilder()
                .setUri(toLongUri(TEST_ENTITY_URI))
                .setProperty(TEST_PROPERTY1)
                .setValue(propertyValue)
                .build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processLDSUpdateProperty(requestMessage);
        UStatus sts = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).updateProperty(TEST_PROPERTY1, propertyValue, TEST_ENTITY_URI);
        verify(mAssetManager).writeFileToInternalStorage(eq(mContext), eq(LDS_DB_FILENAME), anyString());
        assertEquals(UCode.OK, sts.getCode());
    }

    @Test
    public void negative_processUpdateProperty_exception() {
        // pack incorrect protobuf message type
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(UStatus.getDefaultInstance())).build();

        UPayload responsePayload = mRPCHandler.processLDSUpdateProperty(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager, never()).updateProperty(anyString(), any(), any());
        verifyNoInteractions(mAssetManager);
        assertEquals(UCode.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    public void positive_processAddNodes() {
        UUri uri = UUri.newBuilder()
                .setAuthority(TEST_AUTHORITY)
                .setEntity(UEntity.newBuilder()
                        .setName(TEST_ALTERNATE_ENTITY)
                        .build())
                .build();
        Node node = Node.newBuilder().setUri(toLongUri(uri)).setType(Node.Type.ENTITY).build();

        AddNodesRequest request = AddNodesRequest.newBuilder()
                .setParentUri(toLongUri(TEST_AUTHORITY_URI))
                .addNodes(node)
                .build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processAddNodesLDS(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).addNodes(any(), any());
        verify(mAssetManager).writeFileToInternalStorage(eq(mContext), eq(LDS_DB_FILENAME), anyString());
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void positive_processAddNodes_verbose() {
        setLogLevel(Log.VERBOSE);
        UUri uri = UUri.newBuilder()
                .setAuthority(TEST_AUTHORITY)
                .setEntity(UEntity.newBuilder()
                        .setName(TEST_ALTERNATE_ENTITY)
                        .build())
                .build();
        Node node = Node.newBuilder().setUri(toLongUri(uri)).setType(Node.Type.ENTITY).build();

        AddNodesRequest request = AddNodesRequest.newBuilder()
                .setParentUri(toLongUri(TEST_AUTHORITY_URI))
                .addNodes(node)
                .build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processAddNodesLDS(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).addNodes(any(), any());
        verify(mAssetManager).writeFileToInternalStorage(eq(mContext), eq(LDS_DB_FILENAME), anyString());
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void negative_processAddNodes_exception() {
        // pack incorrect protobuf message type
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(UStatus.getDefaultInstance())).build();

        UPayload responsePayload = mRPCHandler.processAddNodesLDS(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verifyNoInteractions(mAssetManager);
        assertEquals(UCode.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    public void positive_processDeleteNodes() {
        DeleteNodesRequest request = DeleteNodesRequest.newBuilder().addUris(toLongUri(TEST_ENTITY_URI)).build();

        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processDeleteNodes(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).deleteNodes(List.of(TEST_ENTITY_URI));
        verify(mAssetManager).writeFileToInternalStorage(eq(mContext), eq(LDS_DB_FILENAME), anyString());
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void positive_processDeleteNodes_debug() {
        setLogLevel(Log.DEBUG);

        DeleteNodesRequest request = DeleteNodesRequest.newBuilder().addUris(toLongUri(TEST_ENTITY_URI)).build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        UPayload responsePayload = mRPCHandler.processDeleteNodes(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mDiscoveryManager).deleteNodes(List.of(TEST_ENTITY_URI));

        verify(mAssetManager).writeFileToInternalStorage(eq(mContext), eq(LDS_DB_FILENAME), anyString());
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void negative_processDeleteNodes_exception() {
        // pack incorrect protobuf message type
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(UStatus.getDefaultInstance())).build();

        UPayload responsePayload = mRPCHandler.processDeleteNodes(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verifyNoInteractions(mAssetManager);
        assertEquals(UCode.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    public void positive_RegisterForNotifications() {
        UUri observerUri = UUri.getDefaultInstance();
        UUri nodeUri = TEST_AUTHORITY_URI;

        ObserverInfo info = ObserverInfo.newBuilder().setUri(toLongUri(observerUri)).build();
        NotificationsRequest request = NotificationsRequest.newBuilder()
                .setObserver(info)
                .addUris(toLongUri(nodeUri))
                .build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        when(mObserverManager.registerObserver(List.of(nodeUri), observerUri)).thenReturn(STATUS_OK);

        UPayload responsePayload = mRPCHandler.processRegisterNotifications(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mObserverManager).registerObserver(List.of(nodeUri), observerUri);
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void positive_RegisterForNotifications_debug() {
        setLogLevel(Log.DEBUG);
        UUri observerUri = UUri.getDefaultInstance();
        UUri nodeUri = TEST_AUTHORITY_URI;
        ObserverInfo info = ObserverInfo.newBuilder().setUri(toLongUri(observerUri)).build();
        NotificationsRequest request = NotificationsRequest.newBuilder()
                .setObserver(info)
                .addUris(toLongUri(nodeUri))
                .build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        when(mObserverManager.registerObserver(List.of(nodeUri), observerUri)).thenReturn(STATUS_OK);

        UPayload responsePayload = mRPCHandler.processRegisterNotifications(requestMessage);

        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mObserverManager).registerObserver(List.of(nodeUri), observerUri);
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void positive_UnregisterForNotifications() {
        UUri observerUri = UUri.getDefaultInstance();
        UUri nodeUri = TEST_AUTHORITY_URI;
        ObserverInfo info = ObserverInfo.newBuilder().setUri(toLongUri(observerUri)).build();
        NotificationsRequest request = NotificationsRequest.newBuilder()
                .setObserver(info)
                .addUris(toLongUri(nodeUri))
                .build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();

        when(mObserverManager.unregisterObserver(List.of(nodeUri), observerUri)).thenReturn(STATUS_OK);

        UPayload responsePayload = mRPCHandler.processUnregisterNotifications(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mObserverManager).unregisterObserver(List.of(nodeUri), observerUri);
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void positive_UnregisterForNotifications_debug() {
        setLogLevel(Log.DEBUG);
        UUri observerUri = UUri.getDefaultInstance();
        UUri nodeUri = TEST_AUTHORITY_URI;
        ObserverInfo info = ObserverInfo.newBuilder().setUri(toLongUri(observerUri)).build();
        NotificationsRequest request = NotificationsRequest.newBuilder()
                .setObserver(info)
                .addUris(toLongUri(nodeUri))
                .build();
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(request)).build();
        when(mObserverManager.unregisterObserver(List.of(nodeUri), observerUri)).thenReturn(STATUS_OK);

        UPayload responsePayload = mRPCHandler.processUnregisterNotifications(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verify(mObserverManager).unregisterObserver(List.of(nodeUri), observerUri);
        assertEquals(UCode.OK, status.getCode());
    }

    @Test
    public void negative_extractPayload_exception() {
        // build a payload object omitting the format
        UPayload payload = UPayload.getDefaultInstance();
        UMessage requestMessage = UMessage.newBuilder().setPayload(payload).build();

        UPayload responsePayload = mRPCHandler.processLookupUriFromLDS(requestMessage);
        UStatus lookupStatus = unpack(responsePayload, LookupUriResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD)).getStatus();

        responsePayload = mRPCHandler.processFindNodesFromLDS(requestMessage);
        UStatus findNodesStatus = unpack(responsePayload, FindNodesResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD)).getStatus();

        responsePayload = mRPCHandler.processFindNodeProperties(requestMessage);
        UStatus findNodePropertiesStatus = unpack(responsePayload, FindNodePropertiesResponse.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD)).getStatus();

        responsePayload = mRPCHandler.processLDSUpdateNode(requestMessage);
        UStatus updateNodeStatus = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        responsePayload = mRPCHandler.processLDSUpdateProperty(requestMessage);
        UStatus updateNodePropertyStatus = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        responsePayload = mRPCHandler.processAddNodesLDS(requestMessage);
        UStatus addNodesStatus = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        responsePayload = mRPCHandler.processDeleteNodes(requestMessage);
        UStatus deleteNodesStatus = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        responsePayload = mRPCHandler.processRegisterNotifications(requestMessage);
        UStatus registerForNotificationStatus = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        responsePayload = mRPCHandler.processUnregisterNotifications(requestMessage);
        UStatus unregisterForNotificationStatus = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        assertEquals(UCode.INVALID_ARGUMENT, lookupStatus.getCode());
        assertEquals(UCode.INVALID_ARGUMENT, findNodesStatus.getCode());
        assertEquals(UCode.INVALID_ARGUMENT, findNodePropertiesStatus.getCode());
        assertEquals(UCode.INVALID_ARGUMENT, updateNodeStatus.getCode());
        assertEquals(UCode.INVALID_ARGUMENT, updateNodePropertyStatus.getCode());
        assertEquals(UCode.INVALID_ARGUMENT, addNodesStatus.getCode());
        assertEquals(UCode.INVALID_ARGUMENT, deleteNodesStatus.getCode());
        assertEquals(UCode.INVALID_ARGUMENT, registerForNotificationStatus.getCode());
        assertEquals(UCode.INVALID_ARGUMENT, unregisterForNotificationStatus.getCode());
    }

    @Test
    public void negative_refreshDatabase() {
        UMessage requestMessage = UMessage.newBuilder().setPayload(packToAny(UpdateNodeRequest.getDefaultInstance())).build();

        UPayload responsePayload = mRPCHandler.processLDSUpdateNode(requestMessage);
        UStatus status = unpack(responsePayload, UStatus.class).
                orElseThrow(() -> new UStatusException(UCode.INVALID_ARGUMENT, UNEXPECTED_PAYLOAD));

        verifyNoInteractions(mAssetManager);
        assertEquals(UCode.INVALID_ARGUMENT, status.getCode());
    }
}
