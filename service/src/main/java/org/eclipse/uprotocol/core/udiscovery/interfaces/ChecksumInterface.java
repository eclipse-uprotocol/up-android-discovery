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

package org.eclipse.uprotocol.core.udiscovery.interfaces;
/**
 * The ChecksumInterface is an interface that provides methods for generating and verifying hashes.
 * It is used to ensure data integrity by creating a unique hash for a given input and verifying it.
 */
public interface ChecksumInterface {

    /**
     * Generates a hash for the given input.
     *
     * @param input The input string for which the hash is to be generated.
     * @return The generated hash as a string.
     */
    String generateHash(String input);

    /**
     * Verifies the hash for the given data.
     *
     * @param data The data for which the hash was generated.
     * @param hash The hash that needs to be verified.
     * @return true if the hash matches the data, false otherwise.
     */
    boolean verifyHash(String data, String hash);
}
