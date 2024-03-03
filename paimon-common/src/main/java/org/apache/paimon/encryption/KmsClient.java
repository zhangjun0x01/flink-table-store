/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.encryption;

import org.apache.paimon.options.Options;

import java.io.Closeable;
import java.io.Serializable;

/** KMS client interface, provide common operations for KMS. */
public interface KmsClient extends Serializable, Closeable {
    void configure(Options options);

    /**
     * Create the master key from the kms.
     *
     * @return the CreateKeyResult which contains the key id and the encrypted key.
     */
    CreateKeyResult createKey();

    /**
     * Get the encrypted key by the key id from the kms.
     *
     * @param keyId keyId
     * @return the encrypted key
     */
    byte[] getKey(String keyId);

    byte[] encryptDataKey(byte[] plaintextDataKey, byte[] kek);

    byte[] decryptDataKey(byte[] encryptedDataKey, byte[] kek);

    String identifier();

    /** Wraps the result for created key from kms. */
    class CreateKeyResult implements java.io.Serializable {
        private final String keyId;
        private final byte[] key;

        public CreateKeyResult(String keyId, byte[] key) {
            this.keyId = keyId;
            this.key = key;
        }

        public String keyId() {
            return keyId;
        }

        public byte[] key() {
            return key;
        }
    }
}
