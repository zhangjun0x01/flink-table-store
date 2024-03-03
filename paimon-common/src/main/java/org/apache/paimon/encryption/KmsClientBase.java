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

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/** Provide base operations for KMS. */
public abstract class KmsClientBase implements KmsClient {
    public static final int NONCE_LENGTH = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    @Override
    public byte[] encryptDataKey(byte[] plaintextDataKey, byte[] keyId) {
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            SecretKeySpec secretKeySpec = new SecretKeySpec(keyId, "AES");
            byte[] nonce = new byte[NONCE_LENGTH];
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, spec);
            return cipher.doFinal(plaintextDataKey);
        } catch (Exception e) {
            throw new RuntimeException("Encrypt data key error", e);
        }
    }

    @Override
    public byte[] decryptDataKey(byte[] ciphertext, byte[] kek) {

        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            SecretKeySpec secretKeySpec = new SecretKeySpec(kek, "AES");
            byte[] nonce = new byte[NONCE_LENGTH];
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, spec);
            return cipher.doFinal(ciphertext);
        } catch (Exception e) {
            throw new RuntimeException("Decrypt data key error", e);
        }
    }
}
