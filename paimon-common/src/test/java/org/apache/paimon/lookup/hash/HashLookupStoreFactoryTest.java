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

package org.apache.paimon.lookup.hash;

import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.MathUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HashLookupStoreFactory}. */
public class HashLookupStoreFactoryTest {

    @TempDir Path tempDir;

    private final RandomDataGenerator random = new RandomDataGenerator();

    private File file;
    private HashLookupStoreFactory factory;

    @BeforeEach
    public void setUp() throws IOException {
        this.factory =
                new HashLookupStoreFactory(
                        new CacheManager(1024, MemorySize.ofMebiBytes(1)), 0.75d);
        this.file = new File(tempDir.toFile(), UUID.randomUUID().toString());
        if (!file.createNewFile()) {
            throw new IOException("Can not create file: " + file);
        }
    }

    private byte[] toBytes(Object o) {
        return toBytes(o.toString());
    }

    private byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void testEmpty() throws IOException {
        HashLookupStoreWriter writer = factory.createWriter(file);
        writer.close();

        assertThat(file.exists()).isTrue();

        HashLookupStoreReader reader = factory.createReader(file);

        assertThat(reader.lookup(toBytes(1))).isNull();

        reader.close();
    }

    @Test
    public void testOneKey() throws IOException {
        HashLookupStoreWriter writer = factory.createWriter(file);
        writer.put(toBytes(1), toBytes("foo"));
        writer.close();

        HashLookupStoreReader reader = factory.createReader(file);
        assertThat(reader.lookup(toBytes(1))).isEqualTo(toBytes("foo"));
        reader.close();
    }

    @Test
    public void testTwoFirstKeyLength() throws IOException {
        int key1 = 1;
        int key2 = 245;

        // Write
        writeStore(file, new Object[] {key1, key2}, new Object[] {1, 6});

        // Read
        HashLookupStoreReader reader = factory.createReader(file);
        assertThat(reader.lookup(toBytes(key1))).isEqualTo(toBytes(1));
        assertThat(reader.lookup((toBytes(key2)))).isEqualTo(toBytes(6));
        assertThat(reader.lookup(toBytes(0))).isNull();
        assertThat(reader.lookup(toBytes(6))).isNull();
        assertThat(reader.lookup(toBytes(244))).isNull();
        assertThat(reader.lookup(toBytes(246))).isNull();
        assertThat(reader.lookup(toBytes(1245))).isNull();
    }

    @Test
    public void testKeyLengthGap() throws IOException {
        int key1 = 1;
        int key2 = 2450;

        // Write
        writeStore(file, new Object[] {key1, key2}, new Object[] {1, 6});

        // Read
        HashLookupStoreReader reader = factory.createReader(file);
        assertThat(reader.lookup(toBytes(key1))).isEqualTo(toBytes(1));
        assertThat(reader.lookup((toBytes(key2)))).isEqualTo(toBytes(6));
        assertThat(reader.lookup(toBytes(0))).isNull();
        assertThat(reader.lookup(toBytes(6))).isNull();
        assertThat(reader.lookup(toBytes(244))).isNull();
        assertThat(reader.lookup(toBytes(267))).isNull();
        assertThat(reader.lookup(toBytes(2449))).isNull();
        assertThat(reader.lookup(toBytes(2451))).isNull();
        assertThat(reader.lookup(toBytes(2454441))).isNull();
    }

    @Test
    public void testKeyLengthStartTwo() throws IOException {
        int key1 = 245;
        int key2 = 2450;

        // Write
        writeStore(file, new Object[] {key1, key2}, new Object[] {1, 6});

        // Read
        HashLookupStoreReader reader = factory.createReader(file);
        assertThat(reader.lookup(toBytes(key1))).isEqualTo(toBytes(1));
        assertThat(reader.lookup((toBytes(key2)))).isEqualTo(toBytes(6));
        assertThat(reader.lookup(toBytes(6))).isNull();
        assertThat(reader.lookup(toBytes(244))).isNull();
        assertThat(reader.lookup(toBytes(267))).isNull();
        assertThat(reader.lookup(toBytes(2449))).isNull();
        assertThat(reader.lookup(toBytes(2451))).isNull();
        assertThat(reader.lookup(toBytes(2454441))).isNull();
    }

    @Test
    public void testDataOnTwoBuffers() throws IOException {
        Object[] keys = new Object[] {1, 2, 3};
        Object[] values =
                new Object[] {
                    generateStringData(100), generateStringData(10000), generateStringData(100)
                };

        int byteSize = toBytes(values[0]).length + toBytes(values[1]).length;

        // Write
        writeStore(file, keys, values);

        // Read
        factory =
                new HashLookupStoreFactory(
                        new CacheManager(
                                MathUtils.roundDownToPowerOf2(byteSize - 100),
                                MemorySize.ofMebiBytes(1)),
                        0.75d);
        HashLookupStoreReader reader = factory.createReader(file);
        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }
    }

    @Test
    public void testDataSizeOnTwoBuffers() throws IOException {
        Object[] keys = new Object[] {1, 2, 3};
        Object[] values =
                new Object[] {
                    generateStringData(100), generateStringData(10000), generateStringData(100)
                };

        byte[] b1 = toBytes(values[0]);
        byte[] b2 = toBytes(values[1]);
        int byteSize = b1.length + b2.length;
        int sizeSize =
                VarLengthIntUtils.encodeInt(new DataOutputSerializer(4), b1.length)
                        + VarLengthIntUtils.encodeInt(new DataOutputSerializer(4), b2.length);

        // Write
        writeStore(file, keys, values);

        // Read
        factory =
                new HashLookupStoreFactory(
                        new CacheManager(
                                MathUtils.roundDownToPowerOf2(byteSize + sizeSize + 3),
                                MemorySize.ofMebiBytes(1)),
                        0.75d);
        HashLookupStoreReader reader = factory.createReader(file);
        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }
    }

    @Test
    public void testReadStringToString() throws IOException {
        testReadKeyToString(generateStringKeys(100));
    }

    @Test
    public void testReadIntToString() throws IOException {
        testReadKeyToString(generateIntKeys(100));
    }

    @Test
    public void testReadDoubleToString() throws IOException {
        testReadKeyToString(generateDoubleKeys(100));
    }

    @Test
    public void testReadLongToString() throws IOException {
        testReadKeyToString(generateLongKeys(100));
    }

    @Test
    public void testReadStringToInt() throws IOException {
        testReadKeyToInt(generateStringKeys(100));
    }

    @Test
    public void testReadByteToInt() throws IOException {
        testReadKeyToInt(generateByteKeys(100));
    }

    @Test
    public void testReadIntToInt() throws IOException {
        testReadKeyToInt(generateIntKeys(100));
    }

    @Test
    public void testReadCompoundToString() throws IOException {
        testReadKeyToString(generateCompoundKeys(100));
    }

    @Test
    public void testReadCompoundByteToString() throws IOException {
        testReadKeyToString(new Object[] {generateCompoundByteKey()});
    }

    @Test
    public void testCacheExpiration() throws IOException {
        int len = 1000;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Object[] keys = new Object[len];
        Object[] values = new Object[len];
        for (int i = 0; i < len; i++) {
            keys[i] = rnd.nextInt();
            values[i] = generateStringData(100);
        }

        // Write
        writeStore(file, keys, values);

        // Read
        factory = new HashLookupStoreFactory(new CacheManager(1024, new MemorySize(8096)), 0.75d);
        HashLookupStoreReader reader = factory.createReader(file);
        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }
    }

    @Test
    public void testIterate() throws IOException {
        Integer[] keys = generateIntKeys(100);
        String[] values = generateStringData(keys.length, 12);

        // Write
        writeStore(file, keys, values);

        // Sets
        Set<Integer> keysSet = new HashSet<>(Arrays.asList(keys));
        Set<String> valuesSet = new HashSet<>(Arrays.asList(values));

        // Read
        HashLookupStoreReader reader = factory.createReader(file);
        Iterator<Map.Entry<byte[], byte[]>> itr = reader.iterator();
        for (int i = 0; i < keys.length; i++) {
            assertThat(itr.hasNext()).isTrue();
            Map.Entry<byte[], byte[]> entry = itr.next();
            assertThat(entry).isNotNull();
            assertThat(keysSet.remove(Integer.valueOf(new String(entry.getKey())))).isTrue();
            assertThat(valuesSet.remove(new String(entry.getValue()))).isTrue();

            assertThat(reader.lookup(entry.getKey())).isEqualTo(entry.getValue());
        }
        assertThat(itr.hasNext()).isFalse();
        reader.close();

        assertThat(keysSet).isEmpty();
        assertThat(valuesSet).isEmpty();
    }

    // UTILITY

    private void testReadKeyToString(Object[] keys) throws IOException {
        // Write
        Object[] values = generateStringData(keys.length, 10);
        writeStore(file, keys, values);

        // Read
        HashLookupStoreReader reader = factory.createReader(file);

        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }

        reader.close();
    }

    private void testReadKeyToInt(Object[] keys) throws IOException {
        // Write
        Integer[] values = generateIntData(keys.length);
        writeStore(file, keys, values);

        // Read
        HashLookupStoreReader reader = factory.createReader(file);

        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }

        reader.close();
    }

    private void writeStore(File location, Object[] keys, Object[] values) throws IOException {
        HashLookupStoreWriter writer = factory.createWriter(location);
        for (int i = 0; i < keys.length; i++) {
            writer.put(toBytes(keys[i]), toBytes(values[i]));
        }
        writer.close();
    }

    private Integer[] generateIntKeys(int count) {
        Integer[] res = new Integer[count];
        for (int i = 0; i < count; i++) {
            res[i] = i;
        }
        return res;
    }

    private String[] generateStringKeys(int count) {
        String[] res = new String[count];
        for (int i = 0; i < count; i++) {
            res[i] = i + "";
        }
        return res;
    }

    private Byte[] generateByteKeys(int count) {
        if (count > 127) {
            throw new RuntimeException("Too large range");
        }
        Byte[] res = new Byte[count];
        for (int i = 0; i < count; i++) {
            res[i] = (byte) i;
        }
        return res;
    }

    private Double[] generateDoubleKeys(int count) {
        Double[] res = new Double[count];
        for (int i = 0; i < count; i++) {
            res[i] = (double) i;
        }
        return res;
    }

    private Long[] generateLongKeys(int count) {
        Long[] res = new Long[count];
        for (int i = 0; i < count; i++) {
            res[i] = (long) i;
        }
        return res;
    }

    private Object[] generateCompoundKeys(int count) {
        Object[] res = new Object[count];
        Random random = new Random(345);
        for (int i = 0; i < count; i++) {
            Object[] k = new Object[] {(byte) random.nextInt(10), i};
            res[i] = k;
        }
        return res;
    }

    private Object[] generateCompoundByteKey() {
        Object[] res = new Object[2];
        res[0] = (byte) 6;
        res[1] = (byte) 0;
        return res;
    }

    private String generateStringData(int letters) {
        return random.nextHexString(letters);
    }

    private String[] generateStringData(int count, int letters) {
        String[] res = new String[count];
        for (int i = 0; i < count; i++) {
            res[i] = random.nextHexString(letters);
        }
        return res;
    }

    private Integer[] generateIntData(int count) {
        Integer[] res = new Integer[count];
        Random random = new Random(count + 34593263544354353L);
        for (int i = 0; i < count; i++) {
            res[i] = random.nextInt(1000000);
        }
        return res;
    }
}
