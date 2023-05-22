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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.stream.Stream;

/** Utils for file reading and writing. */
public class FileUtils {

    public static final ForkJoinPool COMMON_IO_FORK_JOIN_POOL;

    // if we want to name threads in the fork join pool we need all these
    // see https://stackoverflow.com/questions/34303094/
    static {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory =
                pool -> {
                    ForkJoinWorkerThread worker =
                            ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    worker.setName("file-store-common-io-" + worker.getPoolIndex());
                    return worker;
                };
        COMMON_IO_FORK_JOIN_POOL =
                new ForkJoinPool(Runtime.getRuntime().availableProcessors(), factory, null, false);
    }

    public static <T> List<T> readListFromFile(
            FileIO fileIO,
            Path path,
            ObjectSerializer<T> serializer,
            FormatReaderFactory readerFactory)
            throws IOException {
        List<T> result = new ArrayList<>();
        createFormatReader(fileIO, readerFactory, path)
                .forEachRemaining(row -> result.add(serializer.fromRow(row)));
        return result;
    }

    /**
     * List versioned files for the directory.
     *
     * @return version stream
     */
    public static Stream<Long> listVersionedFiles(FileIO fileIO, Path dir, String prefix)
            throws IOException {
        if (!fileIO.exists(dir)) {
            return Stream.of();
        }

        FileStatus[] statuses = fileIO.listStatus(dir);

        if (statuses == null) {
            throw new RuntimeException(
                    String.format(
                            "The return value is null of the listStatus for the '%s' directory.",
                            dir));
        }

        return Arrays.stream(statuses)
                .map(FileStatus::getPath)
                .map(Path::getName)
                .filter(name -> name.startsWith(prefix))
                .map(name -> Long.parseLong(name.substring(prefix.length())));
    }

    public static RecordReader<InternalRow> createFormatReader(
            FileIO fileIO, FormatReaderFactory format, Path file) throws IOException {
        if (!fileIO.exists(file)) {
            throw new FileNotFoundException(
                    String.format(
                            "File '%s' not found, Possible causes: "
                                    + "1.snapshot expires too fast, you can configure 'snapshot.time-retained'"
                                    + " option with a larger value. "
                                    + "2.consumption is too slow, you can improve the performance of consumption"
                                    + " (For example, increasing parallelism).",
                            file));
        }

        return format.createReader(fileIO, file);
    }
}
