/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.ORCMetaReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.DataReader;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.impl.reader.StripePlanner;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.orc.impl.ReaderImpl.extractFileTail;

public class ORCMetaReaderImpl implements ORCMetaReader {
    private final Configuration configuration;
    private final FileSystem preheatFileSystem;

    public ORCMetaReaderImpl(Configuration configuration, FileSystem fileSystem) {
        this.configuration = configuration;
        this.preheatFileSystem = fileSystem;
    }

    @Override
    public PreheatFileMeta preheat(Path path) throws IOException {
        PreheatFileMeta result = new PreheatFileMeta(path);
        // 0. build and cache file reader
        ReaderImpl fileReader = null;
        try {
            fileReader = (ReaderImpl) OrcFile.createReader(path,
                OrcFile.readerOptions(configuration).filesystem(preheatFileSystem)
            );

            // 1. extract orc tail and cache it
            ByteBuffer footerBuffer = fileReader.getSerializedFileFooter();
            OrcTail orcTail = extractFileTail(footerBuffer, -1, -1);
            result.setPreheatTail(orcTail);

            // 2. build and cache each stripe metadata in file.
            Map<Long, PreheatStripeMeta> preheatContextMap = preheatStripe(path, fileReader, preheatFileSystem);
            result.setPreheatStripes(preheatContextMap);
        } finally {
            // prevent from IO resource leak
            if (fileReader != null) {
                fileReader.close();
            }
        }
        return result;
    }

    private Map<Long, PreheatStripeMeta> preheatStripe(Path path, ReaderImpl fileReader, FileSystem preheatFileSystem)
        throws IOException {
        Map<Long, PreheatStripeMeta> result = new ConcurrentHashMap<>();

        // 1. build data reader
        try (DataReader dataReader = buildDataReader(path, fileReader, preheatFileSystem)) {
            for (StripeInformation stripe : fileReader.getStripes()) {

                // 2. build stripe planner for each stripe.
                StripePlanner planner = buildStripePlanner(fileReader, dataReader);

                boolean[] allColumns = new boolean[fileReader.getSchema().getMaximumId() + 1];
                Arrays.fill(allColumns, true);

                // 3. planner parse meta info of Stripe
                // get info of data streams + index streams and cache it in planner object.
                planner.parseStripe(stripe, allColumns, null);

                // 4. get row index
                // NOTE: Stripe Planner will NOT cache the row indexes already fetched.
                OrcIndex index = planner.readRowIndex(allColumns, null);

                // 5. get stripe footer
                OrcProto.StripeFooter stripeFooter = dataReader.readStripeFooter(stripe);

                planner.clearDataReader();
                PreheatStripeMeta preheatStripeMeta = new PreheatStripeMeta(
                    stripe.getStripeId(), index, stripeFooter);

                result.put(stripe.getStripeId(), preheatStripeMeta);
            }
        }

        return result;
    }

    @NotNull
    private StripePlanner buildStripePlanner(ReaderImpl fileReader, DataReader dataReader) {
        int maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(configuration);
        boolean ignoreNonUtf8BloomFilter = OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(configuration);

        StripePlanner planner = new StripePlanner(
            fileReader.getSchema(),
            fileReader.getEncryption(),
            dataReader,
            fileReader.getWriterVersion(),
            ignoreNonUtf8BloomFilter,
            maxDiskRangeChunkLimit);
        return planner;
    }

    private DataReader buildDataReader(Path path, ReaderImpl fileReader, FileSystem fileSystem) throws IOException {
        int maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(configuration);
        Reader.Options options = fileReader.options();

        InStream.StreamOptions unencryptedOptions =
            InStream.options()
                .withCodec(OrcCodecPool.getCodec(fileReader.getCompressionKind()))
                .withBufferSize(fileReader.getCompressionSize());
        DataReaderProperties.Builder builder =
            DataReaderProperties.builder()
                .withCompression(unencryptedOptions)
                .withFileSystemSupplier(() -> fileSystem)
                .withPath(path)
                .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
                .withZeroCopy(options.getUseZeroCopy());
        FSDataInputStream file = fileSystem.open(path);
        if (file != null) {
            builder.withFile(file);
        }

        DataReader dataReader = RecordReaderUtils.createDefaultDataReader(
            builder.build());
        return dataReader;
    }

    @Override
    public void close() throws IOException {

    }
}
