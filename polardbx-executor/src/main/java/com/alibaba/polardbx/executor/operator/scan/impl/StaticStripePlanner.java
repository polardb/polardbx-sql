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

import com.google.protobuf.CodedInputStream;
import org.apache.orc.DataReader;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.BufferChunkList;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.ReaderEncryptionVariant;
import org.apache.orc.impl.reader.StreamInformation;

import java.io.IOException;
import java.security.Key;
import java.util.Arrays;
import java.util.Map;

public class StaticStripePlanner {
    /**
     * Parse a new stripe. Resets the current stripe state.
     *
     * @param columnInclude an array with true for each column to read
     * @return this for method chaining
     */
    public static StreamManager parseStripe(
        StripeContext stripeContext,
        boolean[] columnInclude,
        OrcProto.StripeFooter footer) {
        StripeInformation stripe = stripeContext.getStripeInformation();
        stripeContext.setCurrentStripeId(stripe.getStripeId());
        stripeContext.setOriginalStripeId(stripe.getEncryptionStripeId());
        stripeContext.setWriterTimezone(footer.getWriterTimezone());

        StreamManager streamManager = new StreamManager();
        streamManager.setStripeContext(stripeContext);

        // fill the encoding info of stripe context.
        buildEncodings(stripeContext, footer, columnInclude);
        findStreams(streamManager, stripeContext, stripe.getOffset(), footer, columnInclude);

        // figure out whether each column has null values in this stripe
        boolean[] hasNull = stripeContext.getHasNull();
        Arrays.fill(hasNull, false);
        for (StreamInformation stream : streamManager.getDataStreams()) {
            if (stream.kind == OrcProto.Stream.Kind.PRESENT) {
                hasNull[stream.column] = true;
            }
        }
        return streamManager;
    }

    public static OrcIndex readRowIndex(StreamManager streamManager,
                                        StripeContext stripeContext,
                                        DataReader dataReader) throws IOException {
        OrcProto.Stream.Kind[] bloomFilterKinds = stripeContext.getBloomFilterKinds();
        TypeDescription schema = stripeContext.getSchema();

        int typeCount = schema.getMaximumId() + 1;
        OrcIndex output = new OrcIndex(new OrcProto.RowIndex[typeCount],
            new OrcProto.Stream.Kind[typeCount],
            new OrcProto.BloomFilterIndex[typeCount],
            new OrcProto.BitmapIndex[typeCount]);

        System.arraycopy(bloomFilterKinds, 0, output.getBloomFilterKinds(), 0,
            bloomFilterKinds.length);
        BufferChunkList ranges = planIndexReading(streamManager, stripeContext);
        dataReader.readFileData(ranges, false);
        OrcProto.RowIndex[] indexes = output.getRowGroupIndex();
        OrcProto.BloomFilterIndex[] blooms = output.getBloomFilterIndex();
        for (StreamInformation stream : streamManager.getIndexStreams()) {
            int column = stream.column;
            if (stream.firstChunk != null) {
                CodedInputStream data = InStream.createCodedInputStream(InStream.create(
                    "index", stream.firstChunk, stream.offset,
                    stream.length, getStreamOptions(
                        stripeContext,
                        column, stream.kind)));
                switch (stream.kind) {
                case ROW_INDEX:
                    indexes[column] = OrcProto.RowIndex.parseFrom(data);
                    break;
                case BLOOM_FILTER:
                case BLOOM_FILTER_UTF8:
                    break;
                default:
                    break;
                }
            }
        }
        return output;
    }

    /**
     * Get the planned IO ranges by requested columns and row-groups
     *
     * @param stripeContext stripe-level context.
     * @param streamManager stream info of multiple columns.
     * @param streamOptions stream options.
     * @param index preheated row-indexes in this stripe.
     * @param rowGroupIncludeMap Mapping from column-id to row-group bitmap.
     * @param selectedColumns selected columns.
     * @return Planned IO ranges info for the IO processing.
     */
    public static BufferChunkList planGroupsInColumn(
        StripeContext stripeContext,
        StreamManager streamManager,
        InStream.StreamOptions streamOptions,
        OrcIndex index,
        Map<Integer, boolean[]> rowGroupIncludeMap,
        boolean[] selectedColumns) {
        BufferChunkList chunks = new BufferChunkList();

        OrcProto.RowIndex[] rowIndex = index.getRowGroupIndex();
        boolean isCompressed = streamOptions.getCodec() != null;
        int bufferSize = streamOptions.getBufferSize();

        for (StreamInformation stream : streamManager.getDataStreams()) {
            // Check the column id.
            // The count of matched streams is >= 1 because there are many stream kind in one column.
            if (stream.column < selectedColumns.length && selectedColumns[stream.column]) {
                processStream(
                    stripeContext,
                    stream,
                    chunks,
                    rowIndex,
                    0,
                    rowGroupIncludeMap.get(stream.column),
                    isCompressed,
                    bufferSize);
            }
        }

        return chunks;
    }

    public static BufferChunkList planGroupsInColumn(
        StripeContext stripeContext,
        StreamManager streamManager,
        InStream.StreamOptions streamOptions,
        OrcIndex index,
        boolean[] rowGroupInclude,
        int columnId) {
        BufferChunkList chunks = new BufferChunkList();

        OrcProto.RowIndex[] rowIndex = index.getRowGroupIndex();
        boolean isCompressed = streamOptions.getCodec() != null;
        int bufferSize = streamOptions.getBufferSize();

        for (StreamInformation stream : streamManager.getDataStreams()) {
            // Check the column id.
            // The count of matched streams is >= 1 because there are many stream kind in one column.
            if (stream.column == columnId) {

                processStream(stripeContext, stream, chunks, rowIndex, 0,
                    rowGroupInclude, isCompressed, bufferSize);
            }
        }

        return chunks;
    }

    private static void processStream(
        StripeContext stripeContext,
        StreamInformation stream,
        BufferChunkList result,
        OrcProto.RowIndex[] rowIndex,
        int startGroup,
        boolean[] includedRowGroups,
        boolean isCompressed,
        int bufferSize) {

        // check existence of row-groups.
        if (!hasTrue(includedRowGroups)) {
            return;
        }

        OrcProto.ColumnEncoding[] encodings = stripeContext.getEncodings();
        TypeDescription schema = stripeContext.getSchema();
        boolean[] hasNull = stripeContext.getHasNull();

        if (RecordReaderUtils.isDictionary(stream.kind, encodings[stream.column])) {
            addChunk1(stripeContext, result, stream, stream.offset, stream.length);
        } else {
            int column = stream.column;
            OrcProto.RowIndex ri = rowIndex[column];
            TypeDescription.Category kind = schema.findSubtype(column).getCategory();
            long alreadyRead = 0;
            for (int group = startGroup; group < includedRowGroups.length; ++group) {
                if (includedRowGroups[group]) {
                    // find the last group that is selected
                    int endGroup = group;
                    while (endGroup < includedRowGroups.length - 1 &&
                        includedRowGroups[endGroup + 1]) {
                        endGroup += 1;
                    }
                    int posn = RecordReaderUtils.getIndexPosition(
                        encodings[stream.column].getKind(), kind, stream.kind,
                        isCompressed, hasNull[column]);
                    long start = Math.max(alreadyRead,
                        stream.offset + (group == 0 ? 0 : ri.getEntry(group).getPositions(posn)));
                    long end = stream.offset;
                    if (endGroup == includedRowGroups.length - 1) {
                        end += stream.length;
                    } else {
                        long nextGroupOffset = ri.getEntry(endGroup + 1).getPositions(posn);
                        end += RecordReaderUtils.estimateRgEndOffset(isCompressed,
                            bufferSize, false, nextGroupOffset,
                            stream.length);
                    }
                    if (alreadyRead < end) {
                        addChunk1(stripeContext, result, stream, start, end - start);
                        alreadyRead = end;
                    }
                    group = endGroup;
                }
            }
        }
    }

    private static void addChunk1(
        StripeContext stripeContext,
        BufferChunkList list,
        StreamInformation stream,
        long offset, long length) {
        long maxBufferSize = stripeContext.getMaxBufferSize();
        while (length > 0) {
            long thisLen = Math.min(length, maxBufferSize);
            BufferChunk chunk = new BufferChunk(offset, (int) thisLen);
            if (stream.firstChunk == null) {
                stream.firstChunk = chunk;
            }
            list.add(chunk);
            offset += thisLen;
            length -= thisLen;
        }
    }

    // NOTE: we must ensure that the groupId is monotonically increasing.
    public static BufferChunkList planNextGroup(
        StripeContext stripeContext,
        StreamManager streamManager,
        InStream.StreamOptions streamOptions,
        OrcIndex index,
        int groupId,
        boolean isLastGroup,
        boolean[] selectedColumns) {
        BufferChunkList chunks = new BufferChunkList();

        boolean isCompressed = streamOptions.getCodec() != null;
        int bufferSize = streamOptions.getBufferSize();
        OrcProto.RowIndex[] rowIndex = index.getRowGroupIndex();

        for (StreamInformation stream : streamManager.getDataStreams()) {
            // check the column id in selected columns bitmap
            if (stream.column < selectedColumns.length && selectedColumns[stream.column]) {
                processStream(
                    stripeContext,
                    stream,
                    chunks,
                    rowIndex,
                    groupId,
                    isLastGroup,
                    isCompressed,
                    bufferSize);
            }
        }

        return chunks;
    }

    private static void processStream(
        StripeContext stripeContext,
        StreamInformation stream,
        BufferChunkList result,
        OrcProto.RowIndex[] rowIndex,
        int group,
        boolean isLastGroup,
        boolean isCompressed,
        int bufferSize) {
        OrcProto.ColumnEncoding[] encodings = stripeContext.getEncodings();
        TypeDescription schema = stripeContext.getSchema();
        boolean[] hasNull = stripeContext.getHasNull();
        long maxBufferSize = stripeContext.getMaxBufferSize();

        if (RecordReaderUtils.isDictionary(stream.kind, encodings[stream.column])) {
            addChunk(result, stream, maxBufferSize, stream.offset, stream.length);
        } else {
            int column = stream.column;
            OrcProto.RowIndex ri = rowIndex[column];
            TypeDescription.Category kind = schema.findSubtype(column).getCategory();

            int position = RecordReaderUtils.getIndexPosition(
                encodings[stream.column].getKind(), kind, stream.kind,
                isCompressed, hasNull[column]);
            long start = stream.offset + (group == 0 ? 0 : ri.getEntry(group).getPositions(position));
            long end = stream.offset;
            if (isLastGroup) {
                end += stream.length;
            } else {
                long nextGroupOffset = ri.getEntry(group + 1).getPositions(position);
                end += RecordReaderUtils.estimateRgEndOffset(isCompressed,
                    bufferSize, false, nextGroupOffset,
                    stream.length);
            }

            addChunk(result, stream, maxBufferSize, start, end - start);
        }
    }

    private static void addChunk(BufferChunkList list, StreamInformation stream, long maxBufferSize,
                                 long offset, long length) {
        while (length > 0) {
            long thisLen = Math.min(length, maxBufferSize);
            BufferChunk chunk = new BufferChunk(offset, (int) thisLen);
            if (stream.firstChunk == null) {
                stream.firstChunk = chunk;
            } else {
                // If we handle chunks in stripe-level at-a-time, we don't need this logic.
                // Because all chunks in a stream will be fetched at once.
                stream.firstChunk.next = chunk;
            }
            list.add(chunk);
            offset += thisLen;
            length -= thisLen;
        }
    }

    private static boolean hasSomeRowGroups(boolean[] includedRowGroups) {
        for (boolean include : includedRowGroups) {
            if (include) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the stream options for a stream in a stripe.
     *
     * @param column the column we are reading
     * @param kind the stream kind we are reading
     * @return a new stream options to read the given column
     */
    public static InStream.StreamOptions getStreamOptions(
        StripeContext stripeContext,
        int column,
        OrcProto.Stream.Kind kind
    ) throws IOException {
        ReaderEncryption encryption = stripeContext.getEncryption();
        long currentStripeId = stripeContext.getCurrentStripeId();
        long originalStripeId = stripeContext.getOriginalStripeId();

        ReaderEncryptionVariant variant = encryption.getVariant(column);
        InStream.StreamOptions compression = stripeContext.getStreamOptions();
        if (variant == null) {
            return compression;
        } else {
            EncryptionAlgorithm algorithm = variant.getKeyDescription().getAlgorithm();
            byte[] iv = new byte[algorithm.getIvLength()];
            Key key = variant.getStripeKey(currentStripeId);
            CryptoUtils.modifyIvForStream(column, kind, originalStripeId).accept(iv);
            return new InStream.StreamOptions(compression)
                .withEncryption(algorithm, key, iv);
        }
    }

    /**
     * The length of encoding[] array is equal to column count,
     * and the encoding of column that not included is null.
     */
    public static OrcProto.ColumnEncoding[] buildEncodings(
        ReaderEncryption encryption,
        boolean[] columnInclude,
        OrcProto.StripeFooter footer) {
        OrcProto.ColumnEncoding[] encodings =
            new OrcProto.ColumnEncoding[columnInclude.length];

        for (int c = 0; c < encodings.length; ++c) {
            if (columnInclude == null || columnInclude[c]) {
                ReaderEncryptionVariant variant = encryption.getVariant(c);
                if (variant == null) {
                    encodings[c] = footer.getColumns(c);
                } else {
                    int subColumn = c - variant.getRoot().getId();
                    encodings[c] = footer.getEncryption(variant.getVariantId())
                        .getEncoding(subColumn);
                }
            }
        }
        return encodings;
    }

    // fill the encoding info of stripe context.
    private static void buildEncodings(
        StripeContext stripeContext,
        OrcProto.StripeFooter footer,
        boolean[] columnInclude) {

        OrcProto.ColumnEncoding[] encodings = stripeContext.getEncodings();
        ReaderEncryption encryption = stripeContext.getEncryption();

        for (int c = 0; c < encodings.length; ++c) {
            if (columnInclude == null || columnInclude[c]) {
                ReaderEncryptionVariant variant = encryption.getVariant(c);
                if (variant == null) {
                    encodings[c] = footer.getColumns(c);
                } else {
                    int subColumn = c - variant.getRoot().getId();
                    encodings[c] = footer.getEncryption(variant.getVariantId())
                        .getEncoding(subColumn);
                }
            }
        }
    }

    /**
     * Find the complete list of streams.
     *
     * @param streamStart the starting offset of streams in the file
     * @param footer the footer for the stripe
     * @param columnInclude which columns are being read
     */
    private static void findStreams(
        StreamManager streamManager,
        StripeContext stripeContext,
        long streamStart,
        OrcProto.StripeFooter footer,
        boolean[] columnInclude) {
        OrcProto.Stream.Kind[] bloomFilterKinds = stripeContext.getBloomFilterKinds();
        ReaderEncryption encryption = stripeContext.getEncryption();

        long currentOffset = streamStart;
        Arrays.fill(bloomFilterKinds, null);
        for (OrcProto.Stream stream : footer.getStreamsList()) {
            currentOffset += handleStream(streamManager, stripeContext, currentOffset, columnInclude, stream, null);
        }

        // Add the encrypted streams that we are using
        for (ReaderEncryptionVariant variant : encryption.getVariants()) {
            int variantId = variant.getVariantId();
            OrcProto.StripeEncryptionVariant stripeVariant =
                footer.getEncryption(variantId);
            for (OrcProto.Stream stream : stripeVariant.getStreamsList()) {
                currentOffset +=
                    handleStream(streamManager, stripeContext, currentOffset, columnInclude, stream, variant);
            }
        }
    }

    /**
     * For each stream, decide whether to include it in the list of streams.
     *
     * @param offset the position in the file for this stream
     * @param columnInclude which columns are being read
     * @param stream the stream to consider
     * @param variant the variant being read
     * @return the offset for the next stream
     */
    private static long handleStream(
        StreamManager streamManager,
        StripeContext stripeContext,
        long offset,
        boolean[] columnInclude,
        OrcProto.Stream stream,
        ReaderEncryptionVariant variant) {
        OrcProto.Stream.Kind[] bloomFilterKinds = stripeContext.getBloomFilterKinds();
        ReaderEncryption encryption = stripeContext.getEncryption();
        boolean ignoreNonUtf8BloomFilter = stripeContext.isIgnoreNonUtf8BloomFilter();
        TypeDescription schema = stripeContext.getSchema();
        OrcFile.WriterVersion version = stripeContext.getVersion();

        int column = stream.getColumn();
        if (stream.hasKind()) {
            OrcProto.Stream.Kind kind = stream.getKind();

            if (kind == OrcProto.Stream.Kind.ENCRYPTED_INDEX ||
                kind == OrcProto.Stream.Kind.ENCRYPTED_DATA) {
                // Ignore the placeholders that shouldn't count toward moving the
                // offsets.
                return 0;
            }

            if (columnInclude[column] && encryption.getVariant(column) == variant) {
                // Ignore any broken bloom filters unless the user forced us to use
                // them.
                if (kind != OrcProto.Stream.Kind.BLOOM_FILTER ||
                    !ignoreNonUtf8BloomFilter ||
                    !hadBadBloomFilters(schema.findSubtype(column).getCategory(),
                        version)) {
                    // record what kind of bloom filters we are using
                    if (kind == OrcProto.Stream.Kind.BLOOM_FILTER_UTF8 ||
                        kind == OrcProto.Stream.Kind.BLOOM_FILTER) {
                        bloomFilterKinds[column] = kind;
                    }
                    StreamInformation info =
                        new StreamInformation(kind, column, offset, stream.getLength());
                    switch (StreamName.getArea(kind)) {
                    case DATA:
                        streamManager.getDataStreams().add(info);
                        break;
                    case INDEX:
                        streamManager.getIndexStreams().add(info);
                        break;
                    default:
                    }
                    streamManager.getStreams().put(new StreamName(column, kind), info);
                }
            }
        }
        return stream.getLength();
    }

    private static boolean hadBadBloomFilters(TypeDescription.Category category,
                                              OrcFile.WriterVersion version) {
        switch (category) {
        case STRING:
        case CHAR:
        case VARCHAR:
            return !version.includes(OrcFile.WriterVersion.HIVE_12055);
        case DECIMAL:
            // fixed by ORC-101, but ORC-101 changed stream kind to BLOOM_FILTER_UTF8
            return true;
        case TIMESTAMP:
            return !version.includes(OrcFile.WriterVersion.ORC_135);
        default:
            return false;
        }
    }

    /**
     * Plans the list of disk ranges that the given stripe needs to read the
     * indexes. All of the positions are relative to the start of the stripe.
     *
     * @return a list of merged disk ranges to read
     */
    private static BufferChunkList planIndexReading(StreamManager streamManager, StripeContext stripeContext) {
        OrcProto.Stream.Kind[] bloomFilterKinds = stripeContext.getBloomFilterKinds();
        BufferChunkList result = new BufferChunkList();
        for (StreamInformation stream : streamManager.getIndexStreams()) {
            switch (stream.kind) {
            case ROW_INDEX:
                addChunk(result, stream, stripeContext.getMaxBufferSize(), stream.offset, stream.length);
                break;
            case BLOOM_FILTER:
            case BLOOM_FILTER_UTF8:
                break;
            default:
                // PASS
                break;
            }
        }
        return result;
    }

    private static boolean hasTrue(boolean[] rowGroups) {
        if (rowGroups == null) {
            return false;
        }
        for (int i = 0; i < rowGroups.length; i++) {
            if (rowGroups[i]) {
                return true;
            }
        }
        return false;
    }

}
