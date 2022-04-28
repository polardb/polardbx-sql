/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl.reader;

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
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class handles parsing the stripe information and handling the necessary
 * filtering and selection.
 *
 * It supports:
 * <ul>
 *   <li>column projection</li>
 *   <li>row group selection</li>
 *   <li>encryption</li>
 * </ul>
 */
public class StripePlanner {
  // global information
  private final TypeDescription schema;
  private final OrcFile.WriterVersion version;
  private final OrcProto.ColumnEncoding[] encodings;
  private final ReaderEncryption encryption;
  private final DataReader dataReader;
  private final boolean ignoreNonUtf8BloomFilter;
  private final long maxBufferSize;

  // specific to the current stripe
  private String writerTimezone;
  private long currentStripeId;
  private long originalStripeId;
  private Map<StreamName, StreamInformation> streams = new HashMap<>();
  // the index streams sorted by offset
  private final List<StreamInformation> indexStreams = new ArrayList<>();
  // the data streams sorted by offset
  private final List<StreamInformation> dataStreams = new ArrayList<>();
  private final OrcProto.Stream.Kind[] bloomFilterKinds;
  // does each column have a null stream?
  private final boolean[] hasNull;

  /**
   * Create a stripe parser.
   * @param schema the file schema
   * @param encryption the encryption information
   * @param dataReader the underlying data reader
   * @param version the file writer version
   * @param ignoreNonUtf8BloomFilter ignore old non-utf8 bloom filters
   * @param maxBufferSize the largest single buffer to use
   */
  public StripePlanner(TypeDescription schema,
                       ReaderEncryption encryption,
                       DataReader dataReader,
                       OrcFile.WriterVersion version,
                       boolean ignoreNonUtf8BloomFilter,
                       long maxBufferSize) {
    this.schema = schema;
    this.version = version;
    encodings = new OrcProto.ColumnEncoding[schema.getMaximumId()+1];
    this.encryption = encryption;
    this.dataReader = dataReader;
    this.ignoreNonUtf8BloomFilter = ignoreNonUtf8BloomFilter;
    bloomFilterKinds = new OrcProto.Stream.Kind[schema.getMaximumId() + 1];
    hasNull = new boolean[schema.getMaximumId() + 1];
    this.maxBufferSize = maxBufferSize;
  }

  public StripePlanner(StripePlanner old) {
    this(old.schema, old.encryption, old.dataReader, old.version,
        old.ignoreNonUtf8BloomFilter, old.maxBufferSize);
  }

  /**
   * Parse a new stripe. Resets the current stripe state.
   * @param stripe the new stripe
   * @param columnInclude an array with true for each column to read
   * @return this for method chaining
   */
  public StripePlanner parseStripe(StripeInformation stripe,
                                   boolean[] columnInclude) throws IOException {
    OrcProto.StripeFooter footer = dataReader.readStripeFooter(stripe);
    currentStripeId = stripe.getStripeId();
    originalStripeId = stripe.getEncryptionStripeId();
    writerTimezone = footer.getWriterTimezone();
    streams.clear();
    dataStreams.clear();
    indexStreams.clear();
    buildEncodings(footer, columnInclude);
    findStreams(stripe.getOffset(), footer, columnInclude);
    // figure out whether each column has null values in this stripe
    Arrays.fill(hasNull, false);
    for(StreamInformation stream: dataStreams) {
      if (stream.kind == OrcProto.Stream.Kind.PRESENT) {
        hasNull[stream.column] = true;
      }
    }
    return this;
  }

  /**
   * Read the stripe data from the file.
   * @param index null for no row filters or the index for filtering
   * @param rowGroupInclude null for all of the rows or an array with boolean
   *                       for each row group in the current stripe.
   * @param forceDirect should direct buffers be created?
   * @return the buffers that were read
   */
  public BufferChunkList readData(OrcIndex index,
                                boolean[] rowGroupInclude,
                                boolean forceDirect) throws IOException {
    BufferChunkList chunks = (index == null || rowGroupInclude == null)
        ? planDataReading() : planPartialDataReading(index, rowGroupInclude);
    dataReader.readFileData(chunks, forceDirect);
    return chunks;
  }

  public String getWriterTimezone() {
    return writerTimezone;
  }

  /**
   * Get the stream for the given name.
   * It is assumed that the name does <b>not</b> have the encryption set,
   * because the TreeReader's don't know if they are reading encrypted data.
   * Assumes that readData has already been called on this stripe.
   * @param name the column/kind of the stream
   * @return a new stream with the options set correctly
   */
  public InStream getStream(StreamName name) throws IOException {
    StreamInformation stream = streams.get(name);
    return stream == null ? null
      : InStream.create(name, stream.firstChunk, stream.offset, stream.length,
            getStreamOptions(stream.column, stream.kind));
  }

  /**
   * Release all of the buffers for the current stripe.
   */
  public void clearStreams() {
    if (dataReader.isTrackingDiskRanges()) {
      for (StreamInformation stream : indexStreams) {
        stream.releaseBuffers(dataReader);
      }
      for (StreamInformation stream : dataStreams) {
        stream.releaseBuffers(dataReader);
      }
    }
    indexStreams.clear();
    dataStreams.clear();
    streams.clear();
  }

  /**
   * Get the stream options for a stream in a stripe.
   * @param column the column we are reading
   * @param kind the stream kind we are reading
   * @return a new stream options to read the given column
   */
  private InStream.StreamOptions getStreamOptions(int column,
                                                  OrcProto.Stream.Kind kind
                                                  ) throws IOException {
    ReaderEncryptionVariant variant = encryption.getVariant(column);
    InStream.StreamOptions compression = dataReader.getCompressionOptions();
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

  public OrcProto.ColumnEncoding getEncoding(int column) {
    return encodings[column];
  }

  private void buildEncodings(OrcProto.StripeFooter footer,
                              boolean[] columnInclude) {
    for(int c=0; c < encodings.length; ++c) {
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
   * For each stream, decide whether to include it in the list of streams.
   * @param offset the position in the file for this stream
   * @param columnInclude which columns are being read
   * @param stream the stream to consider
   * @param variant the variant being read
   * @return the offset for the next stream
   */
  private long handleStream(long offset,
                            boolean[] columnInclude,
                            OrcProto.Stream stream,
                            ReaderEncryptionVariant variant) {
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
            dataStreams.add(info);
            break;
          case INDEX:
            indexStreams.add(info);
            break;
          default:
          }
          streams.put(new StreamName(column, kind), info);
        }
      }
    }
    return stream.getLength();
  }

  /**
   * Find the complete list of streams.
   * @param streamStart the starting offset of streams in the file
   * @param footer the footer for the stripe
   * @param columnInclude which columns are being read
   */
  private void findStreams(long streamStart,
                           OrcProto.StripeFooter footer,
                           boolean[] columnInclude) throws IOException {
    long currentOffset = streamStart;
    Arrays.fill(bloomFilterKinds, null);
    for(OrcProto.Stream stream: footer.getStreamsList()) {
      currentOffset += handleStream(currentOffset, columnInclude, stream, null);
    }

    // Add the encrypted streams that we are using
    for(ReaderEncryptionVariant variant: encryption.getVariants()) {
      int variantId = variant.getVariantId();
      OrcProto.StripeEncryptionVariant stripeVariant =
          footer.getEncryption(variantId);
      for(OrcProto.Stream stream: stripeVariant.getStreamsList()) {
        currentOffset += handleStream(currentOffset, columnInclude, stream, variant);
      }
    }
  }

  /**
   * Read and parse the indexes for the current stripe.
   * @param sargColumns the columns we can use bloom filters for
   * @param output an OrcIndex to reuse
   * @return the indexes for the required columns
   */
  public OrcIndex readRowIndex(boolean[] sargColumns,
                               OrcIndex output) throws IOException {
    int typeCount = schema.getMaximumId() + 1;
    if (output == null) {
      output = new OrcIndex(new OrcProto.RowIndex[typeCount],
          new OrcProto.Stream.Kind[typeCount],
          new OrcProto.BloomFilterIndex[typeCount]);
    }
    System.arraycopy(bloomFilterKinds, 0, output.getBloomFilterKinds(), 0,
        bloomFilterKinds.length);
    BufferChunkList ranges = planIndexReading(sargColumns);
    dataReader.readFileData(ranges, false);
    OrcProto.RowIndex[] indexes = output.getRowGroupIndex();
    OrcProto.BloomFilterIndex[] blooms = output.getBloomFilterIndex();
    for(StreamInformation stream: indexStreams) {
      int column = stream.column;
      if (stream.firstChunk != null) {
        CodedInputStream data = InStream.createCodedInputStream(InStream.create(
            "index", stream.firstChunk, stream.offset,
            stream.length, getStreamOptions(column, stream.kind)));
        switch (stream.kind) {
        case ROW_INDEX:
          indexes[column] = OrcProto.RowIndex.parseFrom(data);
          break;
        case BLOOM_FILTER:
        case BLOOM_FILTER_UTF8:
          if (sargColumns != null && sargColumns[column]) {
            blooms[column] = OrcProto.BloomFilterIndex.parseFrom(data);
          }
          break;
        default:
          break;
        }
      }
    }
    return output;
  }

  private void addChunk(BufferChunkList list, StreamInformation stream,
                        long offset, long length) {
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

  /**
   * Plans the list of disk ranges that the given stripe needs to read the
   * indexes. All of the positions are relative to the start of the stripe.
   * @param bloomFilterColumns true for the columns (indexed by file columns) that
   *                    we need bloom filters for
   * @return a list of merged disk ranges to read
   */
  private BufferChunkList planIndexReading(boolean[] bloomFilterColumns) {
    BufferChunkList result = new BufferChunkList();
    for(StreamInformation stream: indexStreams) {
      switch (stream.kind) {
      case ROW_INDEX:
        addChunk(result, stream, stream.offset, stream.length);
        break;
      case BLOOM_FILTER:
      case BLOOM_FILTER_UTF8:
        if (bloomFilterColumns[stream.column] &&
                bloomFilterKinds[stream.column] == stream.kind) {
          addChunk(result, stream, stream.offset, stream.length);
        }
        break;
      default:
        // PASS
        break;
      }
    }
    return result;
  }

  /**
   * Plans the list of disk ranges that the given stripe needs to read the
   * data.
   * @return a list of merged disk ranges to read
   */
  private BufferChunkList planDataReading() {
    BufferChunkList result = new BufferChunkList();
    for(StreamInformation stream: dataStreams) {
      addChunk(result, stream, stream.offset, stream.length);
    }
    return result;
  }

  static boolean hadBadBloomFilters(TypeDescription.Category category,
                                    OrcFile.WriterVersion version) {
    switch(category) {
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

  private static boolean hasSomeRowGroups(boolean[] includedRowGroups) {
    for(boolean include: includedRowGroups) {
      if (include) {
        return true;
      }
    }
    return false;
  }

  /**
   * Plan the ranges of the file that we need to read given the list of
   * columns and row groups.
   *
   * @param index              the index to use for offsets
   * @param includedRowGroups  which row groups are needed
   * @return the list of disk  ranges that will be loaded
   */
  private BufferChunkList planPartialDataReading(OrcIndex index,
                                                 @NotNull boolean[] includedRowGroups) {
    BufferChunkList result = new BufferChunkList();
    if (hasSomeRowGroups(includedRowGroups)) {
      InStream.StreamOptions compression = dataReader.getCompressionOptions();
      boolean isCompressed = compression.getCodec() != null;
      int bufferSize = compression.getBufferSize();
      OrcProto.RowIndex[] rowIndex = index.getRowGroupIndex();
      for (StreamInformation stream : dataStreams) {
        if (RecordReaderUtils.isDictionary(stream.kind, encodings[stream.column])) {
          addChunk(result, stream, stream.offset, stream.length);
        } else {
          int column = stream.column;
          OrcProto.RowIndex ri = rowIndex[column];
          TypeDescription.Category kind = schema.findSubtype(column).getCategory();
          long alreadyRead = 0;
          for (int group = 0; group < includedRowGroups.length; ++group) {
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
                    bufferSize, false, nextGroupOffset, stream.length);
              }
              if (alreadyRead < end) {
                addChunk(result, stream, start, end - start);
                alreadyRead = end;
              }
              group = endGroup;
            }
          }
        }
      }
    }
    return result;
  }

  private static class StreamInformation {
    final OrcProto.Stream.Kind kind;
    final int column;
    final long offset;
    final long length;
    BufferChunk firstChunk;

    StreamInformation(OrcProto.Stream.Kind kind,
                      int column, long offset, long length) {
      this.kind = kind;
      this.column = column;
      this.offset = offset;
      this.length = length;
    }

    void releaseBuffers(DataReader reader) {
      long end = offset + length;
      BufferChunk ptr = firstChunk;
      while (ptr != null && ptr.getOffset() < end) {
        ByteBuffer buffer = ptr.getData();
        if (buffer != null) {
          reader.releaseBuffer(buffer);
          ptr.setChunk(null);
        }
        ptr = (BufferChunk) ptr.next;
      }
    }
  }
}
