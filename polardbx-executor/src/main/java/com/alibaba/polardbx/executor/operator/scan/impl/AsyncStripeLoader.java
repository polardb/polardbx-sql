package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.ORCMetricsWrapper;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileKeys;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.customized.ORCProfile;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.BufferChunkList;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.StreamInformation;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.alibaba.polardbx.executor.operator.scan.metrics.MetricsNameBuilder.columnMetricsKey;
import static com.alibaba.polardbx.executor.operator.scan.metrics.MetricsNameBuilder.columnsMetricsKey;
import static com.alibaba.polardbx.executor.operator.scan.metrics.MetricsNameBuilder.streamMetricsKey;

public class AsyncStripeLoader implements StripeLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    // Name of metrics
    public static final String ASYNC_STRIPE_LOADER_MEMORY = "AsyncStripeLoader.Memory";
    public static final String ASYNC_STRIPE_LOADER_TIMER = "AsyncStripeLoader.Timer";
    public static final String ASYNC_STRIPE_LOADER_BYTES_RANGE = "AsyncStripeLoader.BytesRange";

    // To sort the InStream with different stream name.
    private static final Comparator<StreamName> STREAM_NAME_COMPARATOR = (s1, s2) -> {
        if (s1.getColumn() != s2.getColumn()) {
            return s1.getColumn() - s2.getColumn();
        } else {
            return s1.getKind().name().compareTo(s2.getKind().name());
        }
    };

    // parameters for IO processing
    private final ExecutorService ioExecutor;
    private final FileSystem fileSystem;
    private final Configuration configuration;
    private final Path filePath;
    private final boolean[] columnIncluded;

    // for compression
    private final int compressionSize;
    private final CompressionKind compressionKind;

    // preheated meta of this stripe
    private final PreheatFileMeta preheatFileMeta;

    // context for stripe parser
    private final StripeInformation stripeInformation;
    private final TypeDescription fileSchema;
    private final OrcFile.WriterVersion version;

    private final ReaderEncryption encryption;
    private final OrcProto.ColumnEncoding[] encodings;
    private final boolean ignoreNonUtf8BloomFilter;
    private final long maxBufferSize;
    private final int maxDiskRangeChunkLimit;
    private final long maxMergeDistance;

    // need initialized
    private StripeContext stripeContext;
    private StreamManager streamManager;
    private InStream.StreamOptions streamOptions;

    // register loading or loaded columns.
    // NODE: The Stripe-Loader is stateful, and a column can only be loaded once in one stripe.
    private ConcurrentHashMap<Integer, boolean[]> registerMap;

    // for metrics
    private final RuntimeMetrics metrics;
    private final boolean enableMetrics;
    private boolean isOpened;
    private Counter openingTimer;

    // for memory management.
    private final MemoryAllocatorCtx memoryAllocatorCtx;
    private AtomicLong totalAllocatedBytes;
    private Set<StreamName> releasedStreams;

    public AsyncStripeLoader(
        // for file io execution
        ExecutorService ioExecutor, FileSystem fileSystem,
        Configuration configuration, Path filePath, boolean[] columnIncluded,

        // for compression
        int compressionSize, CompressionKind compressionKind,

        // preheated meta of this stripe
        PreheatFileMeta preheatFileMeta,

        // context for stripe parser
        StripeInformation stripeInformation,
        TypeDescription fileSchema, OrcFile.WriterVersion version,
        ReaderEncryption encryption,
        OrcProto.ColumnEncoding[] encodings,
        boolean ignoreNonUtf8BloomFilter, long maxBufferSize,
        int maxDiskRangeChunkLimit, long maxMergeDistance,

        // for metrics
        RuntimeMetrics metrics,
        boolean enableMetrics, MemoryAllocatorCtx memoryAllocatorCtx) {
        this.maxDiskRangeChunkLimit = maxDiskRangeChunkLimit;
        this.maxMergeDistance = maxMergeDistance;
        this.enableMetrics = enableMetrics;
        this.memoryAllocatorCtx = memoryAllocatorCtx;
        // NOTE: the 0th column in array is tree-struct.
        Preconditions.checkArgument(columnIncluded != null
            && columnIncluded.length == fileSchema.getMaximumId() + 1);

        this.ioExecutor = ioExecutor;
        this.fileSystem = fileSystem;
        this.configuration = configuration;
        this.filePath = filePath;
        this.columnIncluded = columnIncluded;
        this.compressionSize = compressionSize;
        this.compressionKind = compressionKind;
        this.preheatFileMeta = preheatFileMeta;
        this.stripeInformation = stripeInformation;
        this.fileSchema = fileSchema;
        this.version = version;
        this.encryption = encryption;
        this.encodings = encodings;
        this.ignoreNonUtf8BloomFilter = ignoreNonUtf8BloomFilter;
        this.maxBufferSize = maxBufferSize;

        this.metrics = metrics;

        // internal state
        this.registerMap = new ConcurrentHashMap<>();
        this.isOpened = false;

        if (enableMetrics) {
            this.openingTimer = metrics.addCounter(
                ProfileKeys.ORC_STRIPE_LOADER_OPEN_TIMER.getName(),
                ASYNC_STRIPE_LOADER_TIMER,
                ProfileUnit.NANO_SECOND
            );
        }

        this.totalAllocatedBytes = new AtomicLong(0);
        this.releasedStreams = new HashSet<>();
    }

    @Override
    public void open() {
        long start = System.nanoTime();
        streamOptions = InStream.options()
            .withCodec(OrcCodecPool.getCodec(compressionKind))
            .withBufferSize(compressionSize);

        stripeContext = new StripeContext(
            stripeInformation, fileSchema, encryption, version, streamOptions, ignoreNonUtf8BloomFilter, maxBufferSize
        );

        OrcProto.StripeFooter footer =
            preheatFileMeta.getStripeFooter(stripeInformation.getStripeId());

        // Get all stream information in this stripe.
        streamManager = StaticStripePlanner.parseStripe(
            stripeContext, columnIncluded, footer
        );
        releasedStreams.addAll(streamManager.getStreams().keySet());

        isOpened = true;
        if (enableMetrics) {
            openingTimer.inc(System.nanoTime() - start);
        }
    }

    @Override
    public CompletableFuture<Map<StreamName, InStream>> load(List<Integer> columnIds,
                                                             Map<Integer, boolean[]> rowGroupBitmaps,
                                                             Supplier<Boolean> controller) {
        Preconditions.checkArgument(isOpened, "The stripe loader has not already been opened");
        // Column-level parallel data loading is only suitable for columns that size > 2MB in one stripe.
        // In some cases, we need merge all columns in one IO task.

        OrcIndex orcIndex = preheatFileMeta.getOrcIndex(
            stripeInformation.getStripeId()
        );

        // build selected columns bitmap
        boolean[] selectedColumns = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(selectedColumns, false);
        columnIds.forEach(col -> selectedColumns[col] = true);

        // Build IO plans for each column with different row group bitmaps
        // and merge them into one buffer-chunk-list.

        // Get the IO plan of all streams in this column.
        BufferChunkList result = StaticStripePlanner.planGroupsInColumn(
            stripeContext,
            streamManager,
            streamOptions,
            orcIndex,
            rowGroupBitmaps,
            selectedColumns
        );

        // check buffer chunk list
        long bytesInIOPlan = 0L;
        long bytesHitStream = 0L;
        for (BufferChunk node = result.get(); node != null; node = (BufferChunk) node.next) {
            bytesInIOPlan += node.getLength();
        }

        // metrics the logical bytes range.
        Counter bytesRangeCounter = enableMetrics ? metrics.addCounter(
            columnsMetricsKey(selectedColumns, ProfileKeys.ORC_LOGICAL_BYTES_RANGE),
            ASYNC_STRIPE_LOADER_BYTES_RANGE,
            ProfileKeys.ORC_LOGICAL_BYTES_RANGE.getProfileUnit()
        ) : null;
        for (Map.Entry<StreamName, StreamInformation> entry : streamManager.getStreams().entrySet()) {
            StreamName streamName = entry.getKey();
            if (streamName.getColumn() < selectedColumns.length && selectedColumns[streamName.getColumn()]) {
                StreamInformation stream = entry.getValue();

                // filter stream with no data.
                if (stream != null && stream.firstChunk != null) {
                    for (DiskRangeList node = stream.firstChunk; node != null; node = node.next) {
                        if (node.getOffset() >= stream.offset
                            && node.getEnd() <= stream.offset + stream.length) {
                            if (enableMetrics) {
                                bytesRangeCounter.inc(node.getLength());
                            }
                            bytesHitStream += node.getLength();
                        }
                    }
                }
            }
        }

        Preconditions.checkArgument(bytesHitStream == bytesInIOPlan,
            String.format(
                "bytesHitStream = %s but bytesInIOPlan = %s",
                bytesHitStream, bytesInIOPlan
            ));

        // large memory allocation: buffer chunk list in stripe-level IO processing.
        // We must multiply by a factor of 2 because the OSS network buffer
        // or file read buffer requires the same memory size as bytesInIOPlan.
        final long allocatedBytes = 2 * bytesInIOPlan;
        memoryAllocatorCtx.allocateReservedMemory(allocatedBytes);
        totalAllocatedBytes.addAndGet(allocatedBytes);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(MessageFormat.format("filePath = {0}, stripeId = {1}, allocatedBytes = {2}",
                filePath, stripeInformation.getStripeId(), allocatedBytes));
        }

        return CompletableFuture.supplyAsync(
            () -> readData(selectedColumns, result, controller), ioExecutor
        );
    }

    @Override
    public CompletableFuture<Map<StreamName, InStream>> load(int targetColumnId,
                                                             boolean[] targetRowGroups,
                                                             Supplier<Boolean> controller) {
        Preconditions.checkArgument(isOpened, "The stripe loader has not already been opened");
        // Column-level parallel data loading is only suitable for columns that size > 2MB in one stripe.
        // In some cases, we need merge all columns in one IO task.

        if (registerMap.putIfAbsent(targetColumnId, targetRowGroups) != null) {
            // The Stripe-Loader is stateful, and a column can only be loaded once in one stripe.
            throw new RuntimeException(
                MessageFormat.format("The column id {0} in stripe can only be planned once", targetColumnId)
            );
        }

        OrcIndex orcIndex = preheatFileMeta.getOrcIndex(
            stripeInformation.getStripeId()
        );

        // Get the IO plan of all streams in this column.
        BufferChunkList ioPlan = StaticStripePlanner.planGroupsInColumn(
            stripeContext,
            streamManager,
            streamOptions,
            orcIndex,
            targetRowGroups,
            targetColumnId
        );

        // check buffer chunk list
        long bytesInIOPlan = 0L;
        long bytesHitStream = 0L;
        for (BufferChunk node = ioPlan.get(); node != null; node = (BufferChunk) node.next) {
            bytesInIOPlan += node.getLength();
        }

        // metrics the logical bytes range.
        Counter bytesRangeCounter = enableMetrics ? metrics.addCounter(
            columnMetricsKey(targetColumnId, ProfileKeys.ORC_LOGICAL_BYTES_RANGE),
            ASYNC_STRIPE_LOADER_BYTES_RANGE,
            ProfileKeys.ORC_LOGICAL_BYTES_RANGE.getProfileUnit()
        ) : null;
        for (Map.Entry<StreamName, StreamInformation> entry : streamManager.getStreams().entrySet()) {
            StreamName streamName = entry.getKey();
            if (streamName.getColumn() == targetColumnId) {
                StreamInformation stream = entry.getValue();

                // filter stream with no data.
                if (stream != null && stream.firstChunk != null) {
                    for (DiskRangeList node = stream.firstChunk; node != null; node = node.next) {
                        if (node.getOffset() >= stream.offset
                            && node.getEnd() <= stream.offset + stream.length) {
                            if (enableMetrics) {
                                bytesRangeCounter.inc(node.getLength());
                            }
                            bytesHitStream += node.getLength();
                        }
                    }
                }
            }
        }

        Preconditions.checkArgument(bytesHitStream == bytesInIOPlan,
            String.format(
                "bytesHitStream = %s but bytesInIOPlan = %s",
                bytesHitStream, bytesInIOPlan
            ));

        // large memory allocation: buffer chunk list in stripe-level IO processing.
        // We must multiply by a factor of 2 because the OSS network buffer
        // or file read buffer requires the same memory size as bytesInIOPlan.
        final long allocatedBytes = 2 * bytesInIOPlan;
        memoryAllocatorCtx.allocateReservedMemory(allocatedBytes);
        totalAllocatedBytes.addAndGet(allocatedBytes);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(MessageFormat.format("filePath = {0}, stripeId = {1}, allocatedBytes = {2}",
                filePath, stripeInformation.getStripeId(), allocatedBytes));
        }

        return CompletableFuture.supplyAsync(
            () -> readData(targetColumnId, ioPlan, controller), ioExecutor
        );
    }

    @Override
    public long clearStream(StreamName streamName) {
        if (streamManager == null) {
            return 0L;
        }
        // find stream information and clear the buffer chunk list.
        Map<StreamName, StreamInformation> allStreams = streamManager.getStreams();
        StreamInformation streamInformation;
        if ((streamInformation = allStreams.get(streamName)) != null) {
            long releasedBytes = streamInformation.releaseBuffers();

            // allocate the memory of data IO.
            memoryAllocatorCtx.releaseReservedMemory(2 * releasedBytes, true);
            totalAllocatedBytes.getAndAdd(-2 * releasedBytes);

            releasedStreams.remove(streamName);
            if (releasedStreams.isEmpty()) {
                // all streams have been released.
                memoryAllocatorCtx.releaseReservedMemory(totalAllocatedBytes.get(), true);
                totalAllocatedBytes.getAndAdd(-totalAllocatedBytes.get());
            }

            return releasedBytes;
        }
        return 0L;
    }

    /**
     * For validation:
     * The list of {range, data} must be in range of stream [offset, length].
     * And the result RangDiskList must be constructed by linked list of all streams.
     *
     * @return stream manager holding the stream information.
     */
    public StreamManager getStreamManager() {
        return streamManager;
    }

    private Map<StreamName, InStream> readData(boolean[] selectedColumns, BufferChunkList ioPlan,
                                               Supplier<Boolean> controller) {
        try (DataReader dataReader = buildDataReader()) {
            dataReader.setController(controller);
            if (enableMetrics) {
                // build profile for IO processing.
                ORCProfile memoryCounter = new ORCMetricsWrapper(
                    columnsMetricsKey(selectedColumns, ProfileKeys.ORC_IO_RAW_DATA_MEMORY_COUNTER),
                    ASYNC_STRIPE_LOADER_MEMORY,
                    ProfileKeys.ORC_IO_RAW_DATA_MEMORY_COUNTER.getProfileUnit(),
                    metrics);

                ORCProfile ioTimer = new ORCMetricsWrapper(
                    columnsMetricsKey(selectedColumns, ProfileKeys.ORC_IO_RAW_DATA_TIMER),
                    ASYNC_STRIPE_LOADER_TIMER,
                    ProfileKeys.ORC_IO_RAW_DATA_TIMER.getProfileUnit(),
                    metrics);

                // Execute IO tasks within the buffer chunk list.
                dataReader.readFileData(ioPlan, false, memoryCounter, null, ioTimer);
            } else {
                dataReader.readFileData(ioPlan, false);
            }
        } catch (Throwable t) {
            // IO ERROR
            throw GeneralUtil.nestedException(t);
        }

        // Build in-streams after IO tasks done
        return buildInStreams((col) -> col < selectedColumns.length && selectedColumns[col]);
    }

    private Map<StreamName, InStream> readData(int targetColumnId, BufferChunkList ioPlan,
                                               Supplier<Boolean> controller) {
        try (DataReader dataReader = buildDataReader()) {
            dataReader.setController(controller);
            if (enableMetrics) {
                // build profile for IO processing.
                ORCProfile memoryCounter = new ORCMetricsWrapper(
                    columnMetricsKey(targetColumnId, ProfileKeys.ORC_IO_RAW_DATA_MEMORY_COUNTER),
                    ASYNC_STRIPE_LOADER_MEMORY,
                    ProfileKeys.ORC_IO_RAW_DATA_MEMORY_COUNTER.getProfileUnit(),
                    metrics);

                ORCProfile ioTimer = new ORCMetricsWrapper(
                    columnMetricsKey(targetColumnId, ProfileKeys.ORC_IO_RAW_DATA_TIMER),
                    ASYNC_STRIPE_LOADER_TIMER,
                    ProfileKeys.ORC_IO_RAW_DATA_TIMER.getProfileUnit(),
                    metrics);

                // Execute IO tasks within the buffer chunk list.
                dataReader.readFileData(ioPlan, false, memoryCounter, null, ioTimer);
            } else {
                dataReader.readFileData(ioPlan, false);
            }
        } catch (Throwable t) {
            // IO ERROR
            throw GeneralUtil.nestedException(t);
        }

        // Build in-streams after IO tasks done
        Map<StreamName, InStream> results = buildInStreams((col) -> col == targetColumnId);

        return results;
    }

    @NotNull
    private Map<StreamName, InStream> buildInStreams(Predicate<Integer> columnFilter) {
        Map<StreamName, InStream> results = new TreeMap<>(STREAM_NAME_COMPARATOR);
        for (Map.Entry<StreamName, StreamInformation> entry : streamManager.getStreams().entrySet()) {
            StreamName streamName = entry.getKey();
            if (columnFilter.test(streamName.getColumn())) {
                StreamInformation stream = entry.getValue();

                // filter stream with no data.
                if (stream != null && stream.firstChunk != null) {

                    InStream inStream = InStream.create(
                        streamName,
                        stream.firstChunk,
                        stream.offset,
                        stream.length,
                        streamOptions);

                    // build profile for in stream.
                    ORCProfile memoryCounter = enableMetrics ? new ORCMetricsWrapper(
                        streamMetricsKey(streamName, ProfileKeys.ORC_IN_STREAM_MEMORY_COUNTER),
                        ASYNC_STRIPE_LOADER_MEMORY,
                        ProfileKeys.ORC_IN_STREAM_MEMORY_COUNTER.getProfileUnit(),
                        metrics) : null;

                    ORCProfile decompressTimer = enableMetrics ? new ORCMetricsWrapper(
                        streamMetricsKey(streamName, ProfileKeys.ORC_IN_STREAM_DECOMPRESS_TIMER),
                        ASYNC_STRIPE_LOADER_TIMER,
                        ProfileKeys.ORC_IN_STREAM_DECOMPRESS_TIMER.getProfileUnit(),
                        metrics) : null;

                    inStream.setMemoryCounter(memoryCounter);
                    inStream.setDecompressTimer(decompressTimer);

                    results.put(streamName, inStream);
                }
            }
        }
        return results;
    }

    private DataReader buildDataReader() throws IOException {
        // The stream options will be modified when data-reader closing.
        InStream.StreamOptions streamOptions = InStream.options()
            .withCodec(OrcCodecPool.getCodec(compressionKind))
            .withBufferSize(compressionSize);

        DataReaderProperties.Builder builder =
            DataReaderProperties.builder()
                .withCompression(streamOptions)
                .withFileSystemSupplier(() -> fileSystem)
                .withPath(filePath)
                .withMaxMergeDistance(maxMergeDistance)
                .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
                .withZeroCopy(false);
        FSDataInputStream file = fileSystem.open(filePath);
        if (file != null) {
            builder.withFile(file);
        }

        DataReader dataReader = RecordReaderUtils.createDefaultDataReader(builder.build());
        return dataReader;
    }

    @Override
    public void close() throws IOException {
        // nothing should be closed here.
    }

}
