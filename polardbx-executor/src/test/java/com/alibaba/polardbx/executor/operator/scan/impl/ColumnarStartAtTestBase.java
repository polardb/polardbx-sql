package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.ORCMetaReader;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.reader.ReaderEncryption;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ColumnarStartAtTestBase {
    protected static final int DEFAULT_INDEX_STRIDE = 10000;

    // variables
    protected String orcFileName;
    protected int columnId;
    protected int stripeId;

    protected Configuration configuration;
    protected Path filePath;
    protected FileSystem fileSystem;
    protected ExecutorService ioExecutor;

    protected CompressionKind compressionKind;
    protected int compressionSize;

    // stripe-level and file-level meta
    protected TreeMap<Integer, StripeInformation> stripeInformationMap;
    protected TypeDescription fileSchema;
    protected OrcFile.WriterVersion version;
    protected ReaderEncryption encryption;
    protected boolean ignoreNonUtf8BloomFilter;
    protected int maxBufferSize;
    protected int indexStride;
    protected int maxDiskRangeChunkLimit;
    protected long maxMergeDistance;

    // need preheating
    protected PreheatFileMeta preheatFileMeta;
    protected MemoryAllocatorCtx memoryAllocatorCtx;
    protected OrcIndex orcIndex;
    protected AsyncStripeLoader stripeLoader;
    protected boolean[] rowGroupIncluded;
    protected boolean[] columnIncluded;
    protected OrcProto.ColumnEncoding[] encodings;

    public ColumnarStartAtTestBase(String orcFileName, int columnId, int stripeId) throws IOException {
        this.orcFileName = orcFileName;
        this.columnId = columnId;
        this.stripeId = stripeId;

        ioExecutor = Executors.newFixedThreadPool(4);

        configuration = new Configuration();
        OrcConf.ROW_INDEX_STRIDE.setInt(configuration, DEFAULT_INDEX_STRIDE);
        OrcConf.MAX_MERGE_DISTANCE.setLong(configuration, 64L * 1024); // 64KB
        OrcConf.USE_ZEROCOPY.setBoolean(configuration, true);
        OrcConf.STRIPE_SIZE.setLong(configuration, 64 * 1024 * 1024); // 64MB

        Path workDir = new Path(this.getClass().getClassLoader().getResource(".").toString());

        filePath = new Path(workDir, orcFileName);

        fileSystem = FileSystem.get(
            filePath.toUri(), configuration
        );
    }

    protected void prepareReading() throws IOException {
        // preheat
        ORCMetaReader metaReader = null;
        try {
            metaReader = ORCMetaReader.create(configuration, fileSystem);
            preheatFileMeta = metaReader.preheat(filePath);
        } finally {
            metaReader.close();
        }

        OrcTail orcTail = preheatFileMeta.getPreheatTail();

        // compression info
        compressionKind = orcTail.getCompressionKind();
        compressionSize = orcTail.getCompressionBufferSize();

        // get the mapping from stripe id to stripe information.
        List<StripeInformation> stripeInformationList = orcTail.getStripes();
        stripeInformationMap = stripeInformationList.stream().collect(Collectors.toMap(
            stripe -> (int) stripe.getStripeId(),
            stripe -> stripe,
            (s1, s2) -> s1,
            () -> new TreeMap<>()
        ));

        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        orcIndex = preheatFileMeta.getOrcIndex(stripeInformation.getStripeId());

        fileSchema = orcTail.getSchema();
        version = orcTail.getWriterVersion();

        this.columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, true);

        // encryption info for reading.
        OrcProto.Footer footer = orcTail.getFooter();
        encryption = new ReaderEncryption(footer, fileSchema,
            orcTail.getStripeStatisticsOffset(), orcTail.getTailBuffer(), stripeInformationList,
            null, configuration);

        // should the reader ignore the obsolete non-UTF8 bloom filters.
        ignoreNonUtf8BloomFilter = OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(configuration);

        // max buffer size in single IO task.
        maxBufferSize = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(configuration);

        // the max row count in one row group.
        indexStride = OrcConf.ROW_INDEX_STRIDE.getInt(configuration);

        maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(configuration);
        maxMergeDistance = OrcConf.MAX_MERGE_DISTANCE.getLong(configuration);

        encodings = StaticStripePlanner.buildEncodings(
            encryption, columnIncluded, preheatFileMeta.getStripeFooter(stripeId)
        );

        ExecutionContext context = new ExecutionContext();
        context.setTraceId("mock_trace_id");
        if (context.getMemoryPool() == null) {
            context.setMemoryPool(MemoryManager.getInstance().createQueryMemoryPool(
                WorkloadUtil.isApWorkload(
                    context.getWorkloadType()), context.getTraceId(), context.getExtraCmds()));
        }
        MemoryPool memoryPool = MemoryPoolUtils
            .createOperatorTmpTablePool("ColumnarScanExec@" + System.identityHashCode(this),
                context.getMemoryPool());
        this.memoryAllocatorCtx = memoryPool.getMemoryAllocatorCtx();

        stripeLoader = new AsyncStripeLoader(
            ioExecutor,
            fileSystem,
            configuration,
            filePath,
            columnIncluded,
            compressionSize,
            compressionKind,
            preheatFileMeta,
            stripeInformationMap.get(stripeId),
            fileSchema,
            version,
            encryption,
            encodings, ignoreNonUtf8BloomFilter,
            maxBufferSize,
            maxDiskRangeChunkLimit, maxMergeDistance, null,
            false, memoryAllocatorCtx);

        stripeLoader.open();

        final int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);
        this.rowGroupIncluded = new boolean[groupsInStripe];
        Arrays.fill(rowGroupIncluded, true);

    }

    protected static String getFileFromClasspath(String name) {
        URL url = ClassLoader.getSystemResource(name);
        if (url == null) {
            throw new IllegalArgumentException("Could not find " + name);
        }
        return url.getPath();
    }
}
