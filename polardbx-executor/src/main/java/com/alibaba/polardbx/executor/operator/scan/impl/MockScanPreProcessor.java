package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.columnar.pruning.ColumnarPruneManager;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.executor.operator.scan.ORCMetaReader;
import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.OrcTail;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils.transformRexToIndexMergeTree;

/**
 * A mocked implementation of ScanPreProcessor that can generate
 * file preheat meta, deletion bitmap and pruning result (all selected).
 */
public class MockScanPreProcessor implements ScanPreProcessor {

    private static final Cache<Path, PreheatFileMeta> PREHEATED_CACHE =
        CacheBuilder.newBuilder().maximumSize(1 << 12).build();

    /**
     * File path participated in preprocessor.
     */
    private final Set<Path> filePaths;

    /**
     * A shared configuration object to avoid initialization of large parameter list.
     */
    private final Configuration configuration;

    /**
     * The filesystem storing the files in file path list.
     */
    private final FileSystem fileSystem;

    /**
     * The ratio that row-groups will be selected in a stripe.
     */
    private final double groupsRatio;

    /**
     * The ratio that row positions in file will be marked.
     */
    private final double deletionRatio;

    /**
     * Mapping from file path to its preheated file meta.
     */
    private final Map<Path, PreheatFileMeta> preheatFileMetaMap;

    /**
     * The future will be null if preparation has not been invoked.
     */
    private ListenableFuture<?> future;

    /**
     * The mocked pruning results that mapping from file path to stride + group info.
     */
    private Map<Path, SortedMap<Integer, boolean[]>> rowGroupMatrix;

    /**
     * Mapping from file path to deletion bitmap.
     */
    private Map<Path, RoaringBitmap> deletions;

    private List<RexNode> rexList;

    /**
     * Store the throwable info generated during preparation.
     */
    private Throwable throwable;

    private IndexPruneContext ipc;

    private final boolean enableOssCompatible;

    public MockScanPreProcessor(Configuration configuration,
                                FileSystem fileSystem,
                                List<RexNode> rexList,
                                Map<Integer, ParameterContext> params,
                                double groupsRatio,
                                double deletionRatio,
                                boolean enableOssCompatible) {
        this.filePaths = new TreeSet<>();

        this.configuration = configuration;
        this.fileSystem = fileSystem;
        this.deletionRatio = deletionRatio;
        this.groupsRatio = groupsRatio;

        this.preheatFileMetaMap = new HashMap<>();
        this.rowGroupMatrix = new HashMap<>();
        this.deletions = new HashMap<>();
        this.rexList = rexList;
        this.ipc = new IndexPruneContext();
        ipc.setParameters(new Parameters(params));

        this.enableOssCompatible = enableOssCompatible;
    }

    @Override
    public void addFile(Path filePath) {
        this.filePaths.add(filePath);
    }

    @Override
    public ListenableFuture<?> prepare(ExecutorService executor, String traceId, ColumnarTracer tracer) {
        SettableFuture<?> future = SettableFuture.create();
        ipc.setPruneTracer(tracer);
        // Is there a more elegant execution mode?
        executor.submit(
            () -> {
                for (Path filePath : filePaths) {
                    try {
                        // preheat all meta from orc file.
                        PreheatFileMeta preheat =
                            PREHEATED_CACHE.get(filePath, () -> preheat(filePath));

                        preheatFileMetaMap.put(filePath, preheat);

                        // rex+pc -> distribution segment condition + indexes merge tree
                        ColumnPredicatePruningInf columnPredicate = transformRexToIndexMergeTree(rexList, ipc);

                        if (columnPredicate == null) {
                            generateFullMatrix(filePath);
                        } else {
                            IndexPruner indexPruner = ColumnarPruneManager.getIndexPruner(filePath, preheat,
                                Collections.emptyList(), 1,
                                IntStream.range(0, preheat.getPreheatTail().getTypes().size()).boxed().collect(
                                    Collectors.toList()), enableOssCompatible);
                            // prune stripe&row groups
                            rowGroupMatrix.put(filePath,
                                indexPruner.pruneToSortMap("", Lists.newArrayList(), columnPredicate, ipc));
                        }

                        // generate deletion according to deletion ratio and row count.
                        generateDeletion(filePath);
                    } catch (Exception e) {
                        throwable = e;
                        future.set(null);
                        return;
                    }
                }
                future.set(null);
            }
        );

        this.future = future;
        return future;
    }

    @Override
    public boolean isPrepared() {
        throwIfFailed();
        return throwable == null && future != null && future.isDone();
    }

    @Override
    public SortedMap<Integer, boolean[]> getPruningResult(Path filePath) {
        throwIfFailed();
        Preconditions.checkArgument(isPrepared());
        return rowGroupMatrix.get(filePath);
    }

    @Override
    public PreheatFileMeta getPreheated(Path filePath) {
        throwIfFailed();
        Preconditions.checkArgument(isPrepared());
        return preheatFileMetaMap.get(filePath);
    }

    @Override
    public RoaringBitmap getDeletion(Path filePath) {
        throwIfFailed();
        Preconditions.checkArgument(isPrepared());
        return deletions.get(filePath);
    }

    @Override
    public void throwIfFailed() {
        if (throwable != null) {
            throw GeneralUtil.nestedException(throwable);
        }
    }

    private void generateMatrix(Path filePath) {
        PreheatFileMeta preheatFileMeta = preheatFileMetaMap.get(filePath);
        OrcTail orcTail = preheatFileMeta.getPreheatTail();

        int indexStride = orcTail.getFooter().getRowIndexStride();

        // but sorted by stripe id.
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        for (StripeInformation stripeInformation : orcTail.getStripes()) {
            int stripeId = (int) stripeInformation.getStripeId();
            int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);

            // build row-group by stripe row count and index stride
            // and mark the first (groupsInStripe * groupsRatio) positions as selected
            boolean[] groupIncluded = new boolean[groupsInStripe];
            int length = Math.min(groupsInStripe, (int) (groupsInStripe * groupsRatio));
            Arrays.fill(groupIncluded, 0, length, true);

            matrix.put(stripeId, groupIncluded);
        }
        rowGroupMatrix.put(filePath, matrix);
    }

    private void generateDeletion(Path filePath) {
        PreheatFileMeta preheatFileMeta = preheatFileMetaMap.get(filePath);
        OrcTail orcTail = preheatFileMeta.getPreheatTail();

        final int rowCount = (int) orcTail.getFileTail().getFooter().getNumberOfRows();
        RoaringBitmap bitmap = new RoaringBitmap();

        // only if stride in proper range or deletion ratio >= 0 we generate a non-empty bitmap.
        int stride;
        if (deletionRatio != 0d && (stride = (int) Math.ceil(1d / deletionRatio)) < rowCount) {
            for (int i = 0; i < rowCount; i += stride) {
                bitmap.add(i);
            }
        }

        deletions.put(filePath, bitmap);
    }

    private PreheatFileMeta preheat(Path filePath) throws IOException {
        ORCMetaReader metaReader = null;
        try {
            metaReader = ORCMetaReader.create(configuration, fileSystem);
            PreheatFileMeta preheatFileMeta = metaReader.preheat(filePath);

            return preheatFileMeta;
        } finally {
            metaReader.close();
        }
    }

    private void generateFullMatrix(Path filePath) {
        PreheatFileMeta preheatFileMeta = preheatFileMetaMap.get(filePath);
        OrcTail orcTail = preheatFileMeta.getPreheatTail();

        int indexStride = orcTail.getFooter().getRowIndexStride();

        // but sorted by stripe id.
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        for (StripeInformation stripeInformation : orcTail.getStripes()) {
            int stripeId = (int) stripeInformation.getStripeId();
            int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);

            // build row-group by stripe row count and index stride
            // mark all groups as selected.
            boolean[] groupIncluded = new boolean[groupsInStripe];
            Arrays.fill(groupIncluded, true);

            matrix.put(stripeId, groupIncluded);
        }
        rowGroupMatrix.put(filePath, matrix);
    }
}
