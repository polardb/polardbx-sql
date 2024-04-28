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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.columnar.pruning.ColumnarPruneManager;
import com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.operator.scan.ORCMetaReader;
import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
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
import static com.alibaba.polardbx.gms.engine.FileStoreStatistics.CACHE_STATS_FIELD_COUNT;

/**
 * A mocked implementation of ScanPreProcessor that can generate
 * file preheat meta, deletion bitmap and pruning result (all selected).
 */
public class DefaultScanPreProcessor implements ScanPreProcessor {

    private static final String PREHEATED_CACHE_NAME = "PREHEATED_CACHE";

    private static final int PREHEATED_CACHE_MAX_ENTRY = 4096;

    private static final Logger logger = LoggerFactory.getLogger(DefaultScanPreProcessor.class);

    protected static final Cache<Path, PreheatFileMeta> PREHEATED_CACHE =
        CacheBuilder.newBuilder().maximumSize(PREHEATED_CACHE_MAX_ENTRY).recordStats().build();

    /**
     * File path participated in preprocessor.
     */
    protected final Set<Path> filePaths;

    /**
     * A shared configuration object to avoid initialization of large parameter list.
     */
    private final Configuration configuration;

    /**
     * The filesystem storing the files in file path list.
     */
    private final FileSystem fileSystem;

    /**
     * To enable index pruning.
     */
    protected final boolean enableIndexPruning;

    /**
     * To enable oss compatible.
     */
    protected final boolean enableOssCompatible;

    protected final String schemaName;
    protected final String logicalTableName;

    /**
     * The column meta list from query plan.
     */
    protected final List<ColumnMeta> columns;

    /**
     * The pushed-down predicate.
     */
    protected final List<RexNode> rexList;

    /**
     * The ratio that row-groups will be selected in a stripe.
     */
    private final double groupsRatio;

    /**
     * The ratio that row positions in file will be marked.
     */
    private final double deletionRatio;

    /**
     * The checkpoint tso in columnar mode.
     * It will be null if in archive mode.
     */
    protected final Long tso;

    /**
     * The columnar manager will be null if in archive mode.
     */
    protected final ColumnarManager columnarManager;

    /**
     * Field id of each column for columnar index
     */
    private final List<Long> columnFieldIdList;

    /**
     * Mapping from file path to its preheated file meta.
     */
    protected final Map<Path, PreheatFileMeta> preheatFileMetaMap;

    /**
     * The future will be null if preparation has not been invoked.
     */
    protected ListenableFuture<?> future;

    /**
     * The mocked pruning results that mapping from file path to stride + group info.
     */
    protected Map<Path, SortedMap<Integer, boolean[]>> rowGroupMatrix;

    /**
     * Mapping from file path to deletion bitmap.
     */
    private Map<Path, RoaringBitmap> deletions;

    /**
     * Store the throwable info generated during preparation.
     */
    protected Throwable throwable;

    protected IndexPruneContext indexPruneContext;

    public DefaultScanPreProcessor(Configuration configuration,
                                   FileSystem fileSystem,

                                   // for pruning
                                   String schemaName,
                                   String logicalTableName,
                                   boolean enableIndexPruning,
                                   boolean enableOssCompatible,
                                   List<ColumnMeta> columns,
                                   List<RexNode> rexList,
                                   Map<Integer, ParameterContext> params,

                                   // for mock
                                   double groupsRatio,
                                   double deletionRatio,

                                   // for columnar mode.
                                   ColumnarManager columnarManager,
                                   Long tso,
                                   List<Long> columnFieldIdList) {

        this.filePaths = new TreeSet<>();
        this.configuration = configuration;
        this.fileSystem = fileSystem;

        // for pruning.
        this.enableIndexPruning = enableIndexPruning;
        this.enableOssCompatible = enableOssCompatible;
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        this.columns = columns;
        this.rexList = rexList;
        this.indexPruneContext = new IndexPruneContext();
        indexPruneContext.setParameters(new Parameters(params));

        // for mock
        this.deletionRatio = deletionRatio;
        this.groupsRatio = groupsRatio;

        // for columnar mode
        this.columnarManager = columnarManager;
        // The checkpoint tso in columnar mode. It will be null if in archive mode.
        this.tso = tso;
        this.columnFieldIdList = columnFieldIdList;

        this.preheatFileMetaMap = new HashMap<>();
        this.rowGroupMatrix = new HashMap<>();
        this.deletions = new HashMap<>();
    }

    @Override
    public void addFile(Path filePath) {
        this.filePaths.add(filePath);
    }

    @Override
    public ListenableFuture<?> prepare(ExecutorService executor, String traceId, ColumnarTracer tracer) {
        SettableFuture<?> future = SettableFuture.create();
        indexPruneContext.setPruneTracer(tracer);
        // Is there a more elegant execution mode?
        executor.submit(
            () -> {
                int stripeNum = 0;
                int rgNum = 0;
                int pruneRgLeft = 0;
                try {
                    // rex+pc -> distribution segment condition + indexes merge tree
                    ColumnPredicatePruningInf columnPredicate =
                        transformRexToIndexMergeTree(rexList, indexPruneContext);
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "column index prune " + schemaName + "," + logicalTableName + "," + PruneUtils.display(
                                columnPredicate, columns, indexPruneContext));
                    }

                    for (Path filePath : filePaths) {
                        boolean needGenerateDeletion = true;

                        // only preheat orc file meta
                        if (filePath.getName().toUpperCase().endsWith(ColumnarFileType.ORC.name())) {
                            // preheat all meta from orc file.
                            PreheatFileMeta preheat =
                                PREHEATED_CACHE.get(filePath, () -> preheat(filePath));

                            preheatFileMetaMap.put(filePath, preheat);

                            if (enableIndexPruning && columnPredicate != null && tso != null
                                && columnFieldIdList != null) {
                                // prune the row-groups.
                                long loadIndexStart = System.nanoTime();
                                // TODO support multi columns for sort key
                                List<Integer> sortKeys =
                                    columnarManager.getSortKeyColumns(tso, schemaName, logicalTableName);
                                Map<Long, Integer> orcIndexesMap =
                                    columnarManager.getPhysicalColumnIndexes(filePath.getName());
                                IndexPruner indexPruner =
                                    ColumnarPruneManager.getIndexPruner(
                                        filePath, preheat, columns, sortKeys.get(0),
                                        columnFieldIdList.stream()
                                            .map(field -> orcIndexesMap.get(field) + 1)
                                            .collect(Collectors.toList()),
                                        enableOssCompatible
                                    );

                                if (tracer != null) {
                                    tracer.tracePruneInit(logicalTableName,
                                        PruneUtils.display(columnPredicate, columns, indexPruneContext),
                                        System.nanoTime() - loadIndexStart);
                                }
                                long indexPruneStart = System.nanoTime();
                                RoaringBitmap rr =
                                    indexPruner.prune(logicalTableName, columns, columnPredicate, indexPruneContext);
                                SortedMap<Integer, boolean[]> rs = indexPruner.pruneToSortMap(rr);
                                if (rr.isEmpty()) {
                                    needGenerateDeletion = false;
                                }
                                if (tracer != null) {
                                    tracer.tracePruneTime(logicalTableName,
                                        PruneUtils.display(columnPredicate, columns, indexPruneContext),
                                        System.nanoTime() - indexPruneStart);
                                }
                                stripeNum += indexPruner.getStripeRgNum().size();
                                rgNum += indexPruner.getRgNum();
                                pruneRgLeft += rr.getCardinality();
                                // prune stripe&row groups
                                rowGroupMatrix.put(filePath, rs);
                            } else {
                                // if pruning is disabled, mark all row-groups as selected.
                                generateFullMatrix(filePath);
                            }
                        }
                        // generate deletion according to deletion ratio and row count.
                        if (needGenerateDeletion) {
                            generateDeletion(filePath);
                        }
                    }
                    if (tracer != null && columnPredicate != null) {
                        tracer.tracePruneResult(logicalTableName,
                            PruneUtils.display(columnPredicate, columns, indexPruneContext),
                            filePaths.size(),
                            stripeNum, rgNum, pruneRgLeft);
                    }
                    if (logger.isDebugEnabled() && columnPredicate != null) {
                        logger.debug(
                            "prune result: " + traceId + "," + logicalTableName + "," + filePaths.size() + ","
                                + stripeNum + "," + rgNum
                                + "," + pruneRgLeft);
                    }

                } catch (Exception e) {
                    throwable = e;
                    future.set(null);
                    return;
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

    protected RoaringBitmap generateDeletion(Path filePath) {
        RoaringBitmap bitmap;
        if (tso == null) {
            // in archive mode.
            bitmap = new RoaringBitmap();
        } else {
            // in columnar mode.
            bitmap = columnarManager.getDeleteBitMapOf(tso, filePath.getName());
        }
        deletions.put(filePath, bitmap);
        return bitmap;
    }

    protected PreheatFileMeta preheat(Path filePath) throws IOException {
        ORCMetaReader metaReader = null;
        try {
            metaReader = ORCMetaReader.create(configuration, fileSystem);
            PreheatFileMeta preheatFileMeta = metaReader.preheat(filePath);

            return preheatFileMeta;
        } finally {
            metaReader.close();
        }
    }

    protected void generateFullMatrix(Path filePath) {
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

    public static byte[][] getCacheStat() {
        CacheStats cacheStats = PREHEATED_CACHE.stats();

        byte[][] results = new byte[CACHE_STATS_FIELD_COUNT][];
        int pos = 0;
        results[pos++] = PREHEATED_CACHE_NAME.getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(PREHEATED_CACHE.size()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(cacheStats.hitCount()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(cacheStats.missCount()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = "IN MEMORY".getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(PREHEATED_CACHE_MAX_ENTRY).getBytes();
        results[pos++] = new StringBuilder().append(-1).append(" BYTES").toString().getBytes();
        return results;
    }

}
