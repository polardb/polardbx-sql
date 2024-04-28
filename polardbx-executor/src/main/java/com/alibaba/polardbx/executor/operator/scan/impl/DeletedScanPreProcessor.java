package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.pruning.ColumnarPruneManager;
import com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils.transformRexToIndexMergeTree;

public class DeletedScanPreProcessor extends DefaultScanPreProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DeletedScanPreProcessor.class);

    public DeletedScanPreProcessor(Configuration configuration,
                                   FileSystem fileSystem,
                                   String schemaName,
                                   String logicalTableName,
                                   boolean enableIndexPruning,
                                   boolean enableOssCompatible,
                                   List<ColumnMeta> columns,
                                   List<RexNode> rexList,
                                   Map<Integer, ParameterContext> params,
                                   double groupsRatio,
                                   double deletionRatio,
                                   ColumnarManager columnarManager,
                                   Long tso,
                                   List<Long> columnFieldIdList) {
        super(
            configuration,
            fileSystem,

            schemaName,
            logicalTableName,
            enableIndexPruning,
            enableOssCompatible,
            columns,
            rexList,
            params,

            groupsRatio,
            deletionRatio,
            columnarManager,
            tso,
            columnFieldIdList
        );
    }

    @Override
    public ListenableFuture<?> prepare(ExecutorService executor, String traceId, ColumnarTracer tracer) {
        SettableFuture<?> future = SettableFuture.create();
        indexPruneContext.setPruneTracer(tracer);
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
                            "[" + this.getClass().getSimpleName() + "]" + "column index prune " + schemaName + ","
                                + logicalTableName + "," + PruneUtils.display(columnPredicate, columns,
                                indexPruneContext));
                    }

                    for (Path filePath : filePaths) {
                        RoaringBitmap deleteBitmap = generateDeletion(filePath);

                        // only preheat orc file meta
                        if (filePath.getName().toUpperCase().endsWith(ColumnarFileType.ORC.name())) {
                            // preheat all meta from orc file.
                            PreheatFileMeta preheat =
                                PREHEATED_CACHE.get(filePath, () -> preheat(filePath));

                            preheatFileMetaMap.put(filePath, preheat);

                            if (enableIndexPruning && tso != null) {
                                // prune the row-groups.
                                long loadIndexStart = System.nanoTime();
                                // TODO support multi columns for sort key
                                List<Integer> sortKeys =
                                    columnarManager.getSortKeyColumns(tso, schemaName, logicalTableName);
                                List<Integer> orcIndexes =
                                    columnarManager.getPhysicalColumnIndexes(tso, filePath.getName(),
                                        IntStream.range(0, columns.size()).boxed().collect(
                                            Collectors.toList()));
                                IndexPruner indexPruner = ColumnarPruneManager.getIndexPruner(
                                    filePath, preheat, columns, sortKeys.get(0), orcIndexes, enableOssCompatible);

                                if (tracer != null) {
                                    tracer.tracePruneInit(logicalTableName,
                                        PruneUtils.display(columnPredicate, columns, indexPruneContext),
                                        System.nanoTime() - loadIndexStart);
                                }
                                long indexPruneStart = System.nanoTime();

                                // Only need row-groups containing deleted data.
                                RoaringBitmap rr = indexPruner.pruneOnlyDeletedRowGroups(deleteBitmap);

                                SortedMap<Integer, boolean[]> rs = indexPruner.pruneToSortMap(rr);
                                if (rr.isEmpty()) {
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
                    }
                    if (tracer != null && columnPredicate != null) {
                        tracer.tracePruneResult(logicalTableName,
                            PruneUtils.display(columnPredicate, columns, indexPruneContext),
                            filePaths.size(),
                            stripeNum, rgNum, pruneRgLeft);
                    }
                    if (logger.isDebugEnabled() && columnPredicate != null) {
                        logger.debug(
                            "[" + this.getClass().getSimpleName() + "]" + "prune result: " + traceId + ","
                                + logicalTableName + "," + filePaths.size() + ","
                                + stripeNum + "," + rgNum + "," + pruneRgLeft);
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
}
