package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.DeletionFileReader;
import com.alibaba.polardbx.executor.columnar.SimpleDeletionFileReader;
import com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarFileMappingAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils.transformRexToIndexMergeTree;

/**
 * This scan pre-processor use designate delete bitmap.
 * For now, it is used for incremental check only.
 *
 * @author yaozhili
 */
public class SpecifiedDeleteBitmapPreProcessor extends DefaultScanPreProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SpecifiedDeleteBitmapPreProcessor.class);

    private final List<String> delFiles;
    private final List<Long> delBeginPos;
    private final List<Long> delEndPos;
    private final Engine engine;
    private Long tableId = null;

    public SpecifiedDeleteBitmapPreProcessor(Configuration configuration,
                                             FileSystem fileSystem, String schemaName,
                                             String logicalTableName, boolean enableIndexPruning,
                                             boolean enableOssCompatible,
                                             List<ColumnMeta> columns,
                                             List<RexNode> rexList,
                                             Map<Integer, ParameterContext> params,
                                             double groupsRatio, double deletionRatio,
                                             ColumnarManager columnarManager, Long tso,
                                             List<Long> columnFieldIdList,
                                             List<String> delFiles,
                                             List<Long> delBeginPos,
                                             List<Long> delEndPos,
                                             Engine engine,
                                             long tableId) {
        super(configuration, fileSystem, schemaName, logicalTableName, enableIndexPruning, enableOssCompatible, columns,
            rexList, params, groupsRatio, deletionRatio, columnarManager, tso, columnFieldIdList);
        if (null == delFiles) {
            Preconditions.checkArgument(delBeginPos == null && delEndPos == null);
            this.delFiles = new ArrayList<>();
            this.delBeginPos = new ArrayList<>();
            this.delEndPos = new ArrayList<>();
        } else {
            this.delFiles = delFiles;
            this.delBeginPos = delBeginPos;
            this.delEndPos = delEndPos;
        }
        Preconditions.checkArgument(
            (this.delFiles.size() == this.delBeginPos.size() && this.delBeginPos.size() == this.delEndPos.size()),
            "delFiles size not match!");
        this.engine = engine;
        this.tableId = tableId;
    }

    public void addDelFiles(List<String> delFiles,
                            List<Long> delStartPos,
                            List<Long> delEndPos,
                            Long tableId) {
        this.delFiles.addAll(delFiles);
        this.delBeginPos.addAll(delStartPos);
        this.delEndPos.addAll(delEndPos);
        this.tableId = tableId;
    }

    @Override
    public ListenableFuture<?> prepare(ExecutorService executor, String traceId, ColumnarTracer tracer) {
        SettableFuture<?> future = SettableFuture.create();
        indexPruneContext.setPruneTracer(tracer);
        // Is there a more elegant execution mode?
        executor.submit(
            () -> {
                DynamicColumnarManager manager = DynamicColumnarManager.getInstance();
                int stripeNum = 0;
                int rgNum = 0;
                int pruneRgLeft = 0;
                try {
                    // rex+pc -> distribution segment condition + indexes merge tree
                    ColumnPredicatePruningInf columnPredicate =
                        transformRexToIndexMergeTree(rexList, indexPruneContext);

                    for (Path filePath : filePaths) {
                        // only preheat orc file meta
                        if (filePath.getName().toUpperCase().endsWith(ColumnarFileType.ORC.name())) {
                            // preheat all meta from orc file.
                            PreheatFileMeta preheat = PREHEATED_CACHE.get(filePath, () -> preheat(filePath));

                            preheatFileMetaMap.put(filePath, preheat);

                            // if pruning is disabled, mark all row-groups as selected.
                            generateFullMatrix(filePath);
                        }
                    }
                    if (tracer != null && columnPredicate != null) {
                        tracer.tracePruneResult(logicalTableName,
                            PruneUtils.display(columnPredicate, columns, indexPruneContext),
                            filePaths.size(),
                            stripeNum, rgNum, pruneRgLeft);
                    }
                    generateDeletion();
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

    protected void generateDeletion() {
        if (null == delFiles || delFiles.isEmpty()) {
            return;
        }
        // Generate delete bitmap.
        Map<String, RoaringBitmap> fileIdToBitmap = new HashMap<>();
        for (int i = 0; i < delFiles.size(); i++) {
            final String fileName = delFiles.get(i);
            final long start = delBeginPos.get(i);
            final long end = delEndPos.get(i);
            if (start == end) {
                continue;
            }
            try (SimpleDeletionFileReader fileReader = new SimpleDeletionFileReader()) {
                try {
                    fileReader.open(
                        engine,
                        fileName,
                        (int) start,
                        (int) (end - start)
                    );
                } catch (IOException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_LOAD_DEL_FILE, e,
                        String.format("Failed to open delete bitmap file, filename: %s, offset: %d, length: %d",
                            fileName, start, end - start));
                }

                DeletionFileReader.DeletionEntry deletionEntry;
                while (fileReader.position() < end && null != (deletionEntry = fileReader.next())) {
                    final int fileId = deletionEntry.getFileId();
                    final RoaringBitmap bitmap = deletionEntry.getBitmap();
                    fileIdToBitmap.compute(String.valueOf(fileId), (k, v) -> {
                        if (v == null) {
                            return bitmap;
                        } else {
                            v.or(bitmap);
                            return v;
                        }
                    });
                }
            }
        }

        if (fileIdToBitmap.isEmpty()) {
            return;
        }

        // Convert file id to file name.
        FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarFileMappingAccessor accessor = new ColumnarFileMappingAccessor();
            accessor.setConnection(connection);
            accessor.queryByFileIdList(fileIdToBitmap.keySet(), schemaName, tableId).forEach(record -> deletions.put(
                // csv/orc file name
                FileSystemUtils.buildPath(fileSystem, record.getFileName(), true),
                // find bitmap by file id
                fileIdToBitmap.get(String.valueOf(record.getColumnarFileId()))
            ));
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER, t, "Failed to diff del-bitmap files");
        }

        if (fileIdToBitmap.size() != deletions.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER,
                "Failed to generate specified del-bitmap: Size not match.");
        }
    }

}
