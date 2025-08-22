package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.FlashbackDeleteBitmapManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlashbackScanPreProcessor extends DefaultScanPreProcessor {

    private final Map<String, List<Pair<String, Long>>> allDelPositions;
    /**
     * partName -> FlashbackDeleteBitmapManager
     */
    private final Map<String, FlashbackDeleteBitmapManager> deleteBitmapManagers = new HashMap<>();

    public FlashbackScanPreProcessor(Configuration configuration,
                                     FileSystem fileSystem, String schemaName, String logicalTableName,
                                     boolean enableIndexPruning, boolean enableOssCompatible,
                                     List<ColumnMeta> columns,
                                     List<RexNode> rexList,
                                     Map<Integer, ParameterContext> params,
                                     double groupsRatio, double deletionRatio,
                                     ColumnarManager columnarManager, Long tso,
                                     List<Long> columnFieldIdList,
                                     Map<String, List<Pair<String, Long>>> allDelPositions) {
        super(configuration, fileSystem, schemaName, logicalTableName, enableIndexPruning, enableOssCompatible, columns,
            rexList, params, groupsRatio, deletionRatio, columnarManager, tso, columnFieldIdList);
        this.allDelPositions = allDelPositions;
    }

    @Override
    protected RoaringBitmap generateDeletion(Path filePath) {
        RoaringBitmap bitmap;
        if (tso == null || allDelPositions == null) {
            // in archive mode.
            bitmap = new RoaringBitmap();
        } else {
            // in columnar mode.
            FlashbackDeleteBitmapManager deleteBitmapManager = deleteBitmapManagers.computeIfAbsent(
                columnarManager.fileMetaOf(filePath.getName()).getPartitionName(),
                p -> columnarManager.getFlashbackDeleteBitmapManager(tso, schemaName, logicalTableName, p,
                    allDelPositions.get(p))
            );
            bitmap = deleteBitmapManager.getDeleteBitmapOf(filePath.getName());
        }
        deletions.put(filePath, bitmap);
        return bitmap;
    }
}
