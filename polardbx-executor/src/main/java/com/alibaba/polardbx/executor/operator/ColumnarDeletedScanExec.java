package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.impl.DeletedScanPreProcessor;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class ColumnarDeletedScanExec extends ColumnarScanExec {

    public ColumnarDeletedScanExec(OSSTableScan ossTableScan,
                                   ExecutionContext context,
                                   List<DataType> outputDataTypes) {
        super(ossTableScan, context, outputDataTypes);
        // TODO: add a validation process to forbid ossTableScan containing filter get here.
    }

    @Override
    @NotNull
    protected DefaultScanPreProcessor getPreProcessor(OssSplit ossSplit,
                                                      String logicalSchema,
                                                      String logicalTableName,
                                                      TableMeta tableMeta,
                                                      FileSystem fileSystem,
                                                      Configuration configuration,
                                                      ColumnarManager columnarManager) {
        return new DefaultScanPreProcessor(
            configuration, fileSystem,

            // for pruning
            logicalSchema,
            logicalTableName,
            enableIndexPruning,
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE),
            tableMeta.getAllColumns(),
            ossTableScan.getOrcNode().getOriFilters(),
            ossSplit.getParams(),

            // for mock
            DEFAULT_GROUPS_RATIO,
            DEFAULT_DELETION_RATIO,

            // for columnar mode.
            columnarManager,
            ossSplit.getCheckpointTso(),
            tableMeta.getColumnarFieldIdList());
    }
}
