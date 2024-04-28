package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaColumnStatistics;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTables;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.ArrayList;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;

/**
 * @author shengyu
 */
public class InformationSchemaColumnStatisticsHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaColumnStatisticsHandler.class);

    public InformationSchemaColumnStatisticsHandler(
        VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaColumnStatistics;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        InformationSchemaColumnStatistics informationSchemaColumnStatistics
            = (InformationSchemaColumnStatistics) virtualView;
        InformationSchemaTables informationSchemaTables =
            new InformationSchemaTables(informationSchemaColumnStatistics.getCluster(),
                informationSchemaColumnStatistics.getTraitSet());

        informationSchemaTables.copyFilters(informationSchemaColumnStatistics);

        Cursor tablesCursor = null;

        try {
            tablesCursor = virtualViewHandler.handle(informationSchemaTables, executionContext);

            Row row;
            while ((row = tablesCursor.next()) != null) {
                String tableSchema = row.getString(1);
                String tableName = row.getString(2);
                if (InformationSchema.NAME.equalsIgnoreCase(tableSchema)) {
                    continue;

                }

                try {
                    TableMeta tableMeta =
                        OptimizerContext.getContext(tableSchema).getLatestSchemaManager().getTable(tableName);

                    for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
                        String columnName = columnMeta.getName();
                        if (columnName.equalsIgnoreCase(IMPLICIT_COL_NAME)) {
                            continue;
                        }
                        String histogramStr =
                            StatisticManager.getInstance().getHistogramSerializable(tableSchema, tableName, columnName);
                        if (histogramStr != null) {
                            cursor.addRow(new Object[] {
                                tableSchema,
                                tableName,
                                columnName,
                                histogramStr
                            });
                        }
                    }
                } catch (Throwable t) {
                    logger.error(t);
                }
            }
        } finally {
            if (tablesCursor != null) {
                tablesCursor.close(new ArrayList<>());
            }
        }

        return cursor;
    }
}
