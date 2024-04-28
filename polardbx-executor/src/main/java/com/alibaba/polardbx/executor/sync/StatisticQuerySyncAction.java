package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.parse.util.Pair;
import com.alibaba.polardbx.optimizer.view.InformationSchemaStatisticsData;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.getColumnMetas;

public class StatisticQuerySyncAction implements ISyncAction {

    public StatisticQuerySyncAction() {
    }

    @Override
    public ResultCursor sync() {
        Map<String, Map<String, StatisticManager.CacheLine>> statisticCache =
            StatisticManager.getInstance().getStatisticCache();

        if (statisticCache == null) {
            return null;
        }
        ArrayResultCursor result = new ArrayResultCursor("statistics_data");
        for (Pair<String, SqlTypeName> pair : InformationSchemaStatisticsData.meta) {
            result.addColumn(pair.getKey(), InformationSchemaStatisticsData.transform(pair.getValue()));
        }
        result.initMeta();
        String host = TddlNode.getHost();

        for (Map.Entry<String, Map<String, StatisticManager.CacheLine>> entrySchema : statisticCache.entrySet()) {
            String schema = entrySchema.getKey();
            if (SystemDbHelper.isDBBuildIn(schema)) {
                continue;
            }
            for (Map.Entry<String, StatisticManager.CacheLine> entryTable : entrySchema.getValue().entrySet()) {
                String table = entryTable.getKey();

                // skip oss table cause sample process would do the same
                if (StatisticUtils.isFileStore(schema, table)) {
                    continue;
                }

                StatisticManager.CacheLine cacheLine = StatisticManager.getInstance().getCacheLine(schema, table);
                List<ColumnMeta> columns = getColumnMetas(false, schema, table);
                if (columns == null) {
                    continue;
                }
                Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();
                for (ColumnMeta columnMeta : columns) {
                    String column = columnMeta.getName().toLowerCase();
                    StatisticResult statisticResult =
                        StatisticManager.getInstance().getCardinality(schema, table, column, false, false);

                    Histogram histogram = histogramMap == null ? null : histogramMap.get(column);
                    String topN = cacheLine.getTopN(column) == null ? "" : cacheLine.getTopN(column).manualReading();
                    result.addRow(new Object[] {
                        host,
                        schema,
                        table,
                        column,
                        cacheLine.getRowCount(),
                        statisticResult.getLongValue(),
                        statisticResult.getSource(),
                        topN == null ? "" : topN,
                        histogram == null || histogram.getBuckets().size() == 0 ?
                            "" : histogram.manualReading(),
                        cacheLine.getSampleRate()
                    });
                }

            }
        }

        return result;
    }
}

