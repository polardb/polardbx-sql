package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertManager;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.CDC_DB_NAME;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_META_DB_NAME;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.INFO_SCHEMA_DB_NAME;

/**
 * @author fangwu
 */
public class OptimizerAlertLoggerStatisticImplTest {

    @Test
    public void testStatisticAlertTest() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        String testSchema = "testDb";
        String testTbl = "testTb";

        // test built-in schema
        OptimizerAlertUtil.statisticsAlert(INFO_SCHEMA_DB_NAME, testTbl, null);
        OptimizerAlertUtil.statisticsAlert(CDC_DB_NAME, testTbl, null);
        OptimizerAlertUtil.statisticsAlert(DEFAULT_DB_NAME, testTbl, null);
        OptimizerAlertUtil.statisticsAlert(DEFAULT_META_DB_NAME, testTbl, null);
        Assert.assertTrue(0L == getStatisticAlertNum());

        OptimizerContext oc = BasePlannerTest.initOptiContext(testSchema, 4, true);
        OptimizerContext.setContext(oc);

        // test rowcount == 0
        StatisticManager.CacheLine c = new StatisticManager.CacheLine();
        c.setRowCount(0L);
        OptimizerAlertUtil.statisticsAlert(testSchema, testTbl, c);
        Assert.assertTrue(0L == getStatisticAlertNum());

        // test cols is null or empty
        c.setRowCount(1000L);
        OptimizerAlertUtil.statisticsAlert(testSchema, testTbl, c);
        Assert.assertTrue(0L == getStatisticAlertNum());

        // test histogram is null
        String colName = "col_test";
        List<String> colList = Lists.newArrayList();
        colList.add(colName);
        oc.getLatestSchemaManager().putTable(testTbl, mockTableMeta(testSchema, testTbl, colList));
        OptimizerAlertUtil.statisticsAlert(testSchema, testTbl, c);
        Assert.assertTrue(1L == getStatisticAlertNum());

        // test topn is not null and histogram is null
        c.setTopN(colName, new TopN(DataTypes.StringType, 1.0));
        OptimizerAlertUtil.statisticsAlert(testSchema, testTbl, c);
        Assert.assertTrue(0L == getStatisticAlertNum());

        // test topn is null and histogram is not null
        c.setTopN(colName, null);
        c.setHistogramMap(new HashMap<>());
        Histogram h = new Histogram(7, new IntegerType(), 1);
        h.getBuckets().add(new Histogram.Bucket());
        c.getHistogramMap().put(colName, h);
        OptimizerAlertUtil.statisticsAlert(testSchema, testTbl, c);
        Assert.assertTrue(0L == getStatisticAlertNum());

        // test both topn and histogram missing
        c.getHistogramMap().put(colName, null);
        OptimizerAlertUtil.statisticsAlert(testSchema, testTbl, c);
        Assert.assertTrue(1L == getStatisticAlertNum());

        // test full null case
        c.getAllNullCols().add(colName);
        OptimizerAlertUtil.statisticsAlert(testSchema, testTbl, c);
        Assert.assertTrue(0L == getStatisticAlertNum());
    }

    private TableMeta mockTableMeta(String schemaName, String tableName, List<String> colList) {
        List<ColumnMeta> columnMetas = new ArrayList<ColumnMeta>();
        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        for (String col : colList) {
            columnMetas.add(new ColumnMeta(tableName, col, col, new Field(tableName, col, typeFactory.createSqlType(
                SqlTypeName.VARCHAR), false, false)));
        }
        TableMeta tm =
            new TableMeta(schemaName, tableName, columnMetas, null, new ArrayList<IndexMeta>(), false,
                TableStatus.PUBLIC, 0, 0);
        return tm;
    }

    private long getStatisticAlertNum() {
        List<Pair<OptimizerAlertType, Long>> alterResult = OptimizerAlertManager.getInstance().collectByScheduleJob();
        for (Pair<OptimizerAlertType, Long> pair : alterResult) {
            if (pair.getKey() == OptimizerAlertType.STATISTIC_MISS) {
                return pair.getValue();
            }
        }
        return 0L;
    }
}
