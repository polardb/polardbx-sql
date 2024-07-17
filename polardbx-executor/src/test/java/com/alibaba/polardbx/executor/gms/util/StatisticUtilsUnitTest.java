package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_HLL;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAINTENANCE_TIME_START;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.SELECT_TABLE_ROWS_SQL;

/**
 * @author fangwu
 */
public class StatisticUtilsUnitTest {

    @Test
    public void testConstructScanSamplingSql() {
        // 准备测试数据
        String logicalTableName = "testTable";
        float sampleRate = 0.5f;
        List<ColumnMeta> columnMetaList = new ArrayList<>();
        columnMetaList.add(new ColumnMeta("testTable", "col1", "c1", null));
        columnMetaList.add(new ColumnMeta("testTable", "col2", "c2", null));

        // 调用待测试的函数
        String sql = StatisticUtils.constructScanSamplingSql(logicalTableName, columnMetaList, sampleRate);

        // 构建期望的结果
        String expectedSqlStart =
            "/*+TDDL:cmd_extra(enable_post_planner=false,enable_index_selection=false,merge_union=false,enable_direct_plan=false,sample_percentage=50.0) */ "
                + "select `col1`,`col2` from `testTable`";

        // 断言结果是否正确
        Assert.assertTrue(sql.startsWith(expectedSqlStart));

    }

    @Test
    public void testBuildCollectRowCountSql() {
        String[] tbls = {
            "select_base_four_multi_db_multi_tb_Nu9i_00", "select_base_four_multi_db_multi_tb_Nu9i_01",
            "select_base_four_multi_db_multi_tb_Nu9i_02", "select_base_four_multi_db_multi_tb_Nu9i_03",
            "select_base_four_multi_db_multi_tb_Nu9i_06"};
        String sql = StatisticUtils.buildCollectRowCountSql(Lists.newArrayList(tbls));
        System.out.println(sql);
        Assert.assertTrue(
            ("SELECT table_schema, table_name, table_rows FROM information_schema.tables "
                + "WHERE TABLE_NAME IN ("
                + "'select_base_four_multi_db_multi_tb_Nu9i_00',"
                + "'select_base_four_multi_db_multi_tb_Nu9i_01',"
                + "'select_base_four_multi_db_multi_tb_Nu9i_02',"
                + "'select_base_four_multi_db_multi_tb_Nu9i_03',"
                + "'select_base_four_multi_db_multi_tb_Nu9i_06')")
                .equals(sql));

        sql = StatisticUtils.buildCollectRowCountSql(null);
        Assert.assertTrue(SELECT_TABLE_ROWS_SQL.equals(sql));
        sql = StatisticUtils.buildCollectRowCountSql(Collections.emptyList());
        Assert.assertTrue(SELECT_TABLE_ROWS_SQL.equals(sql));
    }

    /**
     * test com.alibaba.polardbx.executor.utils.ExecUtils#needSketchInterrupted() and its branch
     */
    @Test
    public void testNeedSketchInterrupted() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        // Test sketch interrupted by ENABLE_BACKGROUND_STATISTIC_COLLECTION=false
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap()
            .put("ENABLE_BACKGROUND_STATISTIC_COLLECTION", "false");

        Assert.assertTrue(InstConfUtil.getBool(ENABLE_BACKGROUND_STATISTIC_COLLECTION) == false);

        Pair<Boolean, String> pair = ExecUtils.needSketchInterrupted();

        Assert.assertTrue(pair.getKey());

        Assert.assertTrue(pair.getValue().equals("ENABLE_BACKGROUND_STATISTIC_COLLECTION not enabled"));

        // revert ENABLE_BACKGROUND_STATISTIC_COLLECTION = true
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap()
            .put("ENABLE_BACKGROUND_STATISTIC_COLLECTION", "true");
        Assert.assertTrue(InstConfUtil.getBool(ENABLE_BACKGROUND_STATISTIC_COLLECTION));

        // Test  sketch interrupted by ENABLE_HLL=false
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("ENABLE_HLL", "false");
        Assert.assertTrue(InstConfUtil.getBool(ENABLE_HLL) == false);

        pair = ExecUtils.needSketchInterrupted();

        Assert.assertTrue(pair.getKey());

        Assert.assertTrue(pair.getValue().equals("ENABLE_HLL not enabled"));

        // revert ENABLE_HLL = true
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("ENABLE_HLL", "true");
        Assert.assertTrue(InstConfUtil.getBool(ENABLE_HLL));
    }

    /**
     * test com.alibaba.polardbx.gms.config.impl.InstConfUtil#isInMaintenanceTimeWindow(java.util.Calendar)
     */
    @Test
    public void testIsInMaintenanceTimeWindow() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Calendar calendar = Calendar.getInstance();

        calendar.set(Calendar.HOUR_OF_DAY, 1);
        Assert.assertTrue(!InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 2);
        Assert.assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 3);
        Assert.assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 4);
        Assert.assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 4);
        calendar.set(Calendar.MINUTE, 40);
        Assert.assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 5);
        calendar.set(Calendar.MINUTE, 1);
        Assert.assertTrue(!InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        // mock error config for MAINTENANCE_TIME_START / MAINTENANCE_TIME_END
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("MAINTENANCE_TIME_START", "xx");
        Assert.assertTrue(InstConfUtil.getOriginVal(MAINTENANCE_TIME_START).equals("xx"));

        Assert.assertTrue(!InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().remove("MAINTENANCE_TIME_START");
        Assert.assertTrue(InstConfUtil.getOriginVal(MAINTENANCE_TIME_START).equals("02:00"));

        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("MAINTENANCE_TIME_START", "23:00");
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("MAINTENANCE_TIME_END", "03:00");
        calendar.set(Calendar.HOUR_OF_DAY, 1);
        Assert.assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 0);
        Assert.assertTrue(InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));

        calendar.set(Calendar.HOUR_OF_DAY, 4);
        Assert.assertTrue(!InstConfUtil.isInMaintenanceTimeWindow(calendar, ConnectionParams.MAINTENANCE_TIME_START,
            ConnectionParams.MAINTENANCE_TIME_END));
    }
}
