package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.utils.Assert;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Collections;

import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.SELECT_TABLE_ROWS_SQL;

/**
 * @author fangwu
 */
public class StatisticUtilsUnitTest {

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
}
