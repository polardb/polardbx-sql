package com.alibaba.polardbx.qatest.columnar.prune;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.selectBaseOneTable;

public class ColumnarTracerTest extends AutoReadBaseTestCase {

    public ColumnarTracerTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Parameterized.Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(selectBaseOneTable());
    }

    @Test
    public void testGlobalShowPruneTracer() throws SQLException {
        final String MPP_HINT =
            "enable_columnar_optimizer=true enable_master_mpp=true ENABLE_HTAP=true workload_type=ap";
        String traceHINT = String.format("trace /*+TDDL: %s */", MPP_HINT);

        String sqlSelect = "select * from select_base_one_one_db_one_tb " +
            "where (varchar_test= \"feed32feed\" or varchar_test in (\"nihaore\")) and (integer_test= 2 or integer_test > 3)";

        String sqlShow = "show prune tracer";

        //execute select sql
        ResultSet rs = JdbcUtil.executeQuery(traceHINT + sqlSelect, tddlConnection);
        Assert.assertTrue(rs.getMetaData().getColumnCount() == 20);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        Assert.assertTrue(count == 93);

        //execute show prune tracer
        ResultSet rsShow = JdbcUtil.executeQuery(sqlShow, tddlConnection);
        int showCount = 0;
        while (rsShow.next()) {
            showCount++;
        }
        rsShow.close();
        Assert.assertTrue(showCount > 0);
    }

}
