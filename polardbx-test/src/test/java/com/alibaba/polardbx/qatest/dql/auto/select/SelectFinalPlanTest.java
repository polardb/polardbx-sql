package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

public class SelectFinalPlanTest extends AutoReadBaseTestCase {
    @Parameterized.Parameters(name = "{index}:table1={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectFinalPlanTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    @Test
    public void selectPagingForceTest() {
        String sql = "select * from " + baseOneTableName +
            " paging_force index(primary) where pk = 1 and integer_test > 10 limit 10;";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }
}
