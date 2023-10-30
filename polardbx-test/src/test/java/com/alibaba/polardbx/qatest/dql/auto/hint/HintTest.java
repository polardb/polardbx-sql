package com.alibaba.polardbx.qatest.dql.auto.hint;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author fangwu
 */
public class HintTest extends AutoReadBaseTestCase {
    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public HintTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    /**
     * use hint flag inside of Execution Context should be cleared
     * by every statement closing
     */
    @Test
    public void testHintFlag() throws SQLException {
        String hint = "/*TDDL:a()*/ ";
        String sql = "select * from " + baseOneTableName;
        tddlConnection.createStatement().execute("clear plancache");

        // execute first and put it into plancache
        tddlConnection.createStatement().execute(sql);

        // get explain, a cached plan should be returned
        String result = getExplainResult(tddlConnection, sql);
        Assert.assertTrue(result.contains("HitCache:true"));

        // get explain by hint, a new plan should be returned
        result = getExplainResult(tddlConnection, hint + sql);
        Assert.assertTrue(result.contains("HitCache:false"));

        // go back to no hint sql, a cached plan should be returned
        result = getExplainResult(tddlConnection, sql);
        Assert.assertTrue(result.contains("HitCache:true"));
    }

    @Test
    public void testHintFlagInsideTran() throws SQLException {
        String hint = "/*TDDL:a()*/ ";
        String sql = "select * from " + baseOneTableName;
        tddlConnection.createStatement().execute("clear plancache");
        try {
            tddlConnection.createStatement().execute("begin");

            // execute first and put it into plancache
            tddlConnection.createStatement().execute(sql);

            // get explain, a cached plan should be returned
            String result = getExplainResult(tddlConnection, sql);
            Assert.assertTrue(result.contains("HitCache:true"));

            // get explain by hint, a new plan should be returned
            result = getExplainResult(tddlConnection, hint + sql);
            Assert.assertTrue(result.contains("HitCache:false"));

            // go back to no hint sql, a cached plan should be returned
            result = getExplainResult(tddlConnection, sql);
            Assert.assertTrue(result.contains("HitCache:true"));
        } finally {
            // clear trans
            tddlConnection.createStatement().execute("rollback");
        }

    }
}
