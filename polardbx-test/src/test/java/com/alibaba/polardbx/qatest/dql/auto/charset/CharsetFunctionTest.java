package com.alibaba.polardbx.qatest.dql.auto.charset;

import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class CharsetFunctionTest extends CharsetTestBase {
    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return PARAMETERS_FOR_PART_TBL;
    }

    public CharsetFunctionTest(String table, String suffix) {
        this.table = randomTableName(table, 4);
        this.suffix = suffix;
    }

    @Test
    public void testCharset() {
        if (PropertiesUtil.usePrepare()) {
            // current prepare mode does not support charset
            return;
        }
        String sql = "select charset(_latin1'abc' collate latin1_general_cs)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCollation() {
        if (PropertiesUtil.usePrepare()) {
            // current prepare mode does not support collation
            return;
        }
        String sql = "select collation(_latin1'abc' collate latin1_general_cs)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
