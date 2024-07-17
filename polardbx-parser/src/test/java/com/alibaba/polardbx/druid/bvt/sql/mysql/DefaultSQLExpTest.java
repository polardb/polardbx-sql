package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class DefaultSQLExpTest {

    private TestExpr testExpr;

    @Parameterized.Parameters(name = "{0}")
    public static List<TestExpr> parameters() {
        return Lists.newArrayList(
            new TestExpr("TINYINT", "1", false),
            new TestExpr("INT", "1", false),
            new TestExpr("BIGINT", "1", false),
            new TestExpr("TINYINT", "NULL", false),
            new TestExpr("TINYINT", "", false, "''"),
            new TestExpr("TINYINT", "1+2", false),
            new TestExpr("INT", "NULL", false),
            new TestExpr("BIGINT", "NULL", false),
            new TestExpr("DECIMAL", "34.6", false),
            new TestExpr("DOUBLE", "2.5", false),
            new TestExpr("DOUBLE", "2.5-23", false),
            new TestExpr("FLOAT", "2.5", false),
            new TestExpr("BIT", "01", true, "0x01"),
            new TestExpr("CHAR", "default", false, "'default'"),
            new TestExpr("VARCHAR", "default", false, "'default'"),
            new TestExpr("VARCHAR", "", false, "''"),
            new TestExpr("ENUM", "value1", false, "'value1'"),
            new TestExpr("DATE", "1000-01-01", false, "'1000-01-01'"),
            new TestExpr("TIME", "00:00:00", false, "'00:00:00'"),
            new TestExpr("datetime", "CURRENT_TIMESTAMP", false),
            new TestExpr("datetime", "1970-01-01 00:00:00", false, "'1970-01-01 00:00:00'"),
            new TestExpr("year", "1976", false, "'1976'"),
            new TestExpr("binary", "FF", true, "0xFF"),
            new TestExpr("varbinary", "hello", false, "'hello'"),
            new TestExpr("varbinary", "0x48656C6C6F", true, "'0x48656C6C6F'")

        );
    }

    public DefaultSQLExpTest(TestExpr testExpr) {
        this.testExpr = testExpr;
    }

    @Test
    public void testDefaultSQLExpr() {
        SQLExpr sqlExpr = SQLUtils.toDefaultSQLExpr(testExpr.dataType, testExpr.val, testExpr.flag);
        Assert.assertEquals(testExpr.expect, sqlExpr.toString());
    }

    public static class TestExpr {

        public SQLDataType dataType;
        public String val;
        public long flag;
        public String expect;

        public TestExpr(String dataTypeName, String val, boolean isHex, String expect) {
            this.dataType = new SQLDataTypeImpl(dataTypeName);
            this.val = val;
            this.flag = isHex ? 2 : 0;
            this.expect = expect;
        }

        public TestExpr(String dataTypeName, String val, boolean isHex) {
            this.dataType = new SQLDataTypeImpl(dataTypeName);
            this.val = val;
            this.flag = isHex ? 2 : 0;
            this.expect = val;
        }

        @Override
        public String toString() {
            return "TestExpr{" +
                "dataType=" + dataType +
                ", val='" + val + '\'' +
                ", flag=" + flag +
                '}';
        }
    }

}
