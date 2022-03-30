package com.alibaba.polardbx.druid.bvt.sql.mysql.show;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

public class MySqlShowTest_43_deadlocks extends MysqlTest {

    public void testShowLocalDeadlocks() {
        final String sql = "SHOW LOCAL DEADLOCKS";

        final SQLStatement stmt = SQLUtils.parseStatements(sql, DbType.mysql).get(0);

        final String result = SQLUtils.toMySqlString(stmt);
        assertEquals("SHOW LOCAL DEADLOCKS", result);

        final SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(DbType.mysql);
        stmt.accept(visitor);

        assertEquals(0, visitor.getTables().size());
        assertEquals(0, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());
    }

    public void testShowGlobalDeadlocks() {
        final String sql = "SHOW GLOBAL DEADLOCKS";

        final SQLStatement stmt = SQLUtils.parseStatements(sql, DbType.mysql).get(0);

        final String result = SQLUtils.toMySqlString(stmt);
        assertEquals("SHOW GLOBAL DEADLOCKS", result);

        final SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(DbType.mysql);
        stmt.accept(visitor);

        assertEquals(0, visitor.getTables().size());
        assertEquals(0, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());
    }

    public void testShowLocalOthersFailed() {
        final String[] localOthers = new String[] {
            "SHOW LOCAL VARIABLES",
            "SHOW LOCAL SESSION VARIABLES",
            "SHOW LOCAL SESSION STATUS",
            "SHOW LOCAL BINARY LOGS",
        };

        boolean allFailed = true;
        for (String sql : localOthers) {
            try {
                SQLUtils.parseStatements(sql, DbType.mysql);

                // Should not reach here since an exception should be thrown before
                allFailed = false;
            } catch (ParserException e) {
                if (!StringUtils.containsIgnoreCase(e.getMessage(), "syntax error, expect DEADLOCKS")) {
                    Assert.fail(String.format("Wrong exception caught, SQL: %s, exception: %s", sql, e.getMessage()));
                }
            }
        }

        assertTrue(allFailed);
    }
}
