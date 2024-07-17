package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class ProcedureSpillTest extends AutoReadBaseTestCase {
    private static final String PROCEDURE_NAME = "test_procedure_spill";

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwo());
    }

    public ProcedureSpillTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Test
    public void testFullTypeSpill() throws SQLException {
        String sql = "CREATE PROCEDURE %s(count int, off int) "
            + "BEGIN   "
            + "DECLARE i int default 0;   "
            + "DECLARE x1 INT;   "
            + "DECLARE x2 INT;   "
            + "DECLARE x3 VARCHAR(255);   "
            + "DECLARE x4 CHAR(255);   "
            + "DECLARE x5 blob;   "
            + "DECLARE x6 tinyint;   "
            + "DECLARE x7 tinyint;   "
            + "DECLARE x8 smallint;   "
            + "DECLARE x9 mediumint;   "
            + "DECLARE x10 bit;   "
            + "DECLARE x11 bigint;   "
            + "DECLARE x12 float;   "
            + "DECLARE x13 double;   "
            + "DECLARE x14 decimal(10, 0);   "
            + "DECLARE x15 date;   "
            + "DECLARE x16 time;   "
            + "DECLARE x17 datetime;   "
            + "DECLARE x18 timestamp;   "
            + "DECLARE x19 year;   "
            + "DECLARE x20 mediumtext;   "
            + "DECLARE x21 VARCHAR(65535);   "

            + "DECLARE y1 INT;   "
            + "DECLARE y2 INT;   "
            + "DECLARE y3 VARCHAR(255);   "
            + "DECLARE y4 CHAR(255);   "
            + "DECLARE y5 blob;   "
            + "DECLARE y6 tinyint;   "
            + "DECLARE y7 tinyint;   "
            + "DECLARE y8 smallint;   "
            + "DECLARE y9 mediumint;   "
            + "DECLARE y10 bit;   "
            + "DECLARE y11 bigint;   "
            + "DECLARE y12 float;   "
            + "DECLARE y13 double;   "
            + "DECLARE y14 decimal(10, 0);   "
            + "DECLARE y15 date;   "
            + "DECLARE y16 time;   "
            + "DECLARE y17 datetime;   "
            + "DECLARE y18 timestamp;   "
            + "DECLARE y19 year;   "
            + "DECLARE y20 mediumtext;   "
            + "DECLARE y21 VARCHAR(65535);   "

            + "DECLARE CONTINUE HANDLER FOR NOT FOUND\n"
            + "\t\tBEGIN\n"
            + "\t\t\tSET @count = i;\n"
            + "\t\tEND;"
            + "DECLARE cur CURSOR FOR SELECT *, repeat(char_test, 255) FROM %s order by pk;   "
            + "DECLARE cur2 CURSOR FOR SELECT *, repeat(char_test, 255) FROM %s order by pk limit 1 offset off;   "
            + "OPEN cur;   "
            + "OPEN cur2;   "
            + "set @check = null; "
            + "set @count = null; "
            + "WHILE i < count DO     "
            + "FETCH cur INTO x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20, x21;     "
            + "IF i = off then begin "
            + "fetch cur2 into y1, y2, y3, y4, y5, y6, y7, y8, y9, y10, y11, y12, y13, y14, y15, y16, y17, y18, y19, y20, y21; "
            + "if x1 = y1 and x2 = y2 and ((x3 = y3) or (isNull(x3) and isNull(y3))) and ((x4 = y4) or (isNull(x4) and isNull(y4))) "
            + "and x5 = y5 and x6 = y6 and x7 = y7 and x8 = y8 and x9 = y9 and x10 = y10 "
            + "and x11 = y11 and x12 = y12 and x13 = y13 and x14 = y14 and x15 = y15 and x16 = y16 "
            + "and x17 = y17 and x18 = y18 and x19 = y19 and x20 = y20 and x21 = y21 "

            + "then set @check = 1; else set @check = -1; end if; end; end if;"
            + "SET i = i + 1;   "
            + "END WHILE;   "
            + "FETCH cur INTO x1, x2;     "
            + "CLOSE cur; "
            + "CLOSE cur2; "
            + "END; ";
        JdbcUtil.executeSuccess(tddlConnection, String.format(sql, PROCEDURE_NAME, baseOneTableName, baseOneTableName));
        long count = getRecordNum("select count(*) from " + baseOneTableName, tddlConnection);
        checkSpill(count, true);
        checkSpill(count, false);
    }

    private void checkSpill(long count, boolean spill) throws SQLException {
        JdbcUtil.executeSuccess(tddlConnection,
            "set PL_CURSOR_MEMORY_LIMIT = " + (spill ? 20 : ConnectionParams.PL_CURSOR_MEMORY_LIMIT.getDefault()));
        for (int i = 0; i < 50; ++i) {
            int random = RandomUtils.nextInt((int) count);
            JdbcUtil.executeSuccess(tddlConnection, String.format("call %s(%s, %s)", PROCEDURE_NAME, count, random));
            checkUserVar("check", 1, tddlConnection);
            checkUserVar("count", count, tddlConnection);
        }
    }

    private void checkUserVar(String name, long expect, Connection tddlConnection) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery("select @" + name, tddlConnection);
        if (rs.next()) {
            Assert.assertTrue(rs.getLong(1) == expect,
                String.format("result not match, expect %s, but get %s", expect, rs.getLong(1)));
        } else {
            Assert.fail("get empty result");
        }
    }

    private long getRecordNum(String sql, Connection tddlConnection) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        long count = -1;
        if (rs.next()) {
            count = rs.getLong(1);
        } else {
            Assert.fail("get empty result");
        }
        return count;
    }

    @Test
    public void testSpillWithJoin() throws SQLException {
        String sql = "CREATE PROCEDURE %s(count int, off int) "
            + "BEGIN   "
            + "DECLARE i int default 0;   "
            + "DECLARE x1 INT;   "
            + "DECLARE x2 INT;   "
            + "DECLARE x3 VARCHAR(255);   "
            + "DECLARE x4 CHAR(255);   "
            + "DECLARE x5 blob;   "
            + "DECLARE x6 tinyint;   "
            + "DECLARE x7 tinyint;   "
            + "DECLARE x8 smallint;   "
            + "DECLARE x9 mediumint;   "
            + "DECLARE x10 bit;   "
            + "DECLARE x11 bigint;   "
            + "DECLARE x12 float;   "
            + "DECLARE x13 double;   "
            + "DECLARE x14 decimal(10, 0);   "
            + "DECLARE x15 date;   "
            + "DECLARE x16 time;   "
            + "DECLARE x17 datetime;   "
            + "DECLARE x18 timestamp;   "
            + "DECLARE x19 year;   "
            + "DECLARE x20 mediumtext;   "
            + "DECLARE x21 VARCHAR(65535);   "

            + "DECLARE y1 INT;   "
            + "DECLARE y2 INT;   "
            + "DECLARE y3 VARCHAR(255);   "
            + "DECLARE y4 CHAR(255);   "
            + "DECLARE y5 blob;   "
            + "DECLARE y6 tinyint;   "
            + "DECLARE y7 tinyint;   "
            + "DECLARE y8 smallint;   "
            + "DECLARE y9 mediumint;   "
            + "DECLARE y10 bit;   "
            + "DECLARE y11 bigint;   "
            + "DECLARE y12 float;   "
            + "DECLARE y13 double;   "
            + "DECLARE y14 decimal(10, 0);   "
            + "DECLARE y15 date;   "
            + "DECLARE y16 time;   "
            + "DECLARE y17 datetime;   "
            + "DECLARE y18 timestamp;   "
            + "DECLARE y19 year;   "
            + "DECLARE y20 mediumtext;   "
            + "DECLARE y21 VARCHAR(65535);   "

            + "DECLARE CONTINUE HANDLER FOR NOT FOUND\n"
            + "\t\tBEGIN\n"
            + "\t\t\tSET @count = i;\n"
            + "\t\tEND;"
            + "DECLARE cur CURSOR FOR SELECT t1.*, repeat(t1.char_test, 255) FROM %s t1 join %s t2 on t1.pk = t2.pk order by pk;   "
            + "DECLARE cur2 CURSOR FOR SELECT t1.*, repeat(t1.char_test, 255) FROM %s t1 join %s t2 on t1.pk = t2.pk order by pk limit 1 offset off;   "
            + "OPEN cur;   "
            + "OPEN cur2;   "
            + "set @check = null; "
            + "set @count = null; "
            + "WHILE i < count DO     "
            + "FETCH cur INTO x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20, x21;     "
            + "IF i = off then begin "
            + "fetch cur2 into y1, y2, y3, y4, y5, y6, y7, y8, y9, y10, y11, y12, y13, y14, y15, y16, y17, y18, y19, y20, y21; "
            + "if x1 = y1 and x2 = y2 and ((x3 = y3) or (isNull(x3) and isNull(y3))) and ((x4 = y4) or (isNull(x4) and isNull(y4))) "
            + "and x5 = y5 and x6 = y6 and x7 = y7 and x8 = y8 and x9 = y9 and x10 = y10 "
            + "and x11 = y11 and x12 = y12 and x13 = y13 and x14 = y14 and x15 = y15 and x16 = y16 "
            + "and x17 = y17 and x18 = y18 and x19 = y19 and x20 = y20 and x21 = y21 "

            + "then set @check = 1; else set @check = -1; end if; end; end if;"
            + "SET i = i + 1;   "
            + "END WHILE;   "
            + "FETCH cur INTO x1, x2;     "
            + "CLOSE cur; "
            + "CLOSE cur2; "
            + "END; ";
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(sql, PROCEDURE_NAME, baseOneTableName, baseTwoTableName, baseOneTableName, baseTwoTableName));
        long count = getRecordNum(
            String.format("select count(*) FROM %s t1 join %s t2 on t1.pk = t2.pk", baseOneTableName, baseTwoTableName),
            tddlConnection);
        checkSpill(count, true);
        checkSpill(count, false);
    }

    @Before
    public void dropProcedure() {
        JdbcUtil.executeSuccess(tddlConnection, "drop procedure if exists " + PROCEDURE_NAME);
    }
}
