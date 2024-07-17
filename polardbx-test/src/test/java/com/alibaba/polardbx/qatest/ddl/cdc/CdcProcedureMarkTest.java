package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-30 17:43
 **/
public class CdcProcedureMarkTest extends CdcBaseTest {

    @Test
    public void testCdcDdlRecord() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            executeAndCheck(stmt, "drop PROCEDURE if exists pro_test", 0);
            executeAndCheck(stmt, "CREATE PROCEDURE pro_test()\n"
                + "  BEGIN\n"
                + "  DECLARE a CHAR(16);\n"
                + "  DECLARE b, c int;\n"
                + "  DECLARE cur1 CURSOR FOR SELECT data, id FROM t1 order by id;\n"
                + "  DECLARE cur2 CURSOR FOR SELECT id FROM t2 order by id;\n"
                + "  DECLARE CONTINUE HANDLER FOR NOT FOUND begin LEAVE read_loop; end;\n"
                + "\n"
                + "  OPEN cur1;\n"
                + "  OPEN cur2;\n"
                + "\n"
                + "  read_loop: LOOP\n"
                + "    FETCH cur1 INTO a, b;\n"
                + "    FETCH cur2 INTO c;\n"
                + "    IF b < c THEN\n"
                + "    INSERT INTO t3 VALUES (b, a);\n"
                + "    ELSE\n"
                + "    INSERT INTO t3 VALUES (c, a);\n"
                + "    END IF;\n"
                + "  END LOOP;\n"
                + "\n"
                + "  CLOSE cur1;\n"
                + "  CLOSE cur2;\n"
                + "  END;"
                + "RETURN x*y*31", 1);
            executeAndCheck(stmt, "drop PROCEDURE pro_test", 1);
            executeAndCheck(stmt, "drop PROCEDURE if exists pro_test", 0);
        }
    }

    private void executeAndCheck(Statement stmt, String ddl, int expectCount) throws SQLException {
        String tokenHints = buildTokenHints();
        String sql = tokenHints + ddl;
        stmt.executeUpdate(sql);

        String currentSchema = getCurrentDbName(stmt);

        List<DdlRecordInfo> list = getDdlRecordInfoListByToken(tokenHints);
        Assert.assertEquals(expectCount, list.size());
        if (expectCount != 0) {
            Assert.assertEquals(sql, list.get(0).getDdlSql());
            Assert.assertEquals(currentSchema, list.get(0).getSchemaName());
            Assert.assertEquals("pro_test", list.get(0).getTableName());
            Assert.assertEquals(DdlScope.Schema.getValue(), list.get(0).getDdlExtInfo().getDdlScope());
        }
    }
}
