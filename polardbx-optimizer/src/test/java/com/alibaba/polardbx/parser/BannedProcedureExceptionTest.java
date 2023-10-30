package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;

public class BannedProcedureExceptionTest extends TestCase {
    private static final List<SQLParserFeature> DEFAULT_FEATURES = Arrays.asList(
        SQLParserFeature.TDDLHint,
        SQLParserFeature.EnableCurrentUserExpr,
        SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DRDSBaseline,
        SQLParserFeature.DrdsGSI,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DrdsCCL
    );

    public void testParseSqlState() {
        String sql = "CREATE PROCEDURE p2(a INT) "
            + "BEGIN   "
            + "DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' BEGIN select 1; END;   "
            + "DECLARE i int default 0;   "
            + "DECLARE col1 INT;   "
            + "DECLARE col2 CHAR(64);   "
            + "DECLARE cur CURSOR FOR SELECT col1, col2 FROM tb1;   "
            + "OPEN cur;   "
            + "WHILE i < a DO     "
            + "FETCH cur INTO col1, col2;     "
            + "SELECT @col1, col2;     "
            + "SET i = i + 1;   "
            + "END WHILE;   "
            + "CLOSE cur; "
            + "END;";
        parseSqlShouldFail(sql, "exception handler for sqlstate not support");
    }

    public void testParseSqlException() {
        String sql = "CREATE PROCEDURE p2(a INT) "
            + "BEGIN   "
            + "DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;   "
            + "DECLARE i int default 0;   "
            + "DECLARE col1 INT;   "
            + "DECLARE col2 CHAR(64);   "
            + "DECLARE cur CURSOR FOR SELECT col1, col2 FROM tb1;   "
            + "OPEN cur;   "
            + "WHILE i < a DO     "
            + "FETCH cur INTO col1, col2;     "
            + "SELECT @col1, col2;     "
            + "SET i = i + 1;   "
            + "END WHILE;   "
            + "CLOSE cur; "
            + "END;";
        parseSqlShouldFail(sql, "exception handler for sql exception not support");
    }

    public void testParseSqlWarning() {
        String sql = "CREATE PROCEDURE p2(a INT) "
            + "BEGIN   "
            + "DECLARE CONTINUE HANDLER FOR SQLWARNING BEGIN END;   "
            + "DECLARE i int default 0;   "
            + "DECLARE col1 INT;   "
            + "DECLARE col2 CHAR(64);   "
            + "DECLARE cur CURSOR FOR SELECT col1, col2 FROM tb1;   "
            + "OPEN cur;   "
            + "WHILE i < a DO     "
            + "FETCH cur INTO col1, col2;     "
            + "SELECT @col1, col2;     "
            + "SET i = i + 1;   "
            + "END WHILE;   "
            + "CLOSE cur; "
            + "END;";
        parseSqlShouldFail(sql, "exception handler for sql warning not support");
    }

    private void parseSqlShouldFail(String sql, String errorMsg) {
        MySqlStatementParser parser =
            new MySqlStatementParser(ByteString.from(sql), DEFAULT_FEATURES.toArray(new SQLParserFeature[0]));
        try {
            parser.parseStatementList().get(0);
        } catch (ParserException e) {
            if (e.getMessage().toLowerCase().contains(errorMsg.toLowerCase())) {
                return;
            }
            Assert.fail(String.format("parse sql: %s should fail with exception: %s, but not!", sql, errorMsg));
        }
        Assert.fail(String.format("parse sql: %s should fail, but not!", sql));
    }
}
