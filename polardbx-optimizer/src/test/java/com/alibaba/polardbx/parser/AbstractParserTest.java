package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;

public class AbstractParserTest extends TestCase {
    protected static final List<SQLParserFeature> DEFAULT_FEATURES = Arrays.asList(
        SQLParserFeature.TDDLHint,
        SQLParserFeature.EnableCurrentUserExpr,
        SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DRDSBaseline,
        SQLParserFeature.DrdsGSI,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DrdsCCL
    );

    protected void parseSqlShouldFail(String sql) {
        MySqlStatementParser parser =
            new MySqlStatementParser(ByteString.from(sql), DEFAULT_FEATURES.toArray(new SQLParserFeature[0]));
        try {
            parser.parseStatementList().get(0);
        } catch (ParserException e) {
            return;
        }
        Assert.fail(String.format("parse sql: %s should fail, but not!", sql));
    }

    public void testEmpty() {
    }
}
