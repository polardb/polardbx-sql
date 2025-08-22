package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.alibaba.polardbx.executor.test.JSONParseTest.buildParams;
import static com.alibaba.polardbx.executor.test.JSONParseTest.buildTableNames;

public class DynamicJdbcSplitTest {

    private JdbcSplit jdbcSplit;

    @Before
    public void setUp() throws Exception {

        ExecutionContext context = new ExecutionContext();
        context.setClientIp("127.1");
        context.setTraceId("testTrace");
        byte[] hint = ExecUtils.buildDRDSTraceCommentBytes(context);
        String sql = "select pk from ? as tb1 where pk in (?) and 'bka_magic' = 'bka_magic'";
        BytesSql bytesSql = BytesSql.getBytesSql(sql);
        List<List<ParameterContext>> params = buildParams();
        List<List<String>> tableNames = buildTableNames();
        jdbcSplit =
            new JdbcSplit("ca", "sc", "db0", hint, bytesSql, null, params, "127.1", tableNames, ITransaction.RW.WRITE,
                true, null, new byte[] {0x01, 0x02, 0x03}, true, null, null);

    }

    @After
    public void tearDown() throws Exception {
        jdbcSplit = null;
    }

    private SqlNode buildCondition() {
        SqlIdentifier sqlIdentifier = new SqlIdentifier("a", SqlParserPos.ZERO);
        SqlDynamicParam dynamicParam = new SqlDynamicParam(4, SqlParserPos.ZERO);
        final SqlNode paramRow =
            new SqlBasicCall(SqlStdOperatorTable.ROW, new SqlNode[] {dynamicParam}, SqlParserPos.ZERO);

        return new SqlBasicCall(SqlStdOperatorTable.IN,
            new SqlNode[] {sqlIdentifier, paramRow},
            SqlParserPos.ZERO);
    }

    @Test
    public void test1() throws Exception {

        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);
        List<ParameterContext> parameterContexts = dynamicJdbcSplit.getFlattedParams();
        Assert.assertTrue(parameterContexts.size() == 20);
    }

    @Test
    public void test2() throws Exception {

        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);
        BytesSql bytesSql = dynamicJdbcSplit.getUnionBytesSql(false);
        Assert.assertTrue(bytesSql.size() == 710);
    }

    @Test
    public void test3() throws Exception {

        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);
        BytesSql bytesSql = dynamicJdbcSplit.getUnionBytesSql(true);
        Assert.assertTrue(bytesSql.size() == 70);
    }

}
