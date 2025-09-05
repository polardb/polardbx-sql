package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ParamsDynamicJdbcSplitTest {
    private JdbcSplit jdbcSplit;

    @Before
    public void setUp() throws Exception {

        ExecutionContext context = new ExecutionContext();
        context.setClientIp("127.1");
        context.setTraceId("testTrace");
        byte[] hint = ExecUtils.buildDRDSTraceCommentBytes(context);
        String sql = "select pk from ? as tb1 where pk in (?)";
        BytesSql bytesSql = BytesSql.getBytesSql(sql);
        List<List<ParameterContext>> params = new ArrayList<>();
        List<ParameterContext> row = Lists.newArrayList();
        ParameterContext tb1 = new ParameterContext();
        tb1.setParameterMethod(ParameterMethod.setObject1);
        Object[] args1 = new Object[2];
        args1[0] = -1;
        args1[1] = "tb";
        tb1.setArgs(args1);
        row.add(tb1);

        ParameterContext tb2 = new ParameterContext();
        tb2.setParameterMethod(ParameterMethod.setDelegateInValue);
        Object[] args2 = new Object[1];
        args2[0] = 1;
        tb2.setArgs(args2);
        row.add(tb2);
        params.add(row);
        List<List<String>> tableNames = new ArrayList<>();
        List<String> tableName = Lists.newArrayList();
        tableName.add("test_table_name_1");
        tableNames.add(tableName);

        this.jdbcSplit =
            new JdbcSplit("ca", "sc", "db0", hint, bytesSql, null, params, "127.1", tableNames, ITransaction.RW.WRITE,
                true, null, new byte[] {0x01, 0x02, 0x03}, true, null, null);

    }

    @Test
    public void test1() {
        Chunk chunk = new Chunk(IntegerBlock.of(0, 1, 2, 3));

        ParamsDynamicJdbcSplit paramsDynamicJdbcSplit = new ParamsDynamicJdbcSplit(jdbcSplit, chunk, true);
        List<ParameterContext> parameterContexts = paramsDynamicJdbcSplit.getFlattedParams();
        Assert.assertTrue(parameterContexts.size() == 8);
        Assert.assertTrue(paramsDynamicJdbcSplit.getUnionBytesSql(false).size() == 214);
        Assert.assertTrue(paramsDynamicJdbcSplit.getUnionBytesSql(true).size() == 214);
    }

    @Test
    public void test2() {
        Chunk chunk = new Chunk(IntegerBlock.of(0, 1, 2));

        ParamsDynamicJdbcSplit paramsDynamicJdbcSplit = new ParamsDynamicJdbcSplit(jdbcSplit, chunk, false);
        List<ParameterContext> parameterContexts = paramsDynamicJdbcSplit.getFlattedParams();
        Assert.assertTrue(parameterContexts.size() == 2);
        Assert.assertTrue(paramsDynamicJdbcSplit.getUnionBytesSql(false).size() == 40);
        Assert.assertTrue(paramsDynamicJdbcSplit.getUnionBytesSql(true).size() == 40);
    }
}
