/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * @author fangwu
 */
public class JSONParseTest {

    @Test
    public void testJdbcSplit() throws Exception {
        ExecutionContext context = new ExecutionContext();
        context.setClientIp("127.1");
        context.setTraceId("testTrace");
        byte[] hint = ExecUtils.buildDRDSTraceCommentBytes(context);
        String sql = "select pk from ? as tb1 where pk in (?)";
        BytesSql bytesSql = BytesSql.getBytesSql(sql);
        List<List<ParameterContext>> params = buildParams();
        List<List<String>> tableNames = buildTableNames();
        JdbcSplit jdbcSplit =
            new JdbcSplit("ca", "sc", "db0", hint, bytesSql, null, params, "127.1", tableNames, ITransaction.RW.WRITE,
                true, null, new byte[] {0x01, 0x02, 0x03}, true);
        String data = JSON.toJSONString(jdbcSplit, SerializerFeature.WriteClassName);
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance().addAccept("com.alibaba.polardbx.executor.mpp.split.JdbcSplit");
        Object obj = JSON.parse(data, parserConfig);
        Assert.assertEquals(jdbcSplit.getClass(), obj.getClass());
        Assert.assertTrue(((JdbcSplit) obj).getSqlTemplate().display().equalsIgnoreCase(sql));
    }

    public static List<List<String>> buildTableNames() {
        List<List<String>> tableNames = Lists.newLinkedList();
        for (int i = 0; i < 10; i++) {
            List<String> row = Lists.newArrayList();
            row.add("test_table_name_" + r.nextInt(5));
            tableNames.add(row);
        }
        return tableNames;
    }

    public static Random r = new Random();

    public static List<List<ParameterContext>> buildParams() {
        List<List<ParameterContext>> rs = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            List<ParameterContext> row = Lists.newArrayList();
            ParameterContext tb = new ParameterContext();
            tb.setParameterMethod(ParameterMethod.setObject1);
            Object[] args = new Object[2];
            args[0] = -1;
            args[1] = "tb";
            tb.setArgs(args);

            row.add(tb);

            List<Object> inVlaue = Lists.newArrayList();
            ParameterContext v = new ParameterContext();
            v.setParameterMethod(ParameterMethod.setObject1);
            Object[] argsV = new Object[2];
            argsV[0] = 2;
            argsV[1] = inVlaue;
            v.setArgs(args);

            for (int j = 0; j < r.nextInt(10); j++) {
                inVlaue.add(j);
            }
            row.add(v);
            rs.add(row);
        }
        return rs;
    }
}
