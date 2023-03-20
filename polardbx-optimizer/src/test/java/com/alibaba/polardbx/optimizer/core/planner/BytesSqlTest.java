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

package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.jdbc.StreamBytesSql;
import com.alibaba.polardbx.common.jdbc.UnionBytesSql;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fangwu
 */
public class BytesSqlTest extends TestCase {
    static String[] sqls = {
        "select 1",
        "select * from t1 where id in (1,3,4,5,6)",
        "select * from t2 where name in ('134', '3gr')",
        "select 1 from t3 where name in ('ndf', 123)",
        "select name from t4 where name in (age, 'age')",
        "select name from t4 where name in (age, 'age') and id>100",
        "select name from t4中文 where name in (age, 'age', '1') and id>100",
        "select name from t4_是 where name in (age, 'age', '是') and id>100",
        "select name from t4_是 where name in (age, 'age', '是') and id>10033  and id>10033 and id>10033 and id>10033 and id>10033 and id>10033 and id>10033 and id>10033 and id>10033 and id>10033 and id>10033 ",
        "select name from t4 where (name,pk) in (('a', 1),('age', 324))"
    };

    static String[] sqlsMultiColumn = {
        "select name from t4 where (name,pk) in (('a', 1),('age', 324))",
        "select name from t4 where (name,pk,a) in (('a', 1, 'er'),('age', 324, 'a'), (34,3,34))"
    };

    public void testStreamBytesSQL() {
        // without limit
        String sql = "select * from t1 where id in (1,3,4,5,6)";
        BytesSql bytesSql = BytesSql.getBytesSql(sql);
        byte[] limit = "10, 1002".getBytes();
        BytesSql rs =
            buildStreamBytesSql(bytesSql.getBytesArray(), bytesSql.isParameterLast(), 1, null, null, limit, false);
        String expectSql = "select * from t1 where id in (1,3,4,5,6) LIMIT 10, 1002";
        System.out.println(rs.display());
        System.out.println(expectSql);
        Assert.assertTrue(expectSql.equals(rs.display()));

        rs = buildStreamBytesSql(bytesSql.getBytesArray(), bytesSql.isParameterLast(), 3, null, null, limit, false);

        expectSql =
            " ( select * from t1 where id in (1,3,4,5,6) )  UNION ALL  ( select * from t1 where id in (1,3,4,5,6) )  UNION ALL  ( select * from t1 where id in (1,3,4,5,6) )  LIMIT 10, 1002";
        String check = rs.display();
        System.out.println(expectSql);
        Assert.assertTrue(expectSql.equals(check), check);
        check = new String(rs.getBytes()).replaceAll("\n", " ");
        Assert.assertTrue(expectSql.equals(check), check);
        check = new String(rs.getBytes((List<ParameterContext>) null)).replaceAll("\n", " ");
        Assert.assertTrue(expectSql.equals(check), check);
        check = new String(rs.getBytes(StandardCharsets.UTF_8)).replaceAll("\n", " ");
        Assert.assertTrue(expectSql.equals(check), check);

        // with limit
        sql = "select * from t1 where id in (1,3,4,5,6) limit 100";
        bytesSql = BytesSql.getBytesSql(sql);
        rs = buildStreamBytesSql(bytesSql.getBytesArray(), bytesSql.isParameterLast(), 1, null, null, limit, true);
        expectSql =
            "SELECT * FROM (select * from t1 where id in (1,3,4,5,6) limit 100 )  __DRDS_ALIAS_T_  LIMIT 10, 1002";
        System.out.println(rs.display());
        System.out.println(expectSql);
        Assert.assertTrue(expectSql.equals(rs.display()));
    }

    public void testUnionBytesSQL() {
        // without limit
        String sql = "select * from t1 where id in (1,3,4,5,6)";
        BytesSql bytesSql = BytesSql.getBytesSql(sql);
        byte[] limit = "10, 1002".getBytes();
        BytesSql rs = buildUnionBytesSql(bytesSql.getBytesArray(), bytesSql.isParameterLast(), 1, null, limit);
        String expectSql = "select * from t1 where id in (1,3,4,5,6)";
        System.out.println(rs.display());
        System.out.println(expectSql);
        Assert.assertTrue(expectSql.equals(rs.display()));

        rs = buildUnionBytesSql(bytesSql.getBytesArray(), bytesSql.isParameterLast(), 3, null, limit);

        expectSql =
            " ( select * from t1 where id in (1,3,4,5,6) )  UNION ALL  ( select * from t1 where id in (1,3,4,5,6) )  UNION ALL  ( select * from t1 where id in (1,3,4,5,6) )  LIMIT 10, 1002";
        String check = rs.display();
        System.out.println(expectSql);
        Assert.assertTrue(expectSql.equals(check), check);
        check = new String(rs.getBytes()).replaceAll("\n", " ");
        Assert.assertTrue(expectSql.equals(check), check);
        check = new String(rs.getBytes((List<ParameterContext>) null)).replaceAll("\n", " ");
        Assert.assertTrue(expectSql.equals(check), check);
        check = new String(rs.getBytes(StandardCharsets.UTF_8)).replaceAll("\n", " ");
        Assert.assertTrue(expectSql.equals(check), check);

        // with limit
        sql = "select * from t1 where id in (1,3,4,5,6) limit 100";
        bytesSql = BytesSql.getBytesSql(sql);
        rs = buildUnionBytesSql(bytesSql.getBytesArray(), bytesSql.isParameterLast(), 1, null, limit);
        expectSql = "select * from t1 where id in (1,3,4,5,6) limit 100";
        System.out.println(rs.display());
        System.out.println(expectSql);
        Assert.assertTrue(expectSql.equals(rs.display()));
    }

    private BytesSql buildStreamBytesSql(byte[][] bytesArray, boolean parameterLast, int unionSize,
                                         byte[] order, byte[] limit, byte[] streamLimit, boolean isContainSelect) {

        return new StreamBytesSql(bytesArray, parameterLast, unionSize, order, limit, streamLimit, isContainSelect);
    }

    private BytesSql buildUnionBytesSql(byte[][] bytesArray, boolean parameterLast, int unionSize,
                                        byte[] order, byte[] limit) {

        return new UnionBytesSql(bytesArray, parameterLast, unionSize, order, limit);
    }

    public void testMultiColumn() {
        for (String sql : sqlsMultiColumn) {
            SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql);
            SqlNodeList astList = new FastsqlParser().parse(sqlParameterized.getSql());
            SqlNode ast = astList.get(0);
            BytesSql sqlTemplate = RelUtils.toNativeBytesSql(ast, DbType.MYSQL);

            List<ParameterContext> l = Lists.newArrayList();
            Object[] args1 = new Object[2];
            args1[0] = 1;
            args1[1] = new RawString((List) sqlParameterized.getParameters().get(0));
            l.add(new ParameterContext(ParameterMethod.setTableName, args1));
            System.out.println(new String(sqlTemplate.getBytes(l)));
        }
    }

    /**
     * test bytessql build
     */
    public void testDynamicSize() {
        int[] dynamicSize = {1, 1, 1, 2, 1, 2, 3, 3, 13, 1};
        int count = 0;
        for (String sql : sqls) {
            SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql);
            SqlNodeList astList = new FastsqlParser().parse(sqlParameterized.getSql());
            SqlNode ast = astList.get(0);
            BytesSql sqlTemplate = RelUtils.toNativeBytesSql(ast, DbType.MYSQL);

            System.out.println(sqlParameterized.getSql());
            System.out.println(sqlTemplate.display());
            System.out.println(sqlTemplate.dynamicSize());
            Assert.assertTrue(sqlTemplate.dynamicSize() == dynamicSize[count++]);
        }
    }

    /**
     * test union bytes sql
     */
    public void testUnion() {
        int unionSize = 3;
        for (String sql : sqls) {
            SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql);
            SqlNodeList astList = new FastsqlParser().parse(sqlParameterized.getSql());
            SqlNode ast = astList.get(0);
            BytesSql sqlTemplate = RelUtils.toNativeBytesSql(ast, DbType.MYSQL);

            UnionBytesSql unionBytesSql =
                new UnionBytesSql(sqlTemplate.getBytesArray(), sqlTemplate.isParameterLast(), unionSize, null, null);

            System.out.println(unionBytesSql.display());
            System.out.println(unionBytesSql.dynamicSize());
            Assert.assertTrue(sqlTemplate.dynamicSize() * unionSize == unionBytesSql.dynamicSize());
        }
    }

    /**
     * test the change of charset(gbk, utf8) for chinese
     */
    public void testGetBytes() {
        Charset charset = Charset.forName("GBK");
        for (String sql : sqls) {
            SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql);
            SqlNodeList astList = new FastsqlParser().parse(sqlParameterized.getSql());
            SqlNode ast = astList.get(0);
            BytesSql sqlTemplate = RelUtils.toNativeBytesSql(ast, DbType.MYSQL);

            String u = new String(sqlTemplate.getBytes(), StandardCharsets.UTF_8);
            String g = new String(sqlTemplate.getBytes(charset), charset);
            System.out.println(u);
            System.out.println(g);
            Assert.assertTrue(u.equals(g));
            sqlTemplate.byteString("utf8").toStringUtf8().equals(sqlTemplate.byteString("gbk").toStringUtf8());
        }
    }

    public void testGetBytesByParams() {
        String testSql = "select * from t1 where id in (1,3,4,5,6) and name in ('bob', 'mary')";

        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(testSql);
        SqlNodeList astList = new FastsqlParser().parse(sqlParameterized.getSql());
        SqlNode ast = astList.get(0);
        BytesSql sqlTemplate = RelUtils.toNativeBytesSql(ast, DbType.MYSQL);

        // test contains rawstring
        List<ParameterContext> parameterContexts = Lists.newLinkedList();
        parameterContexts.add(buildParameterWithRawString());
        Assert.assertTrue(sqlTemplate.containRawString(parameterContexts));
        parameterContexts = Lists.newArrayList(parameterContexts);
        Assert.assertTrue(sqlTemplate.containRawString(parameterContexts));

        // test get bytes
        parameterContexts = Lists.newLinkedList();
        parameterContexts.add(buildParameterWithRawString());
        parameterContexts.add(buildParameterWithoutRawString());
        String rs = new String(sqlTemplate.getBytes(parameterContexts));
        System.out.println(rs);
        Assert.assertTrue("SELECT *\nFROM `t1`\nWHERE ((`id` IN (?,?,?)) AND (`name` IN (?)))".equals(rs));

        parameterContexts.clear();
        parameterContexts.add(buildParameterWithRawString());
        parameterContexts.add(buildParameterWithRawString());
        rs = new String(sqlTemplate.getBytes(parameterContexts));
        System.out.println(rs);
        Assert.assertTrue("SELECT *\nFROM `t1`\nWHERE ((`id` IN (?,?,?)) AND (`name` IN (?,?,?)))".equals(rs));
    }

    public void testEmptyPrepareParams() {
        // test get bytes
        List<ParameterContext> parameterContexts = Lists.newLinkedList();
        parameterContexts.add(buildParameterWithRawString());
        parameterContexts.add(buildParameterWithNullIndex());
        parameterContexts.add(buildParameterWithoutRawString());
        parameterContexts.add(buildParameterWithRawString());
        List<ParameterContext> rsList = GeneralUtil.prepareParam(parameterContexts);
        String rs = rsList.stream().map(p -> p.getArgs()[0]).collect(Collectors.toList()).toString();
        System.out.println(rs);
        Assert.assertTrue("[1, 2, 3, 4, 5, 6, 7, 8]".equals(rs));
    }

    public void testPrepareParams() {
        // test get bytes
        List<ParameterContext> parameterContexts = Lists.newLinkedList();
        parameterContexts.add(buildParameterWithRawString());
        parameterContexts.add(buildParameterWithoutRawString());
        List<ParameterContext> rsList = GeneralUtil.prepareParam(parameterContexts);
        String rs = rsList.stream().map(p -> p.getArgs()[0]).collect(Collectors.toList()).toString();
        System.out.println(rs);
        Assert.assertTrue("[1, 2, 3, 4]".equals(rs));

        parameterContexts.clear();
        parameterContexts.add(buildParameterWithRawString());
        parameterContexts.add(buildParameterWithRawString());
        parameterContexts.add(buildParameterWithoutRawString());
        parameterContexts.add(buildParameterWithRawString());
        rsList = GeneralUtil.prepareParam(parameterContexts);
        rs = rsList.stream().map(p -> p.getArgs()[0]).collect(Collectors.toList()).toString();
        System.out.println(rs);
        Assert.assertTrue("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]".equals(rs));

        parameterContexts.clear();
        parameterContexts.add(buildParameterWithoutRawString());
        parameterContexts.add(buildParameterWithoutRawString());
        parameterContexts.add(buildParameterWithoutRawString());
        rsList = GeneralUtil.prepareParam(parameterContexts);
        Assert.assertTrue(rsList == parameterContexts);
    }

    private ParameterContext buildParameterWithRawString() {
        ParameterContext parameterContext = new ParameterContext();
        parameterContext.setParameterMethod(ParameterMethod.setObject1);
        List<Object> l = Lists.newArrayList();
        l.add(1);
        l.add(1);
        l.add(2);
        parameterContext.setArgs(new Object[] {1, new RawString(l)});
        return parameterContext;
    }

    private ParameterContext buildParameterWithoutRawString() {
        ParameterContext parameterContext = new ParameterContext();
        parameterContext.setParameterMethod(ParameterMethod.setObject1);
        parameterContext.setArgs(new Object[] {1, "test"});
        return parameterContext;
    }

    private ParameterContext buildParameterWithNullIndex() {
        ParameterContext parameterContext = new ParameterContext();
        parameterContext.setParameterMethod(ParameterMethod.setObject1);
        parameterContext.setArgs(new Object[] {null, "test"});
        return parameterContext;
    }
}