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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Test;

import java.util.List;

/**
 * @author lingce.ldm 2018-05-24 21:32
 */
public class MetaUtilsTest {

    @Test
    public void fetchTableNameFromSqlNode() {
        String sql = "select t.a from t";
        FastsqlParser fp = new FastsqlParser();
        SqlNodeList nodeList = fp.parse(sql);
        SqlSelect node = (SqlSelect) nodeList.get(0);
        List<String> tableNames = MetaUtils.buildTableNamesForSelect(node);
        Assert.assertTrue(tableNames.size() == 1 && tableNames.get(0).equalsIgnoreCase("t"));

        sql = "select t.a from t1 as t";
        nodeList = fp.parse(sql);
        node = (SqlSelect) nodeList.get(0);
        tableNames = MetaUtils.buildTableNamesForSelect(node);

        Assert.assertTrue(tableNames.size() == 1 && tableNames.get(0).equalsIgnoreCase("t1"));

        sql = "select a1.a, a2.b from t1 as a1, t2 as a2";
        nodeList = fp.parse(sql);
        node = (SqlSelect) nodeList.get(0);
        tableNames = MetaUtils.buildTableNamesForSelect(node);
        Assert.assertTrue(tableNames.size() == 2 && tableNames.get(0).equalsIgnoreCase("t1")
            && tableNames.get(1).equalsIgnoreCase("t2"));

        sql = "select a1.a, a2.b, 1+1 from t1 as a1, t2 as a2";
        nodeList = fp.parse(sql);
        node = (SqlSelect) nodeList.get(0);
        tableNames = MetaUtils.buildTableNamesForSelect(node);
        Assert.assertTrue(tableNames.size() == 3 && tableNames.get(0).equalsIgnoreCase("t1")
            && tableNames.get(1).equalsIgnoreCase("t2"));
        Assert.assertTrue(tableNames.get(2).equalsIgnoreCase(""));

        sql = "select a1.a, a2.b, 1+1 from (select t.a, t.b, t.c from t) a1, t2 as a2";
        nodeList = fp.parse(sql);
        node = (SqlSelect) nodeList.get(0);
        tableNames = MetaUtils.buildTableNamesForSelect(node);
        Assert.assertTrue(tableNames.size() == 3 && tableNames.get(0).equalsIgnoreCase("t")
            && tableNames.get(1).equalsIgnoreCase("t2"));
        Assert.assertTrue(tableNames.get(2).equalsIgnoreCase(""));

        sql = "select a1.a, a2.b, 1+1 from (select t.a from (select '1+1' as a from t11) as t) as a1, t2 as a2";
        nodeList = fp.parse(sql);
        node = (SqlSelect) nodeList.get(0);
        tableNames = MetaUtils.buildTableNamesForSelect(node);
        Assert.assertTrue(tableNames.size() == 3 && tableNames.get(0).equalsIgnoreCase("")
            && tableNames.get(1).equalsIgnoreCase("t2"));
        Assert.assertTrue(tableNames.get(2).equalsIgnoreCase(""));
    }
}
