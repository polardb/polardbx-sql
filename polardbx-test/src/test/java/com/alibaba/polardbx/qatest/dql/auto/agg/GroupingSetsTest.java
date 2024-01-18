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

package com.alibaba.polardbx.qatest.dql.auto.agg;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;


public class GroupingSetsTest extends AutoReadBaseTestCase {

    String tableNameSet;
    String partInfo;

    @Parameterized.Parameters(name = "{index}:table={0},info={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(
            new String[][] {
                {"requests_set", "partition by hash(`id`)"},
                {"single_requests_set", ""}});
    }

    public GroupingSetsTest(String tableNameSet, String partInfo) {
        this.tableNameSet = tableNameSet;
        this.partInfo = partInfo;
    }

    @Before
    public void initData() {
        useDb(tddlConnection, PropertiesUtil.polardbXAutoDBName1());
        String drop_table_sql = String.format("drop table if exists %s", tableNameSet);
        JdbcUtil.updateData(tddlConnection, drop_table_sql, null);
        JdbcUtil.updateData(tddlConnection, "create table if not exists " + tableNameSet + " (\n"
            + "  `id` int(10) UNSIGNED NOT NULL,\n"
            + "  `os` varchar(20) DEFAULT NULL,\n"
            + "  `device` varchar(20) DEFAULT NULL,\n"
            + "  `city` varchar(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 " + partInfo + ";", null);

        String sql = "insert into " + tableNameSet
            + " (id,os,device,city)"
            + " values (?,?,?,?)";

        List<List<Object>> params = Arrays.asList(
            Arrays.asList(1, "windows", "PC", "Beijing"),
            Arrays.asList(2, "windows", "PC", "Shijiazhuang"),
            Arrays.asList(3, "linux", "Phone", "Beijing"),
            Arrays.asList(4, "windows", "PC", "Beijing"),
            Arrays.asList(5, "ios", "Phone", "Shijiazhuang"),
            Arrays.asList(6, "linux", "PC", "Beijing"),
            Arrays.asList(7, "windows", "Phone", "Shijiazhuang")
        );

        JdbcUtil.updateDataBatch(tddlConnection, sql, params);
    }

    @Test
    public void groupSetsTest() {
        String sql = "select os,device, city ,count(*)\n"
            + "from " + tableNameSet + " group by grouping sets((os, device), (city), ());";

        assertCompare(1, sql);

        sql = "select os,device, city ,count(*) from " + tableNameSet + " \n"
            + "group by grouping sets((city), ROLLUP(os, device));";

        assertCompare(2, sql);

        sql = "select os,device, city ,count(*) from " + tableNameSet + " \n"
            + "group by grouping sets((city), CUBE(os, device));";

        assertCompare(3, sql);

        sql = "select os,device, city, count(*)\n"
            + "from " + tableNameSet + " \n"
            + "group by cube (os, device, city);";

        assertCompare(4, sql);

        sql = "select os,device, city, count(*) \n"
            + "from " + tableNameSet + " \n"
            + "group by cube ((os, device), (device, city));";

        assertCompare(5, sql);

        sql = "select os,device, city, count(*)\n"
            + "from " + tableNameSet + " \n"
            + "group by rollup (os, device, city);";

        assertCompare(6, sql);

        sql = "select os,device, city, count(*)\n"
            + "from " + tableNameSet + " \n"
            + "group by rollup (os, (os,device), city);";

        assertCompare(7, sql);

        sql = "select os,device, city, count(*)\n"
            + "from " + tableNameSet + " \n"
            + "group by os, cube(os,device), grouping sets(city);";

        assertCompare(8, sql);

        sql = "select a,b,c,count(*),\n"
            + "grouping(a) ga, grouping(b) gb, grouping(c) gc, grouping_id(a,b,c) groupingid \n"
            + "from (select 1 as a ,2 as b,3 as c)\n"
            + "group by cube(a,b,c);";

        assertCompare(9, sql);
    }

    public void assertCompare(int sql_id, String sql_content) {
        ResultSet rs = null;

        try {
            String filePath = ConfigConstant.RESOURCE_PATH + "agg/" + "q" + sql_id + ".out";
            rs = JdbcUtil.executeQuery(sql_content, tddlConnection);
            List<List<String>> dRDSResult = JdbcUtil.getStringResult(rs, true);
            List<List<String>> standeredAnswer = PropertiesUtil.read(filePath);
            assertWithMessage("非顺序情况下：标准返回结果与tddl 返回结果不一致")
                .that(dRDSResult)
                .containsExactlyElementsIn(standeredAnswer);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            JdbcUtil.close(rs);

        }
    }
}
