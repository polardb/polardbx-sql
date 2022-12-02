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

package com.alibaba.polardbx.qatest.dql.auto.explain;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * test explain statistics
 *
 * @author Shi Yuxuan
 */
public class ExplainStatisticsTest extends ReadBaseTestCase {
    String tableA = "explain_statistics_a";
    String tableADDL = "CREATE TABLE `explain_statistics_a` (`n_nationkey` int(11) NOT NULL primary key comment 'hhh',"
        + " `n_regionkey` int(11) NOT NULL, n_de int(11),  global index(`n_de`)  PARTITION BY key(`n_de`) )"
        + " PARTITION BY key(`n_regionkey`) PARTITIONS 8 DEFAULT CHARSET = latin1 comment 'ggg';";
    String tableB = "explain_statistics_b";
    String tableBDDL = "CREATE TABLE `explain_statistics_b` (`n_nationkey` int(11) NOT NULL primary key,"
        + " `n_regionkey` int(11) NOT NULL, n_de int(11))"
        + " PARTITION BY key(`n_nationkey`) PARTITIONS 4 DEFAULT CHARSET = latin1;";
    @Before
    public void before() {
        try (final Statement stmt = tddlConnection.createStatement()) {
            String[] tables = {tableA, tableB};
            for (String table : tables) {
                stmt.execute("drop table if exists " + table);
            }
            stmt.execute(tableADDL);
            stmt.execute(String.format("insert into %s(n_nationkey, n_regionkey,n_de) values(1,3,3),(4,2,4);", tableA));
            stmt.execute(tableBDDL);
            stmt.execute(String.format("insert into %s(n_nationkey, n_regionkey,n_de) values(100,300,300),(400,200,400);", tableB));
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @After
    public void after() {
        try (final Statement stmt = tddlConnection.createStatement()) {
            String[] tables = {tableA, tableB};
            for (String table : tables) {
                stmt.execute("drop table if exists "+ table);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
    @Test
    public void explainStatisticsTest() {
        String sql = String.format("explain statistics select * from %s where n_de in (select n_regionkey from %s)",
            tableA, tableB);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        List<List<Object>> rsList = JdbcUtil.getAllResult(rs, false);
        assertWithMessage("failed to get one row " + sql)
            .that(rsList.size())
                .isEqualTo(1);

        Yaml yaml = new Yaml();
        Map<String, Object> totalMaps = yaml.loadAs((String) rsList.get(0).get(0), Map.class);

        assertWithMessage("failed to get sql")
            .that(totalMaps.get("SQL"))
            .isInstanceOf(List.class);

        assertWithMessage("failed to get ddl")
            .that(totalMaps.get("DDL"))
            .isInstanceOf(Map.class);

        assertWithMessage("failed to get config")
            .that(totalMaps.get("CONFIG"))
            .isInstanceOf(Map.class);

        Map<String, String> ddls = (Map<String, String>) totalMaps.get("DDL");
        Assert.assertTrue(StringUtils.containsIgnoreCase(ddls.get(tableA),"CREATE TABLE "));
        Assert.assertTrue(!StringUtils.containsIgnoreCase(ddls.get(tableA),"COMMENT"));
        Assert.assertTrue(ddls.get(tableB).contains("CREATE TABLE "));

        // should contain "analyze table" in suggestion
        String suggestion = (String) rsList.get(0).get(1);
        Assert.assertTrue(StringUtils.containsIgnoreCase(suggestion,"analyze table " + tableA));
        Assert.assertTrue(StringUtils.containsIgnoreCase(suggestion,"analyze table " + tableB));

        // analyze table
        JdbcUtil.executeQuery("analyze table " + tableA, tddlConnection);
        JdbcUtil.executeQuery("analyze table " + tableB, tddlConnection);
        rsList = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection), false);
        assertWithMessage("failed to get one row" + sql)
            .that(rsList.size())
            .isEqualTo(1);

        // should contain statistics after analyze table
        assertWithMessage("failed to get statistics")
            .that(yaml.loadAs((String) rsList.get(0).get(0), Map.class).get("STATISTICS"))
            .isInstanceOf(Map.class);
    }
}