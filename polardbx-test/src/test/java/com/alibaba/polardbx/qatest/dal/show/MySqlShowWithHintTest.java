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

package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.DefaultDBInfo;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.calcite.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.JdbcUtil.getTopology;

/**
 * @author chenmo.cm
 */
public class MySqlShowWithHintTest extends ReadBaseTestCase {

    private static String phyTableName;
    private static String phyDbName;

    public MySqlShowWithHintTest() {

    }

    @Before
    public void before() {
        List<Pair<String, String>> tableTopolgy = getTopology(tddlConnection, "update_delete_base_multi_db_multi_tb");
        if (!usingNewPartDb()) {
            phyTableName = tableTopolgy.get(4).getValue();
        } else {
            phyTableName = tableTopolgy.get(2).getValue();
        }

        phyDbName =
            DefaultDBInfo.getInstance().getShardGroupListByMetaDb(polardbxOneDB, false).getValue().groupAndPhyDbMaps
                .get(MessageFormat.format((PropertiesUtil.polardbXDBName1(usingNewPartDb()) + "_00000{0}_GROUP"), 1)
                    .toUpperCase());
    }

    @Test
    public void showColumns() {

        if (usingNewPartDb()) {
            return;
        }

        String sql = "/*+TDDL:node(1)*/show columns from " + phyTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showColumnsWithDb() {

        if (usingNewPartDb()) {
            return;
        }

        String sql = "/*+TDDL:node(1)*/show columns from " + phyDbName + "." + phyTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);

        sql = "/*+TDDL:node(1)*/show columns from " + phyTableName + " from " + phyDbName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showIndex() {

        if (usingNewPartDb()) {
            return;
        }

        String sql = "/*+TDDL:node(1)*/show index from " + phyTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showIndexWithDb() {

        if (usingNewPartDb()) {
            return;
        }

        String sql = "/*+TDDL:node(1)*/show index from " + phyDbName + "." + phyTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);

        sql = "/*+TDDL:node(1)*/show index from " + phyTableName + " from " + phyDbName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showOpenTables() {

        if (usingNewPartDb()) {
            return;
        }

        String sql = "/*+TDDL:node(1)*/show open tables from " + phyDbName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showTableStatus() {

        if (usingNewPartDb()) {
            return;
        }

        String sql = "/*+TDDL:node(1)*/show table status from " + phyDbName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showTables() {

        if (usingNewPartDb()) {
            return;
        }

        String sql = "/*+TDDL:node(1)*/show tables from " + phyDbName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showTriggers() {

        if (usingNewPartDb()) {
            return;
        }

        String sql = "/*+TDDL:node(1)*/show triggers from " + phyDbName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }
}
