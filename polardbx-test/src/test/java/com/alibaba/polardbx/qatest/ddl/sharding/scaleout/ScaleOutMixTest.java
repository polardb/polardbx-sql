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

package com.alibaba.polardbx.qatest.ddl.sharding.scaleout;

import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.jcip.annotations.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author luoyanxin
 */
@RunWith(Parameterized.class)
@NotThreadSafe
public class ScaleOutMixTest extends ScaleOutBaseTest {

    private static List<ComplexTaskMetaManager.ComplexTaskStatus> moveTableStatus =
        Stream.of(
            ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC,
            ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC).collect(Collectors.toList());
    static boolean firstIn = true;

    public ScaleOutMixTest(StatusInfo tableStatus) {
        super("ScaleOutMixTest", "polardbx_meta_db_polardbx",
            ImmutableList.of(tableStatus.getMoveTableStatus().toString()));
    }

    @Test
    public void testDdlBetweenScaleout() {
        executeDmlSuccess("delete from test_ddl_between_scaleout1 where 1=1");
    }

    @Before
    public void setUpTables() {
        if (firstIn) {
            setUp(true, getTableDefinitions(), true);
            firstIn = false;
        } else {
            setUp(true, createOneTable(), false);
        }
    }

    @After
    public void tearDown() {
    }

    @Parameterized.Parameters(name = "{index}:TableStatus={0}")
    public static List<StatusInfo[]> prepareDate() {
        List<StatusInfo[]> status = new ArrayList<>();
        moveTableStatus.stream().forEach(c -> {
            status.add(new StatusInfo[] {new StatusInfo(c, null)});
        });
        return status;
    }

    private static List<String> createOneTable() {

        List<String> createTables = new ArrayList<>();

        String tableName = "test_ddl_between_scaleout2";
        String createTable = "create table " + tableName + "(a int, b int, k int null, PRIMARY KEY (`a`)) ";
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(b) tbpartitions 3";
        createTables.add(createTable + partitionDef);
        return createTables;
    }

    protected static List<String> getTableDefinitions() {

        List<String> createTables = new ArrayList<>();

        String tableName = "test_ddl_between_scaleout1";
        String createTable = "create table " + tableName + "(a int, b int, k int null, PRIMARY KEY (`a`)) ";
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(b) tbpartitions 3";
        createTables.add(createTable + partitionDef);

        return createTables;
    }
}