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

package com.alibaba.polardbx.qatest.failpoint.base;

import com.alibaba.polardbx.qatest.ddl.auto.dag.BaseDdlEngineTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.SQLException;

@FixMethodOrder(value = MethodSorters.JVM)
public class DdlStructureTest extends BaseDdlEngineTestCase {

    protected static final String DDL_STRUCTURE_SCHEMA_NAME = "ddl_structure";

    protected static Connection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + DDL_STRUCTURE_SCHEMA_NAME);
            JdbcUtil.executeUpdateSuccess(tmpConnection, "create database " + DDL_STRUCTURE_SCHEMA_NAME);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + DDL_STRUCTURE_SCHEMA_NAME);
        }
    }

    @Before
    public void beforeBaseFailPointTestCase() {
        useDb(tddlConnection, DDL_STRUCTURE_SCHEMA_NAME);
        this.connection = tddlConnection;
    }

    @Test
    public void testCreateTable1() {
        String graph = getDDLGraph(connection,
            "CREATE TABLE `t1` (\n"
                + "\t`c1` bigint(20) NOT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\tPRIMARY KEY USING BTREE (`c1`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`c1`) tbpartition by hash(c1) tbpartitions 3");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1392984034686009344 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableValidateTask|taskId:1392984034686009344|onException:ROLLBACK|state:SUCCESS|execute cost:53ms|logicalTableName: t1}\"];\n"
                + "1392984034811838464 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTablePhyDdlTask|taskId:1392984034811838464|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:1002ms}\"];\n"
                + "1392984035050913792 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesMetaTask|taskId:1392984035050913792|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:74ms}\"];\n"
                + "1392984035130605568 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableShowTableMetaTask|taskId:1392984035130605568|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:198ms}\"];\n"
                + "1392984035097051136 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CdcDdlMarkTask|taskId:1392984035097051136|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:1454ms}\"];\n"
                + "1392984034769895424 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesExtMetaTask|taskId:1392984034769895424|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:43ms|table=t1}\"];\n"
                + "1392984035168354304 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1392984035168354304|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:25ms|tableName: t1}\"];\n"
                + "1392984035050913792 -> 1392984035097051136\n"
                + "1392984034769895424 -> 1392984034811838464\n"
                + "1392984034686009344 -> 1392984034769895424\n"
                + "1392984035097051136 -> 1392984035130605568\n"
                + "1392984034811838464 -> 1392984035050913792\n"
                + "1392984035130605568 -> 1392984035168354304\n"
                + "}"
        );
    }

    @Test
    public void testAlterTable() {
        String graph = getDDLGraph(connection,
            "alter table t1 add column c4 bigint");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006741469265922 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{AlterTablePhyDdlTask|taskId:1393006741469265922|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:1622ms}\"];\n"
                + "1393006741469265921 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{AlterTableValidateTask|taskId:1393006741469265921|onException:ROLLBACK|state:SUCCESS|execute cost:29ms|logicalTableName: t1}\"];\n"
                + "1393006741481848832 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006741481848832|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:22ms|tableName: t1}\"];\n"
                + "1393006741477654529 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{AlterTableChangeMetaTask|taskId:1393006741477654529|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:214ms|add columns [c4] on table t1}\"];\n"
                + "1393006741477654528 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CdcDdlMarkTask|taskId:1393006741477654528|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:152ms}\"];\n"
                + "1393006741477654529 -> 1393006741481848832\n"
                + "1393006741469265922 -> 1393006741477654528\n"
                + "1393006741477654528 -> 1393006741477654529\n"
                + "1393006741469265921 -> 1393006741469265922\n"
                + "}"
        );
    }

    @Test
    public void testRenameTable() {
        String graph = getDDLGraph(connection,
            "rename table t1 to t2");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006752156352516 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CdcDdlMarkTask|taskId:1393006752156352516|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:254ms}\"];\n"
                + "1393006752156352517 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{RenameTableUpdateMetaTask|taskId:1393006752156352517|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:166ms}\"];\n"
                + "1393006752156352515 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{RenameTablePhyDdlTask|taskId:1393006752156352515|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:1081ms}\"];\n"
                + "1393006752156352513 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{RenameTableValidateTask|taskId:1393006752156352513|onException:ROLLBACK|state:SUCCESS|execute cost:31ms|from t1 to t2}\"];\n"
                + "1393006752156352518 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{RenameTableSyncTask|taskId:1393006752156352518|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:28ms}\"];\n"
                + "1393006752156352514 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{RenameTableAddMetaTask|taskId:1393006752156352514|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:185ms}\"];\n"
                + "1393006752156352514 -> 1393006752156352515\n"
                + "1393006752156352513 -> 1393006752156352514\n"
                + "1393006752156352516 -> 1393006752156352517\n"
                + "1393006752156352517 -> 1393006752156352518\n"
                + "1393006752156352515 -> 1393006752156352516\n"
                + "}"
        );

        getDDLGraph(connection,
            "rename table t2 to t1");
    }

    @Test
    public void testTruncateTable() {
        String graph = getDDLGraph(connection,
            "truncate table t1");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006770124750848 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TruncateTableValidateTask|taskId:1393006770124750848|onException:ROLLBACK|state:SUCCESS|execute cost:28ms|logicalTableName: t1}\"];\n"
                + "1393006770124750849 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TruncateTablePhyDdlTask|taskId:1393006770124750849|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:959ms}\"];\n"
                + "1393006770124750850 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CdcDdlMarkTask|taskId:1393006770124750850|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:117ms}\"];\n"
                + "1393006770124750848 -> 1393006770124750849\n"
                + "1393006770124750849 -> 1393006770124750850\n"
                + "}"
        );
    }

    @Test
    public void testCreateIndex() {
        String graph = getDDLGraph(connection,
            "create index idx1 on t1(c4)");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006776516870144 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateIndexAddMetaTask|taskId:1393006776516870144|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:85ms}\"];\n"
                + "1393006776525258752 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CdcDdlMarkTask|taskId:1393006776525258752|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:195ms}\"];\n"
                + "1393006776512675840 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateIndexValidateTask|taskId:1393006776512675840|onException:ROLLBACK|state:SUCCESS|execute cost:51ms|logicalTableName: t1}\"];\n"
                + "1393006776525258754 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006776525258754|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:18ms|tableName: t1}\"];\n"
                + "1393006776525258753 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateIndexShowMetaTask|taskId:1393006776525258753|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:84ms}\"];\n"
                + "1393006776521064448 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateIndexPhyDdlTask|taskId:1393006776521064448|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:760ms}\"];\n"
                + "1393006776525258753 -> 1393006776525258754\n"
                + "1393006776521064448 -> 1393006776525258752\n"
                + "1393006776512675840 -> 1393006776516870144\n"
                + "1393006776525258752 -> 1393006776525258753\n"
                + "1393006776516870144 -> 1393006776521064448\n"
                + "}"
        );
    }

    @Test
    public void testDropIndex() {
        String graph = getDDLGraph(connection,
            "drop index idx1 on t1");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006783802376196 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CdcDdlMarkTask|taskId:1393006783802376196|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:247ms}\"];\n"
                + "1393006783802376194 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006783802376194|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:11ms|tableName: t1}\"];\n"
                + "1393006783802376195 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropIndexPhyDdlTask|taskId:1393006783802376195|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:649ms}\"];\n"
                + "1393006783802376193 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropIndexRemoveMetaTask|taskId:1393006783802376193|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:70ms}\"];\n"
                + "1393006783802376192 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropIndexValidateTask|taskId:1393006783802376192|onException:ROLLBACK|state:SUCCESS|execute cost:13ms|logicalTableName: t1}\"];\n"
                + "1393006783802376192 -> 1393006783802376193\n"
                + "1393006783802376194 -> 1393006783802376195\n"
                + "1393006783802376195 -> 1393006783802376196\n"
                + "1393006783802376193 -> 1393006783802376194\n"
                + "}"
        );
    }

    @Test
    public void testAddGsi() {
        String graph = getDDLGraph(connection,
            "alter table t1 add global INDEX `gsi1a`(`c2`) DBPARTITION BY HASH(`c2`) tbpartition by hash(c2) tbpartitions 3");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006789968003081 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006789968003081|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:102ms|GSI(gsi1a) DELETE_ONLY to WRITE_ONLY}\"];\n"
                + "1393006789972197376 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006789972197376|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:116ms|GSI(gsi1a) WRITE_ONLY to WRITE_REORG}\"];\n"
                + "1393006789972197379 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006789972197379|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:41ms|tableName: t1}\"];\n"
                + "1393006789972197378 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006789972197378|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:160ms|GSI(gsi1a) WRITE_REORG to PUBLIC}\"];\n"
                + "1393006789968003083 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{LogicalTableBackFillTask|taskId:1393006789968003083|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:942ms}\"];\n"
                + "1393006789968003076 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesMetaTask|taskId:1393006789968003076|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:45ms}\"];\n"
                + "1393006789968003082 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006789968003082|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:15ms|tableName: t1}\"];\n"
                + "1393006789968003073 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateGsiValidateTask|taskId:1393006789968003073|onException:ROLLBACK|state:SUCCESS|execute cost:74ms|primaryTableName: t1}\"];\n"
                + "1393006789968003080 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006789968003080|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:32ms|tableName: t1}\"];\n"
                + "1393006789968003074 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesExtMetaTask|taskId:1393006789968003074|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:45ms|table=gsi1a}\"];\n"
                + "1393006789968003078 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiInsertIndexMetaTask|taskId:1393006789968003078|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:326ms}\"];\n"
                + "1393006789972197377 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006789972197377|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:22ms|tableName: t1}\"];\n"
                + "1393006789968003079 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006789968003079|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:227ms|GSI(gsi1a) CREATING to DELETE_ONLY}\"];\n"
                + "1393006789968003075 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateGsiPhyDdlTask|taskId:1393006789968003075|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:1206ms}\"];\n"
                + "1393006789968003077 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableShowTableMetaTask|taskId:1393006789968003077|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:255ms}\"];\n"
                + "1393006789968003079 -> 1393006789968003080\n"
                + "1393006789972197378 -> 1393006789972197379\n"
                + "1393006789968003077 -> 1393006789968003078\n"
                + "1393006789968003075 -> 1393006789968003076\n"
                + "1393006789968003083 -> 1393006789972197376\n"
                + "1393006789968003081 -> 1393006789968003082\n"
                + "1393006789972197376 -> 1393006789972197377\n"
                + "1393006789968003082 -> 1393006789968003083\n"
                + "1393006789968003078 -> 1393006789968003079\n"
                + "1393006789968003074 -> 1393006789968003075\n"
                + "1393006789968003076 -> 1393006789968003077\n"
                + "1393006789968003073 -> 1393006789968003074\n"
                + "1393006789968003080 -> 1393006789968003081\n"
                + "1393006789972197377 -> 1393006789972197378\n"
                + "}"
        );
    }

    @Test
    public void testDropGsi() {
        String graph = getDDLGraph(connection,
            "drop index gsi1a on t1");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006808058036228 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006808058036228|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:60ms|tableName: t1}\"];\n"
                + "1393006808058036232 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006808058036232|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:33ms|tableName: t1}\"];\n"
                + "1393006808058036229 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006808058036229|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:205ms|GSI(gsi1a) DELETE_ONLY to ABSENT}\"];\n"
                + "1393006808058036235 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006808058036235|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:20ms|tableName: gsi1a}\"];\n"
                + "1393006808058036230 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006808058036230|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:18ms|tableName: t1}\"];\n"
                + "1393006808058036226 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006808058036226|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:10ms|tableName: t1}\"];\n"
                + "1393006808058036225 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006808058036225|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:139ms|GSI(gsi1a) PUBLIC to WRITE_ONLY}\"];\n"
                + "1393006808058036233 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropGsiPhyDdlTask|taskId:1393006808058036233|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:739ms}\"];\n"
                + "1393006808058036224 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{ValidateGsiExistenceTask|taskId:1393006808058036224|onException:ROLLBACK|state:SUCCESS|execute cost:10ms|indexName: gsi1a}\"];\n"
                + "1393006808058036227 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006808058036227|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:142ms|GSI(gsi1a) WRITE_ONLY to DELETE_ONLY}\"];\n"
                + "1393006808058036231 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiDropCleanUpTask|taskId:1393006808058036231|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:97ms}\"];\n"
                + "1393006808058036234 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropGsiTableRemoveMetaTask|taskId:1393006808058036234|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:127ms}\"];\n"
                + "1393006808058036230 -> 1393006808058036231\n"
                + "1393006808058036225 -> 1393006808058036226\n"
                + "1393006808058036233 -> 1393006808058036234\n"
                + "1393006808058036226 -> 1393006808058036227\n"
                + "1393006808058036229 -> 1393006808058036230\n"
                + "1393006808058036224 -> 1393006808058036225\n"
                + "1393006808058036232 -> 1393006808058036233\n"
                + "1393006808058036227 -> 1393006808058036228\n"
                + "1393006808058036231 -> 1393006808058036232\n"
                + "1393006808058036234 -> 1393006808058036235\n"
                + "1393006808058036228 -> 1393006808058036229\n"
                + "}"
        );
    }

    @Test
    public void testRepartition() {
        String graph = getDDLGraph(connection,
            "ALTER TABLE t1 broadcast");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006817436499976 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006817436499976|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:147ms|GSI(t1_rhsy) DELETE_ONLY to WRITE_ONLY}\"];\n"
                + "1393006817436499968 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateGsiValidateTask|taskId:1393006817436499968|onException:ROLLBACK|state:SUCCESS|execute cost:76ms|primaryTableName: t1}\"];\n"
                + "1393006817436499970 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateGsiPhyDdlTask|taskId:1393006817436499970|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:465ms}\"];\n"
                + "1393006817436499975 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006817436499975|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:18ms|tableName: t1}\"];\n"
                + "1393006817440694274 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006817440694274|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:137ms|GSI(t1_rhsy) PUBLIC to WRITE_ONLY}\"];\n"
                + "1393006817436499978 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{LogicalTableBackFillTask|taskId:1393006817436499978|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:794ms}\"];\n"
                + "1393006817436499977 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006817436499977|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:20ms|tableName: t1}\"];\n"
                + "1393006817440694283 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropGsiTableRemoveMetaTask|taskId:1393006817440694283|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:133ms}\"];\n"
                + "1393006817440694278 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006817440694278|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:184ms|GSI(t1_rhsy) DELETE_ONLY to ABSENT}\"];\n"
                + "1393006817440694275 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006817440694275|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:23ms|tableName: t1}\"];\n"
                + "1393006817440694272 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CdcRepartitionMarkTask|taskId:1393006817440694272|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:211ms}\"];\n"
                + "1393006817440694282 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropGsiPhyDdlTask|taskId:1393006817440694282|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:908ms}\"];\n"
                + "1393006817436499973 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiInsertIndexMetaTask|taskId:1393006817436499973|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:108ms}\"];\n"
                + "1393006817440694280 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiDropCleanUpTask|taskId:1393006817440694280|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:106ms}\"];\n"
                + "1393006817436499974 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006817436499974|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:138ms|GSI(t1_rhsy) CREATING to DELETE_ONLY}\"];\n"
                + "1393006817440694281 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006817440694281|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:15ms|tableName: t1}\"];\n"
                + "1393006817436499972 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableShowTableMetaTask|taskId:1393006817436499972|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:178ms}\"];\n"
                + "1393006817440694277 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006817440694277|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:61ms|tableName: t1}\"];\n"
                + "1393006817440694273 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{ValidateGsiExistenceTask|taskId:1393006817440694273|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:15ms|indexName: t1_rhsy}\"];\n"
                + "1393006817440694279 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006817440694279|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:39ms|tableName: t1}\"];\n"
                + "1393006817436499971 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesMetaTask|taskId:1393006817436499971|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:100ms}\"];\n"
                + "1393006817440694276 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006817440694276|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:109ms|GSI(t1_rhsy) WRITE_ONLY to DELETE_ONLY}\"];\n"
                + "1393006817436499969 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesExtMetaTask|taskId:1393006817436499969|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:46ms|table=t1_rhsy}\"];\n"
                + "1393006817436499979 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{RepartitionCutOverTask|taskId:1393006817436499979|onException:ROLLBACK|state:SUCCESS|execute cost:41ms}\"];\n"
                + "1393006817440694284 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006817440694284|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:24ms|tableName: t1_rhsy}\"];\n"
                + "1393006817436499980 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{RepartitionSyncTask|taskId:1393006817436499980|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:70ms}\"];\n"
                + "1393006817436499974 -> 1393006817436499975\n"
                + "1393006817440694276 -> 1393006817440694277\n"
                + "1393006817436499977 -> 1393006817436499978\n"
                + "1393006817436499969 -> 1393006817436499970\n"
                + "1393006817440694281 -> 1393006817440694282\n"
                + "1393006817436499975 -> 1393006817436499976\n"
                + "1393006817440694282 -> 1393006817440694283\n"
                + "1393006817440694278 -> 1393006817440694279\n"
                + "1393006817440694280 -> 1393006817440694281\n"
                + "1393006817440694273 -> 1393006817440694274\n"
                + "1393006817436499978 -> 1393006817436499979\n"
                + "1393006817436499972 -> 1393006817436499973\n"
                + "1393006817436499973 -> 1393006817436499974\n"
                + "1393006817436499971 -> 1393006817436499972\n"
                + "1393006817436499976 -> 1393006817436499977\n"
                + "1393006817440694272 -> 1393006817440694273\n"
                + "1393006817440694277 -> 1393006817440694278\n"
                + "1393006817440694274 -> 1393006817440694275\n"
                + "1393006817440694275 -> 1393006817440694276\n"
                + "1393006817436499979 -> 1393006817436499980\n"
                + "1393006817440694283 -> 1393006817440694284\n"
                + "1393006817440694279 -> 1393006817440694280\n"
                + "1393006817436499968 -> 1393006817436499969\n"
                + "1393006817436499970 -> 1393006817436499971\n"
                + "1393006817436499980 -> 1393006817440694272\n"
                + "}"
        );
    }

    @Test
    public void testDropTable() {
        String graph = getDDLGraph(connection,
            "drop table t1");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006839376904193 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropTableValidateTask|taskId:1393006839376904193|onException:ROLLBACK|state:SUCCESS|execute cost:42ms|logicalTableName: t1}\"];\n"
                + "1393006839381098498 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropTablePhyDdlTask|taskId:1393006839381098498|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:377ms}\"];\n"
                + "1393006839381098496 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropTableRemoveMetaTask|taskId:1393006839381098496|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:107ms}\"];\n"
                + "1393006839376904194 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{StoreTableLocalityTask|taskId:1393006839376904194|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:68ms}\"];\n"
                + "1393006839381098497 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006839381098497|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:29ms|tableName: t1}\"];\n"
                + "1393006839381098499 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CdcDdlMarkTask|taskId:1393006839381098499|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:261ms}\"];\n"
                + "1393006839381098497 -> 1393006839381098498\n"
                + "1393006839376904193 -> 1393006839376904194\n"
                + "1393006839381098496 -> 1393006839381098497\n"
                + "1393006839376904194 -> 1393006839381098496\n"
                + "1393006839381098498 -> 1393006839381098499\n"
                + "}"
        );
    }

    @Test
    public void testCreateTableWithGsi() {
        String graph = getDDLGraph(connection,
            "CREATE TABLE `t1` (\n"
                + "\t`c1` bigint(20) NOT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\tPRIMARY KEY USING BTREE (`c1`),\n"
                + "\tglobal INDEX `gsi1a`(`c2`) DBPARTITION BY HASH(`c2`) tbpartition by hash(c2) tbpartitions 3,\n"
                + "\tglobal INDEX `gsi2a`(`c2`) DBPARTITION BY HASH(`c2`) tbpartition by hash(c2) tbpartitions 3,\n"
                + "\tglobal INDEX `gsi3a`(`c2`) DBPARTITION BY HASH(`c2`) tbpartition by hash(c2) tbpartitions 3\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`c1`) tbpartition by hash(c1) tbpartitions 3");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006845651582978 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateGsiValidateTask|taskId:1393006845651582978|onException:ROLLBACK|state:SUCCESS|execute cost:38ms|primaryTableName: t1}\"];\n"
                + "1393006845659971585 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesExtMetaTask|taskId:1393006845659971585|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:18ms|table=gsi3a}\"];\n"
                + "1393006845659971599 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006845659971599|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:22ms|GSI(gsi2a) CREATING to PUBLIC}\"];\n"
                + "1393006845651582977 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesExtMetaTask|taskId:1393006845651582977|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:35ms|table=t1}\"];\n"
                + "1393006845659971589 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateGsiPhyDdlTask|taskId:1393006845659971589|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:594ms}\"];\n"
                + "1393006845659971590 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesMetaTask|taskId:1393006845659971590|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:81ms}\"];\n"
                + "1393006845659971596 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiInsertIndexMetaTask|taskId:1393006845659971596|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:66ms}\"];\n"
                + "1393006845659971586 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesExtMetaTask|taskId:1393006845659971586|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:26ms|table=gsi1a}\"];\n"
                + "1393006845659971594 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableShowTableMetaTask|taskId:1393006845659971594|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:189ms}\"];\n"
                + "1393006845659971592 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesMetaTask|taskId:1393006845659971592|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:117ms}\"];\n"
                + "1393006845659971587 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateGsiPhyDdlTask|taskId:1393006845659971587|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:926ms}\"];\n"
                + "1393006845651582979 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateGsiValidateTask|taskId:1393006845651582979|onException:ROLLBACK|state:SUCCESS|execute cost:40ms|primaryTableName: t1}\"];\n"
                + "1393006845655777281 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CdcDdlMarkTask|taskId:1393006845655777281|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:154ms}\"];\n"
                + "1393006845655777280 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesMetaTask|taskId:1393006845655777280|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:71ms}\"];\n"
                + "1393006845659971600 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006845659971600|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:25ms|GSI(gsi3a) CREATING to PUBLIC}\"];\n"
                + "1393006845659971598 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiInsertIndexMetaTask|taskId:1393006845659971598|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:52ms}\"];\n"
                + "1393006845651582976 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableValidateTask|taskId:1393006845651582976|onException:ROLLBACK|state:SUCCESS|execute cost:62ms|logicalTableName: t1}\"];\n"
                + "1393006845659971584 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesExtMetaTask|taskId:1393006845659971584|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:23ms|table=gsi2a}\"];\n"
                + "1393006845659971597 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiInsertIndexMetaTask|taskId:1393006845659971597|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:106ms}\"];\n"
                + "1393006845659971595 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableShowTableMetaTask|taskId:1393006845659971595|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:260ms}\"];\n"
                + "1393006845659971593 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableShowTableMetaTask|taskId:1393006845659971593|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:182ms}\"];\n"
                + "1393006845659971588 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateGsiPhyDdlTask|taskId:1393006845659971588|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:572ms}\"];\n"
                + "1393006845651582980 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateGsiValidateTask|taskId:1393006845651582980|onException:ROLLBACK|state:SUCCESS|execute cost:35ms|primaryTableName: t1}\"];\n"
                + "1393006845659971591 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableAddTablesMetaTask|taskId:1393006845659971591|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:67ms}\"];\n"
                + "1393006845659971601 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{GsiUpdateIndexStatusTask|taskId:1393006845659971601|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:37ms|GSI(gsi1a) CREATING to PUBLIC}\"];\n"
                + "1393006845659971602 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTableShowTableMetaTask|taskId:1393006845659971602|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:297ms}\"];\n"
                + "1393006845651582981 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CreateTablePhyDdlTask|taskId:1393006845651582981|onException:TRY_RECOVERY_THEN_ROLLBACK|state:SUCCESS|execute cost:704ms}\"];\n"
                + "1393006845659971603 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006845659971603|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:14ms|tableName: t1}\"];\n"
                + "1393006845659971587 -> 1393006845659971590\n"
                + "1393006845651582977 -> 1393006845651582980\n"
                + "1393006845659971585 -> 1393006845659971588\n"
                + "1393006845651582980 -> 1393006845651582981\n"
                + "1393006845659971593 -> 1393006845659971596\n"
                + "1393006845659971589 -> 1393006845659971592\n"
                + "1393006845659971599 -> 1393006845659971602\n"
                + "1393006845651582977 -> 1393006845651582979\n"
                + "1393006845659971597 -> 1393006845659971600\n"
                + "1393006845659971594 -> 1393006845659971597\n"
                + "1393006845655777281 -> 1393006845659971584\n"
                + "1393006845651582976 -> 1393006845651582977\n"
                + "1393006845659971595 -> 1393006845659971598\n"
                + "1393006845659971601 -> 1393006845659971602\n"
                + "1393006845659971590 -> 1393006845659971593\n"
                + "1393006845659971591 -> 1393006845659971594\n"
                + "1393006845655777281 -> 1393006845659971585\n"
                + "1393006845651582979 -> 1393006845651582981\n"
                + "1393006845659971592 -> 1393006845659971595\n"
                + "1393006845659971588 -> 1393006845659971591\n"
                + "1393006845651582978 -> 1393006845651582981\n"
                + "1393006845659971596 -> 1393006845659971599\n"
                + "1393006845655777281 -> 1393006845659971586\n"
                + "1393006845651582977 -> 1393006845651582978\n"
                + "1393006845659971602 -> 1393006845659971603\n"
                + "1393006845655777280 -> 1393006845655777281\n"
                + "1393006845659971600 -> 1393006845659971602\n"
                + "1393006845659971584 -> 1393006845659971587\n"
                + "1393006845651582981 -> 1393006845655777280\n"
                + "1393006845659971586 -> 1393006845659971589\n"
                + "1393006845659971598 -> 1393006845659971601\n"
                + "}"
        );
    }

    @Test
    public void testDropTableWithGsi() {
        String graph = getDDLGraph(connection,
            "drop table t1");

        assertGraphsHaveSameStructure(
            graph,
            "digraph G {\n"
                + "1393006868795752459 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropGsiPhyDdlTask|taskId:1393006868795752459|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:495ms}\"];\n"
                + "1393006868795752450 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{ValidateGsiExistenceTask|taskId:1393006868795752450|onException:ROLLBACK|state:SUCCESS|execute cost:15ms|indexName: gsi3a}\"];\n"
                + "1393006868795752454 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006868795752454|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:21ms|tableName: t1}\"];\n"
                + "1393006868795752463 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006868795752463|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:24ms|tableName: gsi3a}\"];\n"
                + "1393006868795752448 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{ValidateGsiExistenceTask|taskId:1393006868795752448|onException:ROLLBACK|state:SUCCESS|execute cost:13ms|indexName: gsi2a}\"];\n"
                + "1393006868795752452 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{StoreTableLocalityTask|taskId:1393006868795752452|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:148ms}\"];\n"
                + "1393006868795752457 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropGsiPhyDdlTask|taskId:1393006868795752457|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:454ms}\"];\n"
                + "1393006868795752464 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006868795752464|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:19ms|tableName: gsi2a}\"];\n"
                + "1393006868795752460 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropGsiTableRemoveMetaTask|taskId:1393006868795752460|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:119ms}\"];\n"
                + "1393006868795752451 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{ValidateGsiExistenceTask|taskId:1393006868795752451|onException:ROLLBACK|state:SUCCESS|execute cost:8ms|indexName: gsi1a}\"];\n"
                + "1393006868795752455 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropTablePhyDdlTask|taskId:1393006868795752455|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:425ms}\"];\n"
                + "1393006868795752462 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropGsiTableRemoveMetaTask|taskId:1393006868795752462|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:91ms}\"];\n"
                + "1393006868795752465 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{TableSyncTask|taskId:1393006868795752465|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:26ms|tableName: gsi1a}\"];\n"
                + "1393006868795752449 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropTableValidateTask|taskId:1393006868795752449|onException:ROLLBACK|state:SUCCESS|execute cost:13ms|logicalTableName: t1}\"];\n"
                + "1393006868795752456 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{CdcDdlMarkTask|taskId:1393006868795752456|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:117ms}\"];\n"
                + "1393006868795752458 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropGsiPhyDdlTask|taskId:1393006868795752458|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:445ms}\"];\n"
                + "1393006868795752461 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropGsiTableRemoveMetaTask|taskId:1393006868795752461|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:96ms}\"];\n"
                + "1393006868795752453 [shape=record  fillcolor=\"#90ee90\" style=filled label=\"{DropTableRemoveMetaTask|taskId:1393006868795752453|onException:TRY_RECOVERY_THEN_PAUSE|state:SUCCESS|execute cost:127ms}\"];\n"
                + "1393006868795752453 -> 1393006868795752454\n"
                + "1393006868795752457 -> 1393006868795752460\n"
                + "1393006868795752460 -> 1393006868795752463\n"
                + "1393006868795752454 -> 1393006868795752455\n"
                + "1393006868795752458 -> 1393006868795752461\n"
                + "1393006868795752461 -> 1393006868795752464\n"
                + "1393006868795752450 -> 1393006868795752453\n"
                + "1393006868795752456 -> 1393006868795752457\n"
                + "1393006868795752459 -> 1393006868795752462\n"
                + "1393006868795752455 -> 1393006868795752456\n"
                + "1393006868795752451 -> 1393006868795752453\n"
                + "1393006868795752456 -> 1393006868795752459\n"
                + "1393006868795752449 -> 1393006868795752452\n"
                + "1393006868795752452 -> 1393006868795752453\n"
                + "1393006868795752462 -> 1393006868795752465\n"
                + "1393006868795752448 -> 1393006868795752453\n"
                + "1393006868795752456 -> 1393006868795752458\n"
                + "}"
        );
    }
}