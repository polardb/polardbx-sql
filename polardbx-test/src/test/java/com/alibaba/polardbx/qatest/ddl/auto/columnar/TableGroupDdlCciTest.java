/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.truth.Truth;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TableGroupDdlCciTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME1 = "tg_ddl_cci_primary_1";
    private static final String PRIMARY_TABLE_NAME2 = "tg_ddl_cci_primary_2";
    private static final String PRIMARY_TABLE_NAME3 = "tg_ddl_cci_primary_3";
    private static final String INDEX_NAME1 = "tg_ddl_cci_cci_1";
    private static final String INDEX_NAME2 = "tg_ddl_cci_cci_2";
    private static final String INDEX_NAME3 = "tg_ddl_cci_cci_3";
    private static final String INNODB_TG_NAME1 = "tg_ddl_cci_innodb_tg_1";
    private static final String CREATE_TABLE = "CREATE TABLE `%s` ("
        + "`pk` int(11) NOT NULL AUTO_INCREMENT,"
        + "`c1` int(11) DEFAULT NULL,"
        + "`c2` int(11) DEFAULT NULL,"
        + "PRIMARY KEY (`pk`)"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
    private static final String CREATE_TABLE_WITH_CCI = "CREATE TABLE `%s` ("
        + "`pk` int(11) NOT NULL AUTO_INCREMENT,"
        + "`c1` int(11) DEFAULT NULL,"
        + "`c2` int(11) DEFAULT NULL,"
        + "PRIMARY KEY (`pk`),"
        + "CLUSTERED COLUMNAR INDEX `%s` (`c2`)"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
    private static final String CREATE_CCI = "CREATE CLUSTERED COLUMNAR INDEX %s on %s(%s)";
    private static final String CCI_SORT_KEY_C1 = "c1";
    private static final String CREATE_TG = "CREATE TABLEGROUP %s";

    private String innoDbTableName;
    private String tableWithCciName;
    private String cciName;
    private String cciNameWithSuffix;
    private String innodbTgName = "";
    private String columnarTgName = "";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void init() {
        this.innoDbTableName = PRIMARY_TABLE_NAME2;
        this.tableWithCciName = PRIMARY_TABLE_NAME1;
        this.cciName = INDEX_NAME1;
        this.innodbTgName = INNODB_TG_NAME1;

        dropTableIfExists(tableWithCciName);
        dropTableIfExists(innoDbTableName);

        String sql;
        sql = String.format(CREATE_TABLE, innoDbTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(CREATE_TABLE, tableWithCciName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(CREATE_CCI, cciName, tableWithCciName, CCI_SORT_KEY_C1);
        createCciSuccess(sql);

        this.cciNameWithSuffix = getRealCciName(tableWithCciName, cciName);

        this.columnarTgName = queryCciTgName();
        Truth
            .assertWithMessage("No columnar table group found")
            .that(columnarTgName)
            .isNotEmpty();

        sql = String.format(CREATE_TG, innodbTgName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @After
    public void clear() {
        dropTableIfExists(innoDbTableName);
        dropTableIfExists(tableWithCciName);
        dropTableGroupIfExists(innodbTgName);
    }

    @Test
    public void test() {
        testCreateTableGroup();
        testDropTableGroup();
        testMergeTableGroup();
        testAlterTableSetTableGroup();
        testAlterTableGroupAddTable();
        testAlterTableGroupSplit();
        testAlterTableGroupSplitHot();
        testAlterTableGroupMergePartition();
        testAlterTableGroupMovePartition();
        testAlterTableGroupAddPartition();
        testAlterTableGroupDropPartition();
        testAlterTableGroupRenamePartition();
        testAlterTableGroupModifyPartition();
    }

    private void testAlterTableSetTableGroup() {
        /* check columnar table */
        String sql = String.format("alter table %s set tablegroup=%s force", cciNameWithSuffix, innodbTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
        /* check columnar index */
        sql = String.format("alter table %s.%s set tablegroup=%s force", tableWithCciName, cciName, innodbTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
        /* check columnar table group */
        sql = String.format("alter table %s set tablegroup=%s", innoDbTableName, columnarTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testAlterTableGroupAddTable() {
        /* check columnar table */
        String sql = String.format("alter tablegroup %s add tables %s", innodbTgName, cciNameWithSuffix);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
        /* check columnar table group */
        sql = String.format("alter tablegroup %s add tables %s", columnarTgName, innoDbTableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testCreateTableGroup() {
        // prevent creating table group like 'columnar_tg%'
        final String invalidTgName =
            TableGroupNameUtil.autoBuildTableGroupName(27149L, TableGroupRecord.TG_TYPE_COLUMNAR_TBL_TG);
        String sql = String.format("create tablegroup %s", invalidTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testDropTableGroup() {
        String sql = "drop tablegroup " + columnarTgName;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testMergeTableGroup() {
        String sql = String.format("merge tablegroups %s into %s force", columnarTgName, innodbTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testAlterTableGroupSplit() {
        String sql = String.format("alter tablegroup %s split partition p1", columnarTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testAlterTableGroupSplitHot() {
        String sql = String.format("alter tablegroup %s extract to partition ph by hot value(9527)", columnarTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testAlterTableGroupMergePartition() {
        String sql = String.format("alter tablegroup %s merge partitions p2,p4 to p24", columnarTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testAlterTableGroupMovePartition() {
        final List<String> storageInstIds = getStorageInstIds(tddlDatabase1);
        Truth.assertThat(storageInstIds).isNotEmpty();
        final String dstDnId = storageInstIds.get(storageInstIds.size() - 1);
        String sql = String.format("alter tablegroup %s move partitions p2,p4 to '%s'", columnarTgName, dstDnId);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testAlterTableGroupAddPartition() {
        String sql =
            String.format("alter tablegroup %s add partition (partition p9 values less than(100400))", columnarTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testAlterTableGroupDropPartition() {
        String sql =
            String.format("alter tablegroup %s drop partition p3", columnarTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testAlterTableGroupRenamePartition() {
        String sql = String.format("alter tablegroup %s rename partition p1 to np1", columnarTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }

    private void testAlterTableGroupModifyPartition() {
        String sql = String.format("alter tablegroup %s modify partition p2 add values(10001, 10002)", columnarTgName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "involves file storage. not support yet!");
    }
}
