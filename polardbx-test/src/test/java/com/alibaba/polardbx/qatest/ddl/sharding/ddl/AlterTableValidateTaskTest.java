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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

public class AlterTableValidateTaskTest extends AsyncDDLBaseNewDBTestCase {
    @Test
    public void testAlterTableAddColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table mengshi1 add column c1 int");
    }

    @Test
    public void testAlterTableAddDuplicateColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateFailed(tddlConnection, "alter table mengshi1 add column a int", "Duplicate column");
    }

    @Test
    public void testAlterTableAddDuplicateColumn2() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil
            .executeUpdateFailed(tddlConnection, "alter table mengshi1 add column a int, add column a varchar(1024)",
                "Duplicate column");
    }

    @Test
    public void testAlterTableAddColumnAfterColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table mengshi1 add column c1 int after a");
    }

    @Test
    public void testAlterTableAddColumnAfterColumn1() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 add column c1 int after a, add column c2 int after c1");
    }

    @Test
    public void testAlterTableAddColumnAfterUnknownColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil
            .executeUpdateFailed(tddlConnection, "alter table mengshi1 add column c1 int after c", "Unknown column");
    }

    @Test
    public void testAlterTableDropColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table mengshi1 drop column a");
    }

    @Test
    public void testAlterTableDropColumn2() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateFailed(tddlConnection, "alter table mengshi1 add column c int, drop column c",
            "Unknown column");
    }

    @Test
    public void testAlterTableDropColumnAddColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table mengshi1 drop column b, add column b int");
    }

    @Test
    public void testAlterTableDropUnknownColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateFailed(tddlConnection, "alter table mengshi1 drop column c1", "Unknown column");
    }

    @Test
    public void testAlterTableModifyColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table mengshi1 modify column b int");
    }

    @Test
    public void testAlterTableModifyColumnAfter() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table mengshi1 modify column a int after b");
    }

    @Test
    public void testAlterTableModifyColumnAfterAddColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil
            .executeUpdateSuccess(tddlConnection,
                "alter table mengshi1 add column c int, modify column a int after c");
    }

    @Test
    public void testAlterTableModifyColumnAfterUnknownColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil
            .executeUpdateFailed(tddlConnection, "alter table mengshi1 modify column a int after c",
                "Unknown column");
    }

    @Test
    public void testAlterTableUnknownColumnDefaultValue() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil
            .executeUpdateFailed(tddlConnection, "alter table mengshi1 alter column c set default 0",
                "Unknown column");
    }

    @Test
    public void testAlterTableDefaultValue() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table mengshi1 alter column b set default 0");
    }

    @Test
    public void testAlterTableUnknownColumnDefaultValue2() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil
            .executeUpdateFailed(tddlConnection,
                "alter table mengshi1 add column c int, alter column c set default 0",
                "Unknown column");
    }

    @Test
    public void testAlterTableChangeColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table mengshi1 change column b b int");
    }

    @Test
    public void testAlterTableChangeColumnAfter() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table mengshi1 change column a a int after b");
    }

    @Test
    public void testAlterTableChangeColumnAfterAddColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil
            .executeUpdateSuccess(tddlConnection,
                "alter table mengshi1 add column c int, change column a a int after c");
    }

    @Test
    public void testAlterTableAddColumnAfterChangeColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil
            .executeUpdateSuccess(tddlConnection,
                "alter table mengshi1 change column a c int,add column a int");
    }

    @Test
    public void testAlterTableChangeColumnAfterUnknownColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateFailed(tddlConnection, "alter table mengshi1 change column a a int after c",
            "Unknown column");
    }

    @Test
    public void testAlterTableChangeColumnToDuplicateColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil
            .executeUpdateFailed(tddlConnection, "alter table mengshi1 change column a b int", "Duplicate column");
    }

    @Test
    public void testAlterTableAddIndex() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 add index idx1(b)");
    }

    @Test
    public void testAlterTableAddIndexAfterAddColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 add column c int, add index idx1(c)");
    }

    @Test
    public void testAlterTableAddIndexDuplicate() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char, index idx1(a))");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 add index idx1(a)", "Duplicate key");
    }

    @Test
    public void testAlterTableAddIndexDuplicate2() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 add index idx1(a),add index idx1(a)", "Duplicate key");
    }

    @Test
    public void testAlterTableAddDropIndex() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 add index idx1(a), drop index idx1", "Unknown");
    }

    @Test
    public void testAlterTableDropIndex() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char, index idx1(a))");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 drop index idx1");
    }

    @Test
    public void testAlterTableDropAddIndex() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char, index idx1(a))");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 drop index idx1, add index idx1(b)");
    }

    @Test
    public void testAlterTableAddColumnAddIndex() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 add column c int, add index idx1(c)");
    }

    @Test
    public void testAlterTableRenameIndex() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char, index idx1(a))");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 rename index idx1 to idx2");
    }

    @Test
    public void testAlterTableRenameIndexDuplicate() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil
            .executeUpdateSuccess(tddlConnection,
                "create table mengshi1(a int,b char, index idx1(a), index idx2(a))");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 rename index idx1 to idx2", "Duplicate");
    }

    @Test
    public void testAlterTableAddIndexAfterRename() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char, index idx1(a))");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 rename index idx1 to idx2, add index idx1(a)");
    }

    @Test
    public void testAlterTableAddIndexAfterDrop() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char, index idx1(a))");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 drop index idx1, add index idx1(a)");
    }

    @Test
    public void testAlterTableAddPrimaryOld() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateFailed(tddlConnection, "alter table mengshi1 add primary index (a)", "Multiple");
    }

    @Test
    public void testAlterTableDropPrimaryOld() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char, primary key(a))");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 drop primary key");
    }

    @Test
    public void testAlterTableDropPrimaryColumn() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char, primary key(a))");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 drop column a", "not supported");
    }

    @Test
    public void testAlterTableAddIndexWithoutName() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 add index (a)");
    }

    @Test
    public void testAlterTableDropPartitionKey() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char) dbpartition by hash(a)");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 drop column a", "not supported");

        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table mengshi1(a int,b char) dbpartition by hash(a) tbpartition by hash(b) tbpartitions 2");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 drop column b", "not supported");
    }


    @Test
    public void testAlterTableModifyPartitionKey() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char) dbpartition by hash(a)");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 modify column a int", "not supported");
    }

    @Test
    public void testAlterTableDropAllColumns() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int)");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 drop column a", "delete all columns");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 drop column a, add column b int");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table mengshi1 add column c int, drop column b");
    }

    @Test
    public void testAlterTableModifyAfter() {
        dropTableIfExists(tddlConnection, "wumu1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table wumu1(a int,b int, c char, d int) dbpartition by hash(a)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table wumu1 add global index `idx`(a, b) dbpartition by hash(a)");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table wumu1 modify column b int after c");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table wumu1 modify column b char after c", "not recommended");
    }
}
