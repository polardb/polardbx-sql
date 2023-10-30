package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;

@NotThreadSafe
public class AlterTableCompatibilityTest extends AlterTableCompatBaseTest {

    @Test
    public void testChangeColumnsAndRenameIndexes() {
        String tableName = "test_change_columns_and_rename_indexes";

        String createTable = "create table %s (\n"
            + "    id int not null auto_increment,\n"
            + "    resources_id1 int not null default '0',\n"
            + "    resources_id varchar(30) not null default '',\n"
            + "    primary key (id),\n"
            + "    key idx_rid1 (resources_id1),\n"
            + "    key idx_rid (resources_id)\n"
            + ") broadcast";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s\n"
            + "    change column resources_id extends1 varchar(30) not null default '' ,\n"
            + "    change column resources_id1 resources_id int not null default 0";
        executeAndCheck(alterTable, tableName);

        alterTable = "alter table %s\n"
            + "    rename index idx_rid to idx_ext,\n"
            + "    rename index idx_rid1 to idx_rid";
        executeAndCheck(alterTable, tableName);

        dropTableIfExists(tableName);
        executeAndCheck(createTable, tableName);

        alterTable = "alter table %s\n"
            + "    change column resources_id extends1 varchar(30) not null default '' ,\n"
            + "    change column resources_id1 resources_id int not null default 0,\n"
            + "    rename index idx_rid to idx_ext,\n"
            + "    rename index idx_rid1 to idx_rid";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testAddDuplicateColumn() {
        String tableName = "test_add_duplicate_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add column a int";
        executeAndFail(alterTable, tableName, "Duplicate column");
    }

    @Test
    public void testAddDuplicateColumn2() {
        String tableName = "test_add_duplicate_column_2";

        String createTable = "create table %s (a int)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add column b int, add column b varchar(100)";
        executeAndFail(alterTable, tableName, "Duplicate column");
    }

    @Test
    public void testDropUnknownColumn() {
        String tableName = "test_drop_unknown_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s drop column c";
        executeAndFail(alterTable, tableName, "Unknown column");
    }

    @Test
    public void testDropUnknownColumn2() {
        String tableName = "test_drop_unknown_column_2";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add column c int, drop column c";
        executeAndFail(alterTable, tableName, "Unknown column");
    }

    @Test
    public void testDropAndAddColumn() {
        String tableName = "test_drop_and_add_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s drop column b, add column b bigint";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testAddAndModifyColumn() {
        String tableName = "test_add_and_modify_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add column c bigint, modify column a int after c";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testModifyAfterUnknownColumn() {
        String tableName = "test_modify_after_unknown_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s modify column a int after c";
        executeAndFail(alterTable, tableName, "Unknown column");
    }

    @Test
    public void testSetUnknownColumn() {
        String tableName = "test_set_unknown_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s alter column c set default 0";
        executeAndFail(alterTable, tableName, "Unknown column");
    }

    @Test
    public void testAddAndSetUnknownColumn() {
        String tableName = "test_add_and_set_unknown_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add column c int, alter column c set default 0";
        executeAndFail(alterTable, tableName, "Unknown column");
    }

    @Test
    public void testChangeColumn() {
        String tableName = "test_change_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s change column b b int";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testChangeColumn2() {
        String tableName = "test_change_column_2";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s change column a b int, change column b c int";
        executeAndFail(alterTable, tableName, "Duplicate column");
        // TODO: the usage should be OK, so we will have to change AlterTableValidateTask in the near future.
        //executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testChangeColumnAfter() {
        String tableName = "test_change_column_after";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s change column a a int after b";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testAddAndChangeColumn() {
        String tableName = "test_add_and_change_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add column c int, change column a a int after c";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testChangeAndAddColumn() {
        String tableName = "test_change_and_add_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s change column a c int, add column a int";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testChangeAfterUnknownColumn() {
        String tableName = "test_change_after_unknown_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s change column a a int after c";
        executeAndFail(alterTable, tableName, "Unknown column");
    }

    @Test
    public void testChangeToDuplicateColumn() {
        String tableName = "test_change_to_duplicate_column";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s change column a b int";
        executeAndFail(alterTable, tableName, "Duplicate column");
    }

    @Test
    public void testAddColumnAndIndex() {
        String tableName = "test_add_column_and_index";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add column c int, add index idx1(c)";
        executeAndFail(alterTable, tableName, "Multi alter specifications when create GSI not support yet");
    }

    @Test
    public void testAddColumnAndIndex2() {
        String tableName = "test_add_column_and_index_2";

        String createTable = "create table %s (a int,b char) broadcast";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add column c int, add index idx1(c)";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testAddDuplicateIndex() {
        String tableName = "test_add_duplicate_index";

        String createTable = "create table %s (a int,b char, index idx1(a))";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add index idx1(a)";
        executeAndFail(alterTable, tableName, "Global Secondary Index 'idx1' already exists");
    }

    @Test
    public void testAddDuplicateIndex2() {
        String tableName = "test_add_duplicate_index_2";

        String createTable = "create table %s (a int,b char, index idx1(a)) broadcast";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add index idx1(a)";
        executeAndFail(alterTable, tableName, "Duplicate key");
    }

    @Test
    public void testAddDuplicateIndex3() {
        String tableName = "test_add_duplicate_index_3";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add index idx1(a), add index idx1(a)";
        executeAndFail(alterTable, tableName, "Multi alter specifications when create GSI not support yet");
    }

    @Test
    public void testAddDuplicateIndex4() {
        String tableName = "test_add_duplicate_index_4";

        String createTable = "create table %s (a int,b char) broadcast";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add index idx1(a), add index idx1(a)";
        executeAndFail(alterTable, tableName, "Duplicate key");
    }

    @Test
    public void testAddAndDropIndex() {
        String tableName = "test_add_and_drop_index";

        String createTable = "create table %s (a int,b char)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add index idx1(a), drop index idx1";
        executeAndFail(alterTable, tableName, "Multi alter specifications when create GSI not support yet");
    }

    @Test
    public void testAddAndDropIndex2() {
        String tableName = "test_add_and_drop_index_2";

        String createTable = "create table %s (a int,b char) broadcast";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s add index idx1(a), drop index idx1";
        executeAndFail(alterTable, tableName, "Unknown");
    }

    @Test
    public void testDropAndAddIndex() {
        String tableName = "test_drop_and_add_index";

        String createTable = "create table %s (a int,b char, index idx1(a))";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s drop index idx1, add index idx1(b)";
        executeAndFail(alterTable, tableName, "Multi alter specifications when drop GSI not support yet");
    }

    @Test
    public void testDropAndAddIndex2() {
        String tableName = "test_drop_and_add_index_2";

        String createTable = "create table %s (a int,b char, index idx1(a)) broadcast";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s drop index idx1, add index idx1(b)";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testRenameIndex() {
        String tableName = "test_rename_index";

        String createTable = "create table %s (a int,b char, index idx1(a)) broadcast";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s rename index idx1 to idx2";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testRenameUnknownIndex() {
        String tableName = "test_rename_unknown_index";

        String createTable = "create table %s (a int,b char, index idx1(a))";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s rename index idx2 to idx3";
        executeAndFail(alterTable, tableName, "Unknown");
    }

    @Test
    public void testRenameDuplicateIndex() {
        String tableName = "test_rename_duplicate_index";

        String createTable = "create table %s (a int,b char, index idx1(a), index idx2(a)) broadcast";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s rename index idx1 to idx2";
        executeAndFail(alterTable, tableName, "Duplicate key");
    }

    @Test
    public void testRenameAndAddIndex() {
        String tableName = "test_rename_and_add_index";

        String createTable = "create table %s (a int,b char, index idx1(a)) broadcast";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s rename index idx1 to idx2, add index idx1(a)";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testDropAllColumns() {
        String tableName = "test_drop_all_columns";

        String createTable = "create table %s (a int)";
        executeAndCheck(createTable, tableName);

        String alterTable = "alter table %s drop column a";
        executeAndFail(alterTable, tableName, "can not delete all columns");

        alterTable = "alter table %s drop column a, add column b int";
        executeAndCheck(alterTable, tableName);

        alterTable = "alter table %s add column c int, drop column b";
        executeAndCheck(alterTable, tableName);
    }

    @Test
    public void testMultiChangeColumns() {
        String tableName = "test_multi_change_columns";

        String createTable = "create table %s (a int, b double, c varchar(10), d bigint)";
        executeAndCheck(createTable, tableName);

        String alterTable =
            "alter table %s change column a b int after c, change column b c double after d, change column c d varchar(10) after a, change column d a int after b";
        executeAndFail(alterTable, tableName, "Duplicate column");
    }

    @Test
    public void testMultiAddChangeColumns() {
        String tableName = "test_multi_add_change_columns";

        String createTable = "create table %s (a int, b double, c varchar(10), d bigint)";
        executeAndCheck(createTable, tableName);

        String alterTable =
            "alter table %s add column a int, add column b double, change column a x int after a, change column b y int after b";
        executeAndFail(alterTable, tableName, "Duplicate column");
    }

    @Test
    public void testMultiAddChangeColumns2() {
        String tableName = "test_multi_add_change_columns_2";

        String createTable = "create table %s (a int, b double, c varchar(10), d bigint)";

        executeAndCheck(createTable, tableName);

        String alterTable =
            "alter table %s add column a int, change column a x int after a, change column b y int after b, add column b double";
        executeAndFail(alterTable, tableName, "Duplicate column");
    }

    @Test
    public void testMultiAddChangeColumns3() {
        String tableName = "test_multi_add_change_columns_3";

        String createTable = "create table %s (a int, b double, c varchar(10), d bigint)";

        executeAndCheck(createTable, tableName);

        String alterTable =
            "alter table %s add column a int, change column b y int after b, add column b double, change column a x int after a";
        executeAndFail(alterTable, tableName, "Duplicate column");
    }
}
