package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public class ValidateFileStorageBindingDDLTest extends DDLBaseNewDBTestCase {

    private static Engine engine = Engine.LOCAL_DISK;

    private static String UNARCHIVE_TABLE = "unarchive table";

    private static String UNARCHIVE_TABLEGROUP = "unarchive tablegroup";
    private static String INVOLVE_FILE_STORAGE = "involves file storage";

    private static String NOT_SUPPORTED = "not supported";

    private static String FAIL_BOUND = "bound to";

    public ValidateFileStorageBindingDDLTest(String crossSchema) {
        this.crossSchema = Boolean.parseBoolean(crossSchema);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<String[]> preparePara() {
        return Arrays.asList(
            new String[][] {
                {String.valueOf(false)},
                {String.valueOf(true)}
            });
    }

    private String getInnodbSchema() {
        return tddlDatabase1;
    }

    private Connection getInnodbConn() {
        return getTddlConnection1();
    }

    private Connection getFileStoreConn() {
        return crossSchema ? getTddlConnection2() : getTddlConnection1();
    }

    private String ossHashTable;
    private String ossListTable;
    private String ossRangeTable;
    private String innodbHashTable;
    private String innodbListTable;
    private String innodbRangeTable;

    private String innodbTableGroup;

    private String innodbHashTableGroup;

    @Before
    public void init() {
        this.ossHashTable = randomTableName("oss_table_hash", 4);
        this.ossListTable = randomTableName("oss_table_list", 4);
        this.ossRangeTable = randomTableName("oss_table_range", 4);
        this.innodbHashTable = randomTableName("innodb_table_hash", 4);
        this.innodbListTable = randomTableName("innodb_table_list", 4);
        this.innodbRangeTable = randomTableName("innodb_table_range", 4);
        this.innodbTableGroup = randomTableName("test_tg", 4);

        JdbcUtil.executeUpdateSuccess(getInnodbConn(), String.format("drop table if exists %s", innodbHashTable));
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), String.format("drop table if exists %s", innodbListTable));
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), String.format("drop table if exists %s", innodbRangeTable));
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), String.format("drop tablegroup if exists %s", innodbTableGroup));
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), String.format("drop table if exists %s", ossHashTable));
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), String.format("drop table if exists %s", ossListTable));
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), String.format("drop table if exists %s", ossRangeTable));

        createFileStorage();

        LocalDate startWithDate = LocalDate.now().minusMonths(11L);
        JdbcUtil.executeUpdateSuccess(getInnodbConn(),
            String.format(ValidateFileStorageDDLTest.RANGE_TTL_TABLE, innodbRangeTable, startWithDate));
        JdbcUtil.executeUpdateSuccess(getInnodbConn(),
            String.format(ValidateFileStorageDDLTest.HASH_TTL_TABLE, innodbHashTable, startWithDate));
        JdbcUtil.executeUpdateSuccess(getInnodbConn(),
            String.format(ValidateFileStorageDDLTest.LIST_TTL_TABLE, innodbListTable, startWithDate));

        JdbcUtil.executeSuccess(getFileStoreConn(),
            String.format("create table %s like %s.%s engine = '%s' archive_mode = '%s';", ossHashTable,
                getInnodbSchema(), innodbHashTable, engine.name(), ArchiveMode.TTL.name()));
        JdbcUtil.executeSuccess(getFileStoreConn(),
            String.format("create table %s like %s.%s engine = '%s' archive_mode = '%s';", ossListTable,
                getInnodbSchema(), innodbListTable, engine.name(), ArchiveMode.TTL.name()));
        JdbcUtil.executeSuccess(getFileStoreConn(),
            String.format("create table %s like %s.%s engine = '%s' archive_mode = '%s';", ossRangeTable,
                getInnodbSchema(), innodbRangeTable, engine.name(), ArchiveMode.TTL.name()));

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(getInnodbConn(), "show full tablegroup")) {
            while (rs.next()) {
                String name = rs.getString("TABLES");
                if (name.equalsIgnoreCase(innodbHashTable)) {
                    this.innodbHashTableGroup = rs.getString("TABLE_GROUP_NAME");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        JdbcUtil.executeUpdateSuccess(getInnodbConn(), "create tablegroup " + innodbTableGroup);
    }

    @After
    public void clear() {
        cleanDataBase();
    }

    @Test
    public void testAll() {
        testALterTableRemoveLocalPartition();
        testAlterTableRepartitionLocalPartition();
        testAlterTableAddPartition();
        testAlterTableDropPartition();
        testAlterTableExtractPartition();
        testAlterTableModifyPartition();
        testAlterTableMergePartition();
        testAlterTableSplitPartition();
        testAlterTableSplitPartitionByHotValue();
        testAlterTableRepartition();
        testAlterTableSetTableGroup();
        testAlterTableGroupAddTable();
        testAlterTableGroupRenamePartition();
        testCreateAndDropIndex();
        testMergeTableGroup();
        testOptimizeTable();
        testTruncateTable();
    }

    @Test
    public void testAlterTableCommon() {
        String sql;

        // add generated column
        sql = "alter table " + innodbHashTable + " add column c bigint as (a+1) logical";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, "");
        sql = "alter table " + innodbHashTable + " add column c bigint as (a+1) virtual";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, "");
        sql = "alter table " + innodbHashTable + " add column c bigint as (a+1) stored";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, "");
        sql = "alter table " + ossHashTable + " add column c bigint as (a+1) logical";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);
        sql = "alter table " + ossHashTable + " add column c bigint as (a+1) virtual";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);
        sql = "alter table " + ossHashTable + " add column c bigint as (a+1) stored";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);

        // add column, add fulltext key, drop column
        sql = "alter table " + innodbHashTable + " add column x varchar(10)";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + innodbHashTable + " add fulltext index(x)";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, "Not all physical DDLs");
        sql = "alter table " + innodbHashTable + " drop column x";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + ossHashTable + " add column x varchar(10)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);
        sql = "alter table " + ossHashTable + " add fulltext index(x)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);
        sql = "alter table " + ossHashTable + " drop column x";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);

        //add foreign key
        sql = "alter table " + innodbHashTable + " add foreign key(a)" + " REFERENCES " + innodbHashTable + "(a)";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, "");
        sql = "alter table " + ossHashTable + " add foreign key(a)" + " REFERENCES " + ossRangeTable + "(a)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");

        // add and drop primary key
        sql = "alter table " + innodbHashTable + " drop primary key";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + innodbHashTable + " add primary key(id)";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + ossHashTable + " drop primary key";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);

        // add check
        sql = "alter table " + innodbHashTable + " add check (b > 0)";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, NOT_SUPPORTED);
        sql = "alter table " + ossHashTable + " add check (b > 0)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, NOT_SUPPORTED);

        // set default charset
        sql = "alter table " + innodbHashTable + " DEFAULT CHARACTER SET = gbk";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + ossHashTable + " DEFAULT CHARACTER SET = gbk";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);

        // convert charset
        sql = "alter table " + innodbHashTable + " CONVERT TO CHARACTER SET gbk";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + ossHashTable + " CONVERT TO CHARACTER SET gbk";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);

        // disable and enable keys
        sql = "alter table " + innodbHashTable + " DISABLE keys";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + innodbHashTable + " ENABLE keys";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + ossRangeTable + " DISABLE keys";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);

        // order by
        sql = "alter table " + innodbHashTable + " order by a";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + ossRangeTable + " order by a";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);

        // add, rename and drop index
        sql = "alter table " + innodbHashTable + " add index test_idx(b)";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + innodbHashTable + " rename index test_idx to test_idx1";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + innodbHashTable + " drop index test_idx";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + ossRangeTable + " add index test_idx(b)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);

        // add, rename and drop index
        sql = "alter table " + innodbHashTable + " add local index test_idx(b)";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + innodbHashTable + " rename index test_idx to test_idx1";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + innodbHashTable + " drop index test_idx1";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + ossHashTable + " add local index test_idx(b)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);

        // rename table
        sql = "alter table " + innodbHashTable + " rename as inTest1";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table inTest1 rename as " + innodbHashTable;
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + ossListTable + " rename as inTest1";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, FAIL_BOUND);
    }

    private void testAlterTableAddPartition() {
        String sql = "alter table " + innodbRangeTable + " add partition (partition p3 values less than(100000))";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLE);
        sql = "alter table " + ossRangeTable + " add partition (partition p3 values less than(100000))";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
    }

    private void testAlterTableDropPartition() {
        String sql = "alter table " + innodbRangeTable + " drop partition p2";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLE);
        sql = "alter table " + ossRangeTable + " drop partition p2";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
    }

    private void testAlterTableExtractPartition() {
        String sql = "alter table " + innodbHashTable + " extract to partition hp by hot value(100)";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLE);
        sql = "alter table " + ossHashTable + " extract to partition hp by hot value(100)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
    }

    private void testAlterTableModifyPartition() {
        String sql = "alter table " + innodbListTable + " modify partition p2 add values(10, 11)";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLE);
        sql = "alter table " + ossListTable + " modify partition p2 add values(10, 11)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
    }

    private void testAlterTableMergePartition() {
        String sql = "alter table " + innodbRangeTable + " merge partitions p0, p1 to p01";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLE);
        sql = "alter table " + ossRangeTable + " merge partitions p0, p1 to p01";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
    }

    private void testAlterTableSplitPartition() {
        String sql =
            "alter table " + innodbRangeTable + " split partition p0 at(10000) into (partition p01, partition p02)";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLE);
        sql = "alter table " + ossRangeTable + " split partition p0 at(10000) into (partition p01, partition p02)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
    }

    private void testAlterTableSplitPartitionByHotValue() {
        String sql = "alter table " + innodbRangeTable + " split into hp partitions 5 by hot value(88)";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLE);
        sql = "alter table " + ossRangeTable + " split into hp partitions 5 by hot value(88)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);

    }

    private void testAlterTableRepartition() {
        // TODO(siyun): to be supported
    }

    private void testALterTableRemoveLocalPartition() {
        String sql = "alter table " + innodbHashTable + " remove local partitioning";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, "");
    }

    private void testAlterTableRepartitionLocalPartition() {
        String sql = "alter table " + innodbHashTable
            + " LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 6\n"
            + "PRE ALLOCATE 3 DISABLE SCHEDULE\n";
        LocalDate startWithDate = LocalDate.now().minusMonths(11L);
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), String.format(sql, startWithDate));
    }

    private void testAlterTableSetTableGroup() {
        /* check oss table */
        String sql = String.format("alter table %s set tablegroup=%s", innodbHashTable, innodbTableGroup);
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLE);
        /* check oss table group */
        sql = String.format("alter table %s set tablegroup=%s", ossHashTable, innodbTableGroup);
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
    }

    private void testAlterTableGroupAddTable() {
        String sql = String.format("alter tablegroup %s add tables %s", innodbTableGroup, innodbHashTable);
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLE);
    }

    private void testAlterTableGroupRenamePartition() {
        String sql = String.format("alter tablegroup %s rename partition p1 to np1", innodbHashTableGroup);
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLEGROUP);
    }

    private void testCreateAndDropIndex() {
        String sql = "create index test_idx on " + innodbHashTable + "(b)";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "drop index test_idx on " + innodbHashTable;
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
    }

    private void testMergeTableGroup() {
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), "drop table if exists tmp_test");
        LocalDate startWithDate = LocalDate.now().minusMonths(1L);
        JdbcUtil.executeUpdateSuccess(getInnodbConn(),
            String.format(ValidateFileStorageDDLTest.HASH_TTL_TABLE, "tmp_test", startWithDate));
        JdbcUtil.executeUpdateSuccess(getInnodbConn(),
            String.format("alter table tmp_test set tablegroup=%s", innodbTableGroup));
        String sql = String.format("merge tablegroups %s into %s force", innodbHashTableGroup, innodbTableGroup);
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, UNARCHIVE_TABLEGROUP);
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), "drop table tmp_test");
    }

    private void testOptimizeTable() {
        String sql = "optimize table " + innodbHashTable;
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
    }

    private void testTruncateTable() {
        String sql = "truncate table " + innodbHashTable;
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
    }

    private void createFileStorage() {
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(getFileStoreConn(), "show filestorage");

            boolean hasEngine = false;
            while (rs.next()) {
                String engineName = rs.getString("ENGINE");
                if (engine.name().equalsIgnoreCase(engineName)) {
                    hasEngine = true;
                    break;
                }
            }

            String createFileStorageSql =
                "create filestorage local_disk with(file_uri=\"../spill/file-storage-test\");";
            if (!hasEngine) {
                JdbcUtil.executeUpdateSuccess(getFileStoreConn(), createFileStorageSql);
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
