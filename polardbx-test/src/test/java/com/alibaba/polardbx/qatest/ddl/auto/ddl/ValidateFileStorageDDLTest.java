package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
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

public class ValidateFileStorageDDLTest extends DDLBaseNewDBTestCase {

    private static Engine engine = Engine.LOCAL_DISK;
    private String tableName = "";
    private String innodbTableName = "";
    private String innodbTableGroupName = "";
    private String ossTableGroupName = "";
    private String functionName = "";
    private final String partitionByHashSuffix = "_partition_by_hash";
    private final String partitionByListSuffix = "_partition_by_list";

    private static String NOT_SUPPORTED = "not supported";
    private static String ERR_ADD_FK_ENGINE = "Cannot add foreign key constraint on engine";
    private static String INVOLVE_FILE_STORAGE = "involves file storage";
    private static String HASH_TABLE = "create table %s(a int, b int, id int) partition by hash(a) partitions 3";

    private static String RANGE_TABLE =
        "create table %s(a int, b int, id int) "
            + "partition by range(a) partitions 3 "
            + "(PARTITION p0 VALUES LESS THAN (25000), PARTITION p1 VALUES LESS THAN (50000), PARTITION p2 VALUES LESS THAN (75000))";

    private static String LIST_TABLE =
        "create table %s (a int, b int, id int) partition by list(a)"
            + "(partition p1 values in (1,2,3,4,5), partition p2 values in (6,7,8,9))";

    static String HASH_TTL_TABLE =
        "create table %s("
            + "    id bigint NOT NULL AUTO_INCREMENT,\n"
            + "    a bigint,\n"
            + "    b int,\n"
            + "    gmt_modified DATE NOT NULL,\n"
            + "    KEY (a),\n"
            + "    PRIMARY KEY (id, gmt_modified))\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 3\n"
            + "PRE ALLOCATE 3 DISABLE SCHEDULE\n";

    static String RANGE_TTL_TABLE =
        "create table %s("
            + "    id bigint NOT NULL AUTO_INCREMENT,\n"
            + "    a bigint,\n"
            + "    b int,\n"
            + "    gmt_modified DATE NOT NULL,\n"
            + "    KEY (a),\n"
            + "    PRIMARY KEY (id, gmt_modified))\n"
            + "partition by range(a) partitions 3 "
            + "(PARTITION p0 VALUES LESS THAN (25000), PARTITION p1 VALUES LESS THAN (50000), PARTITION p2 VALUES LESS THAN (75000))"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 3\n"
            + "PRE ALLOCATE 3 DISABLE SCHEDULE\n";

    static String LIST_TTL_TABLE =
        "create table %s("
            + "    id bigint NOT NULL AUTO_INCREMENT,\n"
            + "    a bigint,\n"
            + "    b int,\n"
            + "    gmt_modified DATE NOT NULL,\n"
            + "    KEY (a),\n"
            + "    PRIMARY KEY (id, gmt_modified))\n"
            + "partition by list(a)"
            + "(partition p1 values in (1,2,3,4,5), partition p2 values in (6,7,8,9)) "
            + "PARTITION BY HASH(id)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 3\n"
            + "PRE ALLOCATE 3 DISABLE SCHEDULE\n";

    private ArchiveMode mode;

    public ValidateFileStorageDDLTest(String mode, String crossSchema) {
        this.mode = ArchiveMode.of(mode);
        this.crossSchema = Boolean.parseBoolean(crossSchema);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Parameterized.Parameters(name = "{index}:mode={0},crossSchema={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(
            new String[][] {
                {ArchiveMode.EMPTY.toString(), String.valueOf(false)},
                {ArchiveMode.EMPTY.toString(), String.valueOf(true)},
                {ArchiveMode.TTL.toString(), String.valueOf(false)},
                {ArchiveMode.TTL.toString(), String.valueOf(true)}
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

    @Before
    public void init() {
        this.tableName = randomTableName("oss_table", 4);
        this.innodbTableName = randomTableName("innodb_table", 4);
        this.innodbTableGroupName = randomTableName("test_tg", 4);
        this.functionName = randomTableName("test_func", 4);

        clear();

        createFileStorage();

        switch (mode) {
        case EMPTY:
        case LOADING:
            JdbcUtil.executeUpdateSuccess(getInnodbConn(), String.format(RANGE_TABLE, innodbTableName));
            JdbcUtil.executeUpdateSuccess(getInnodbConn(),
                String.format(HASH_TABLE, innodbTableName + partitionByHashSuffix));
            JdbcUtil.executeUpdateSuccess(getInnodbConn(),
                String.format(LIST_TABLE, innodbTableName + partitionByListSuffix));
            break;
        case TTL:
            LocalDate startWithDate = LocalDate.now().minusMonths(11L);
            JdbcUtil.executeUpdateSuccess(getInnodbConn(),
                String.format(RANGE_TTL_TABLE, innodbTableName, startWithDate));
            JdbcUtil.executeUpdateSuccess(getInnodbConn(),
                String.format(HASH_TTL_TABLE, innodbTableName + partitionByHashSuffix, startWithDate));
            JdbcUtil.executeUpdateSuccess(getInnodbConn(),
                String.format(LIST_TTL_TABLE, innodbTableName + partitionByListSuffix, startWithDate));
            break;
        default:
        }
        JdbcUtil.executeSuccess(getFileStoreConn(),
            String.format("create table %s like %s.%s engine = '%s' archive_mode = '%s';", tableName,
                getInnodbSchema(), innodbTableName, engine.name(), mode));
        JdbcUtil.executeSuccess(getFileStoreConn(),
            String.format("create table %s like %s.%s engine = '%s' archive_mode = '%s';",
                tableName + partitionByHashSuffix,
                getInnodbSchema(), innodbTableName + partitionByHashSuffix, engine.name(), mode));
        JdbcUtil.executeSuccess(getFileStoreConn(),
            String.format("create table %s like %s.%s engine = '%s' archive_mode = '%s';",
                tableName + partitionByListSuffix,
                getInnodbSchema(), innodbTableName + partitionByListSuffix, engine.name(), mode));

        JdbcUtil.executeSuccess(getFileStoreConn(), String.format("unarchive database %s", getInnodbSchema()));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(getFileStoreConn(), "show tablegroup")) {
            while (rs.next()) {
                String name = rs.getString("TABLE_GROUP_NAME");
                if (TableGroupNameUtil.isOssTg(name)) {
                    ossTableGroupName = name;
                    break;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(!ossTableGroupName.equals(""));

        JdbcUtil.executeUpdateSuccess(getInnodbConn(), "create tablegroup " + innodbTableGroupName);
    }

    @Test
    public void testAll() {
        testALterTableRemoveLocalPartition();
        testAlterTableRepartitionLocalPartition();
        testAlterTableAddPartition();
        testAlterTableDropPartition();
        testAlterTableExtractPartition();
        testAlterTableModifyPartition();
        testAlterTableAddAndDropIndex();
        testAlterTableMergePartition();
        testAlterTableSplitPartition();
        testAlterTableSplitPartitionByHotValue();
        testAlterTableRepartition();
        testAlterTableSetTableGroup();
        testAlterTableGroupAddTable();
        testAlterTableGroupRenamePartition();
        testCreateAndDropIndex();
        testCreateTableGroup();
        testDropTableGroup();
        testMergeTableGroup();
        testOptimizeTable();
        testTruncateTable();
        testUnArchive();
        testPushDownUdf();
        testSequenceDdl();
    }

    @Test
    public void testAlterTableCommon() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL ENABLE_FOREIGN_KEY = true");

        String sql;
        // add column, add fulltext key, drop column
        sql = "alter table " + tableName + " add column x varchar(10)";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        if (mode == ArchiveMode.TTL) {
            sql = "alter table " + tableName + " add fulltext index(x)";
            JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "Not all physical DDLs");
        }
        sql = "alter table " + tableName + " drop column x";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);

        sql = "alter table " + innodbTableName + " add column x varchar(10)";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        if (mode == ArchiveMode.TTL) {
            sql = "alter table " + innodbTableName + " add fulltext index(x)";
            JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, "Not all physical DDLs");
        }
        sql = "alter table " + innodbTableName + " drop column x";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);

        //add foreign key
        sql = "alter table " + tableName + " add foreign key(a)" + " REFERENCES " + tableName + partitionByHashSuffix
            + "(a)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + innodbTableName + " add foreign key(a)" + " REFERENCES " + innodbTableName
            + partitionByHashSuffix + "(a)";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);

        // add and drop unique key
        if (mode != ArchiveMode.TTL) {
            sql = "alter table " + tableName + " add unique index test_idx(a, b)";
            JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
            sql = "alter table " + tableName + " drop index test_idx";
            JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
            sql = "alter table " + innodbTableName + " add unique index test_idx(a, b)";
            JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
            sql = "alter table " + innodbTableName + " drop index test_idx";
            JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        }

        // add and drop primary key
        sql = "alter table " + tableName + " drop primary key";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + tableName + " add primary key(id)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);

        // add check
        sql = "alter table " + tableName + " add check (b > 0)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, NOT_SUPPORTED);
        sql = "alter table " + innodbTableName + " add check (b > 0)";
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, NOT_SUPPORTED);

        // set default charset
        sql = "alter table " + tableName + " DEFAULT CHARACTER SET = gbk";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "alter table " + innodbTableName + " DEFAULT CHARACTER SET = gbk";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);

        // convert charset
        sql = "alter table " + tableName + " CONVERT TO CHARACTER SET gbk";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + innodbTableName + " CONVERT TO CHARACTER SET gbk";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);

        // disable and enable keys
        sql = "alter table " + tableName + " DISABLE keys";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + tableName + " ENABLE keys";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, INVOLVE_FILE_STORAGE);
        sql = "alter table " + innodbTableName + " DISABLE keys";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + innodbTableName + " ENABLE keys";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);

        // order by
        sql = "alter table " + tableName + " order by a";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "alter table " + innodbTableName + " order by a";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);

        // add, rename and drop index
        sql = "alter table " + tableName + " add index test_idx(b)";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "alter table " + tableName + " rename index test_idx to test_idx1";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "alter table " + tableName + " drop index test_idx1";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "alter table " + innodbTableName + " add index test_idx(b)";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table " + innodbTableName + " drop index test_idx";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);

        // add, rename and drop index
        sql = "alter table " + tableName + " add local index test_idx(b)";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "alter table " + tableName + " rename index test_idx to test_idx1";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "alter table " + tableName + " drop index test_idx1";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);

        // rename table
        sql = "alter table " + tableName + " rename as inTest1";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "alter table inTest1 rename as " + tableName;
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "alter table " + innodbTableName + " rename as inTest1";
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = "alter table inTest1 rename as " + innodbTableName;
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
    }

    private void testAlterTableAddPartition() {
        String sql = "alter table " + tableName + " add partition (partition p3 values less than(100000))";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testAlterTableDropPartition() {
        String sql = "alter table " + tableName + " drop partition p2";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testAlterTableExtractPartition() {
        String sql = "alter table " + tableName + partitionByHashSuffix
            + " extract to partition hp by hot value(100)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testAlterTableModifyPartition() {
        String sql = "alter table " + tableName + partitionByListSuffix + " modify partition p2 add values(10, 11)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testAlterTableAddAndDropIndex() {
        String sql = "alter table " + tableName + " add index test_idx(b)";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "alter table " + tableName + " drop index test_idx";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
    }

    private void testAlterTableMergePartition() {
        String sql = "alter table " + tableName + " merge partitions p0, p1 to p01";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testAlterTableSplitPartition() {
        String sql = "alter table " + tableName + " split partition p0 at(10000) into (partition p01, partition p02)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testAlterTableSplitPartitionByHotValue() {
        String sql = "alter table " + tableName + " split into hp partitions 5 by hot value(88)";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testAlterTableRepartition() {
        // TODO(siyun): to be supported
    }

    private void testALterTableRemoveLocalPartition() {
        String sql = "alter table " + tableName + " remove local partitioning";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testAlterTableRepartitionLocalPartition() {
        String sql = "alter table " + tableName
            + " LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 6\n"
            + "PRE ALLOCATE 3 DISABLE SCHEDULE\n";
        LocalDate startWithDate = LocalDate.now().minusMonths(11L);
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), String.format(sql, startWithDate), "");
    }

    private void testAlterTableSetTableGroup() {
        /* check oss table */
        String sql = String.format("alter table %s set tablegroup=%s", tableName, innodbTableGroupName);
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
        /* check oss table group */
        sql = String.format("alter table %s set tablegroup=%s", innodbTableName, ossTableGroupName);
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, "");
    }

    private void testAlterTableGroupAddTable() {
        /* check oss table */
        String sql = String.format("alter tablegroup %s add %s", innodbTableGroupName, tableName);
        JdbcUtil.executeUpdateFailed(getInnodbConn(), sql, "");
        /* check oss table group */
        sql = String.format("alter tablegroup %s add %s", ossTableGroupName, innodbTableName);
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testAlterTableGroupRenamePartition() {
        String sql = String.format("alter tablegroup %s rename partition p1 to np1", ossTableGroupName);
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testCreateAndDropIndex() {
        String sql = "create index test_idx on " + tableName + "(b)";
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = "drop index test_idx on " + tableName;
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
    }

    private void testCreateTableGroup() {
        // prevent creating table group like 'oss_%'
        String sql = "create tablegroup oss_tg_invalid";
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testDropTableGroup() {
        String sql = "drop tablegroup " + ossTableGroupName;
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testMergeTableGroup() {
        String sql = String.format("merge tablegroups %s into %s force", ossTableGroupName, innodbTableGroupName);
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testOptimizeTable() {
        // TODO(siyun): to be supported
        String sql = "optimize table " + tableName;
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testTruncateTable() {
        String sql = "truncate table " + tableName;
        JdbcUtil.executeUpdateFailed(getFileStoreConn(), sql, "");
    }

    private void testUnArchive() {
//        String sql = "unarchive table " + tableName;
//        JdbcUtil.executeUpdateFailed(tddlConnection, sql, tableName);
    }

    private void testPushDownUdf() {
//        JdbcUtil.executeSuccess(tddlConnection, "drop function if exists " + functionName);
//        JdbcUtil.executeSuccess(tddlConnection,
//            String.format("create function %s(a int) returns int %s return a + 1", functionName, " no sql"));
//        String sql = String.format("select a, %s(b) from %s", functionName, tableName);
//        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
//        JdbcUtil.executeUpdateFailed(tddlConnection, "pushdown udf", "");
//        String.format("select a, %s(b) from %s", functionName, innodbTableName);
//        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
//        JdbcUtil.executeUpdateSuccess(tddlConnection, "pushdown udf");
    }

    private void testSequenceDdl() {
        /* skip */
    }

    @After
    public void clear() {
        String sql;
        sql = String.format("drop table if exists %s", innodbTableName);
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = String.format("drop table if exists %s", innodbTableName + partitionByHashSuffix);
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = String.format("drop table if exists %s", innodbTableName + partitionByListSuffix);
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);
        sql = String.format("drop tablegroup if exists %s", innodbTableGroupName);
        JdbcUtil.executeUpdateSuccess(getInnodbConn(), sql);

        sql = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = String.format("drop table if exists %s", tableName + partitionByHashSuffix);
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);
        sql = String.format("drop table if exists %s", innodbTableName + partitionByListSuffix);
        JdbcUtil.executeUpdateSuccess(getFileStoreConn(), sql);

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
