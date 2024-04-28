package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group2;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.util.Pair;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BINARY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIT_64;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB_LONG;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB_MEDIUM;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB_TINY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DATETIME_6;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DECIMAL;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ENUM;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_GEOMETORY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_JSON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_LINESTRING;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTILINESTRING;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTIPOINT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTIPOLYGON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_POINT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_POLYGON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_SET;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT_LONG;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT_MEDIUM;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT_TINY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP_6;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIME_6;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.constant.TableConstant.dateType;
import static com.alibaba.polardbx.qatest.constant.TableConstant.floatType;
import static com.alibaba.polardbx.qatest.constant.TableConstant.timeType;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;
import static com.google.common.truth.Truth.assertWithMessage;

public class GsiUpdateTypeTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME = "gsi_update_type_test_primary";
    private static final String INDEX_TABLE_NAME = "gsi_update_type_test_gsi";
    private static final ImmutableMap<String, List<String>> GSI_FULL_TYPE_TEST_INSERTS =
        GsiConstant.buildGsiFullTypeTestInserts(PRIMARY_TABLE_NAME);
    private static final String UPSERT_INIT_DATA = "insert into " + PRIMARY_TABLE_NAME + "(`id`) values (1)";
    private static final String DELETE_DATA = "delete from " + PRIMARY_TABLE_NAME;

    private static final String FULL_TYPE_TABLE =
        ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME, DEFAULT_PARTITIONING_DEFINITION);
    private static final String FULL_TYPE_TABLE_MYSQL = ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME, "");

    private static final Set<String> CN_UNSUPPORTED_FUNC_TYPE = new HashSet<>();
    private static final Set<String> UK_WITH_LENGTH_TYPE = new HashSet<>();

    static {
        // CN does not support geo functions like ST_PointFromText
        CN_UNSUPPORTED_FUNC_TYPE.add(C_GEOMETORY);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_POINT);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_LINESTRING);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_POLYGON);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_MULTIPOINT);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_MULTILINESTRING);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_MULTIPOLYGON);

        UK_WITH_LENGTH_TYPE.add(C_TEXT_TINY);
        UK_WITH_LENGTH_TYPE.add(C_TEXT);
        UK_WITH_LENGTH_TYPE.add(C_TEXT_MEDIUM);
        UK_WITH_LENGTH_TYPE.add(C_TEXT_LONG);
        UK_WITH_LENGTH_TYPE.add(C_BLOB_TINY);
        UK_WITH_LENGTH_TYPE.add(C_BLOB);
        UK_WITH_LENGTH_TYPE.add(C_BLOB_MEDIUM);
        UK_WITH_LENGTH_TYPE.add(C_BLOB_LONG);
    }

    private String dataColumn = null;

    public GsiUpdateTypeTest(String indexSk) {
        this.dataColumn = indexSk;
    }

    @Parameters(name = "{index}:indexSk={0}")
    public static List<String[]> prepareDate() {
        return FULL_TYPE_TABLE_COLUMNS.stream().map(c -> new String[] {c}).collect(Collectors.toList());
    }

    public void initTables() throws SQLException {
        // JDBC handles zero-date differently in prepared statement and statement, so ignore this case in cursor fetch
        org.junit.Assume.assumeFalse(
            PropertiesUtil.useCursorFetch() && (dataColumn.contains("time") || dataColumn.contains("year")
                || dataColumn.contains("date")));
        // just do not test primary key
        org.junit.Assume.assumeFalse(dataColumn.equalsIgnoreCase(C_ID));
        // out of range for BIT_64 in JDBC
        org.junit.Assume.assumeFalse(!useXproto() && dataColumn.equalsIgnoreCase(C_BIT_64));

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, FULL_TYPE_TABLE_MYSQL);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_TABLE);
    }

    private void initData(List<String> inserts) throws SQLException {
        // List<Pair< sql, error_message >>
        List<Pair<String, Exception>> failedList = new ArrayList<>();

        // Prepare data
        for (String insert : inserts) {
            gsiExecuteUpdate(tddlConnection, mysqlConnection, insert, failedList, true, true);
        }

        System.out.println("Failed inserts: ");
        failedList.forEach(p -> System.out.println(p.left));

        final ResultSet resultSet = JdbcUtil.executeQuery("SELECT COUNT(1) FROM " + PRIMARY_TABLE_NAME, tddlConnection);
        assertWithMessage("查询测试数据集大小失败").that(resultSet.next()).isTrue();
        assertWithMessage("测试数据集为空").that(resultSet.getLong(1)).isGreaterThan(0L);
    }

    private void clearData() throws SQLException {
        // List<Pair< sql, error_message >>
        List<Pair<String, Exception>> failedList = new ArrayList<>();

        // Delete data
        gsiExecuteUpdate(tddlConnection, mysqlConnection, DELETE_DATA, failedList, true, true);

        System.out.println("Failed delete: ");
        failedList.forEach(p -> System.out.println(p.left));
    }

    /*
     * Update / Upsert / Replace 下推执行
     */
    @Test
    public void testPushDownDML() throws SQLException {
        initTables();

        // Update
        clearData();
        initData(GSI_FULL_TYPE_TEST_INSERTS.get(C_ID));
        List<String> values = new ArrayList<>(GsiConstant.FULL_TYPE_TEST_VALUES.get(dataColumn));
        for (String value : values) {
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
        }

        // Upsert
        clearData();
        initData(ImmutableList.of(UPSERT_INIT_DATA));

        for (String value : values) {
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update = MessageFormat.format("INSERT INTO {0}(`id`) VALUES (1) ON DUPLICATE KEY UPDATE {1}={2}",
                PRIMARY_TABLE_NAME, dataColumn, value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
        }

        // Replace
        clearData();
        initData(ImmutableList.of(UPSERT_INIT_DATA));

        for (String value : values) {
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update =
                MessageFormat.format("REPLACE INTO {0}(`id`,{1}) VALUES (1,{2})", PRIMARY_TABLE_NAME, dataColumn,
                    value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
        }
    }

    /*
     * Update / Upsert / Replace 逻辑执行
     */
    @Test
    public void testLogicalDMLWithGsi() throws SQLException {
        initTables();

        // Update
        clearData();
        initData(GSI_FULL_TYPE_TEST_INSERTS.get(C_ID));

        // Create a GSI to use logical execution
        String covering = MessageFormat.format("COVERING (`{0}`)", dataColumn);
        String createGsi = MessageFormat.format(
            "CREATE GLOBAL INDEX {0} ON {1}(`id`) {2} DBPARTITION BY HASH(`id`) TBPARTITION BY HASH(`id`) TBPARTITIONS 7",
            INDEX_TABLE_NAME, PRIMARY_TABLE_NAME, covering);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        // Update
        List<String> values = new ArrayList<>(GsiConstant.FULL_TYPE_TEST_VALUES.get(dataColumn));
        for (String value : values) {
            // Update with value pushdown
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

            // Update without value pushdown
            if (!CN_UNSUPPORTED_FUNC_TYPE.contains(dataColumn)) {
                failedList = new ArrayList<>();
                update = MessageFormat.format("/*+TDDL:ENABLE_PUSH_PROJECT=false*/ UPDATE {0} SET {1}={2}",
                    PRIMARY_TABLE_NAME, dataColumn, value);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            }

            // Update with DN select value
            update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, dataColumn);
            if (dataColumn.contains(C_ENUM) && value.contains("0")) {
                // 插入非法 enum 值的时候，在 c_enum=c_enum 时，读出来的是 ""，update 的时候会报 Data truncated
                JdbcUtil.executeUpdateFailed(tddlConnection, update, "Data truncated for column 'c_enum'");
            } else {
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            }
        }

        // Upsert
        if (!CN_UNSUPPORTED_FUNC_TYPE.contains(dataColumn)) {
            for (String value : values) {
                clearData();
                initData(ImmutableList.of(UPSERT_INIT_DATA));

                // set from specified value
                List<Pair<String, Exception>> failedList = new ArrayList<>();
                String update = MessageFormat.format("INSERT INTO {0}(`id`) VALUES (1) ON DUPLICATE KEY UPDATE {1}={2}",
                    PRIMARY_TABLE_NAME, dataColumn, value);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

                // set from insert value
                update =
                    MessageFormat.format("INSERT INTO {0}(`id`) VALUES (1) ON DUPLICATE KEY UPDATE {1}=values({2})",
                        PRIMARY_TABLE_NAME, dataColumn, dataColumn);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

                // set from select value
                update = MessageFormat.format("INSERT INTO {0}(`id`) VALUES (1) ON DUPLICATE KEY UPDATE {1}={2}",
                    PRIMARY_TABLE_NAME, dataColumn, dataColumn);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            }
        }

        // Replace
        for (String value : values) {
            clearData();
            initData(ImmutableList.of(UPSERT_INIT_DATA));

            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update =
                MessageFormat.format("REPLACE INTO {0}(`id`,{1}) VALUES (1,{2})", PRIMARY_TABLE_NAME, dataColumn,
                    value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
        }
    }

    /*
     * Update SK
     */
    @Test
    public void testLogicalUpdateWithGsiSK() throws SQLException {
        if (dataColumn.equalsIgnoreCase(C_BINARY)) {
            // In relocation, DELETE WRITER will use old 0x31310000000000000000 and INSERT writer will use '11', which
            // are in different shards
            return;
        }

        initTables();

        clearData();
        initData(GSI_FULL_TYPE_TEST_INSERTS.get(C_ID));

        // Create a GSI on data column to test modifying sharding key
        String createGsi;

        if (dateType.contains(dataColumn)) {
            createGsi = MessageFormat.format(
                "CREATE GLOBAL INDEX {0} ON {1}({2}) DBPARTITION BY YYYYMM({3}) TBPARTITION BY YYYYMM({4}) TBPARTITIONS 7",
                INDEX_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn, dataColumn, dataColumn);
        } else {
            createGsi = MessageFormat.format(
                "CREATE GLOBAL INDEX {0} ON {1}({2}) DBPARTITION BY HASH({3}) TBPARTITION BY HASH({4}) TBPARTITIONS 7",
                INDEX_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn, dataColumn, dataColumn);
        }
        boolean unsupportedSkType = JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, createGsi,
            ImmutableSet.of("Rule generator dataType is not supported!", "Invalid type for a sharding key"));

        List<String> values = new ArrayList<>(GsiConstant.FULL_TYPE_TEST_VALUES.get(dataColumn));
        for (String value : values) {
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            // Update with value pushdown
            String update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            if (!unsupportedSkType) {
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            } else {
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
            }

            // Update without value pushdown
            if (!CN_UNSUPPORTED_FUNC_TYPE.contains(dataColumn)) {
                update = MessageFormat.format("/*+TDDL:ENABLE_PUSH_PROJECT=false*/ UPDATE {0} SET {1}={2}",
                    PRIMARY_TABLE_NAME, dataColumn, value);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                if (!unsupportedSkType) {
                    gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
                } else {
                    gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
                }
            }

            // Update with DN select value
            update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, dataColumn);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            if (!unsupportedSkType) {
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            } else {
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
            }
        }
    }

    /*
     * Upsert / Replace 逻辑执行，含有 UK
     */
    @Test
    public void testLogicalDMLWithGsiUk() throws SQLException {
        if (CN_UNSUPPORTED_FUNC_TYPE.contains(dataColumn)) {
            // This case needs to eval value in CN, so if data contains unsupported function, it will fail this case
            return;
        }

        if (dataColumn.equalsIgnoreCase(C_JSON)) {
            // DN do not support JSON to be unique key
            return;
        }

        if (dataColumn.equalsIgnoreCase(C_SET)) {
            // SELECT used for SET must be in specified order, otherwise it will not select duplicated value
            return;
        }

        if (dataColumn.equalsIgnoreCase(C_BINARY)) {
            // SELECT used for BINARY must be in hex string, otherwise it will not select duplicated value
            return;
        }

        if (dateType.contains(dataColumn) && !(dataColumn.equalsIgnoreCase(C_TIMESTAMP_6)
            || dataColumn.equalsIgnoreCase(C_DATETIME_6))) {
            // Some data of date type will be truncated after insert, so it will not select duplicated value
            return;
        }

        if (timeType.contains(dataColumn) && !(dataColumn.equalsIgnoreCase(C_TIME_6))) {
            // Some data of date type will be truncated after insert, so it will not select duplicated value
            return;
        }

        if (floatType.contains(dataColumn)) {
            // Some data of float type will be truncated after insert, so it will not select duplicated value
            return;
        }

        if (dataColumn.equalsIgnoreCase(C_DECIMAL)) {
            // Some data of decimal type will be truncated after insert, so it will not select duplicated value
            return;
        }

        initTables();

        // Create a GSI to use logical execution
        String covering = MessageFormat.format("COVERING (`{0}`)", dataColumn);
        String createGsi = MessageFormat.format(
            "CREATE GLOBAL INDEX {0} ON {1}(`id`) {2} DBPARTITION BY HASH(`id`) TBPARTITION BY HASH(`id`) TBPARTITIONS 7",
            INDEX_TABLE_NAME, PRIMARY_TABLE_NAME, covering);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        // Create a local unique index
        String localUk = "local_uk";
        String ukLength = UK_WITH_LENGTH_TYPE.contains(dataColumn) ? "(60)" : "";

        String createUk =
            MessageFormat.format("CREATE LOCAL UNIQUE INDEX {0} on {1}({2}{3})", localUk, PRIMARY_TABLE_NAME,
                dataColumn, ukLength);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createUk);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createUk.replace("LOCAL", ""));

        // Update
        List<String> values = new ArrayList<>(GsiConstant.FULL_TYPE_TEST_VALUES.get(dataColumn));

        // Upsert
        if (!CN_UNSUPPORTED_FUNC_TYPE.contains(dataColumn)) {

            for (String value : values) {
                // ignore bad convert on enum
                if (dataColumn.contains(C_ENUM) && value.contains("0")) {
                    // 在处理 duplicate 的时候，依赖查询，因为 enum 拆入非法值会变成 ""，导致查询的时候查不到，从而使 update 变成了
                    // insert，同时由于 id 自增，且按 id 拆分，又会以非法值插入一条新数据，导致和 MySQL 行为不一致
                    continue;
                }

                clearData();
                initData(ImmutableList.of(UPSERT_INIT_DATA));

                // init data
                List<Pair<String, Exception>> failedList = new ArrayList<>();
                String update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, value);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

                // set from specified value
                failedList = new ArrayList<>();
                update = MessageFormat.format("INSERT INTO {0}(`{1}`) VALUES ({2}) ON DUPLICATE KEY UPDATE {1}={2}",
                    PRIMARY_TABLE_NAME, dataColumn, value);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

                // set from insert value
                update =
                    MessageFormat.format("INSERT INTO {0}(`{1}`) VALUES ({2}) ON DUPLICATE KEY UPDATE {1}=values({3})",
                        PRIMARY_TABLE_NAME, dataColumn, value, dataColumn);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

                // set from select value
                update = MessageFormat.format("INSERT INTO {0}(`{1}`) VALUES ({2}) ON DUPLICATE KEY UPDATE {1}={3}",
                    PRIMARY_TABLE_NAME, dataColumn, value, dataColumn);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            }
        }

        // Replace
        for (String value : values) {
            // ignore bad convert on enum
            if (dataColumn.contains(C_ENUM) && value.contains("0")) {
                // 在处理 duplicate 的时候，依赖查询，因为 enum 拆入非法值会变成 ""，导致查询的时候查不到，从而使 update 变成了
                // insert，同时由于 id 自增，且按 id 拆分，又会以非法值插入一条新数据，导致和 MySQL 行为不一致
                continue;
            }

            clearData();
            initData(ImmutableList.of(UPSERT_INIT_DATA));

            // init data
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

            failedList = new ArrayList<>();
            update =
                MessageFormat.format("REPLACE INTO {0}(`id`,{1}) VALUES (NULL,{2})", PRIMARY_TABLE_NAME, dataColumn,
                    value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
        }
    }

    private void gsiIntegrityCheck(String primary, String index, String dataColumn) throws SQLException {
        if (primary.equalsIgnoreCase(index)) {
            return;
        }
        gsiIntegrityCheck(primary, index, dataColumn, dataColumn, true);
        checkGsi(tddlConnection, index);
    }
}
