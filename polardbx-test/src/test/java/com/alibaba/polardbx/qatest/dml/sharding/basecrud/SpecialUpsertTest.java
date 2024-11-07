package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.google.common.truth.Truth;
import org.apache.calcite.util.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class SpecialUpsertTest extends CrudBasedLockTestCase {

    private static final String TABLE_DEF = "CREATE TABLE `%s` (\n"
        + "   `ID` varchar(32) NOT NULL ,\n"
        + "   `XX_DATE` date NOT NULL,\n"
        + "   PRIMARY KEY (`ID`)\n"
        + ") ";

    private static final String TABLE_DOUBLE_UK_DEF = "CREATE TABLE `%s` (\n"
        + "   `ID` varchar(32) NOT NULL ,\n"
        + "   `XX_DATE` date NOT NULL,\n"
        + "   PRIMARY KEY (`ID`),\n"
        + "   UNIQUE KEY (`XX_DATE`)\n"
        + ") ";

    private static final String PARTITION_DEF =
        "dbpartition by UNI_HASH(`id`) tbpartition by DD(`xx_date`) tbpartitions 31;";

    private static final List<String> CREATE_TIME = new ArrayList<>();

    static {
        CREATE_TIME.add("2024-01-26 00:00:00.0");
        CREATE_TIME.add("2024-01-27 00:00:00.0");
        CREATE_TIME.add("2024-01-28 00:00:00.0");
        CREATE_TIME.add("2024-01-29 00:00:00.0");
        CREATE_TIME.add("2024-01-30 00:00:00.0");
        CREATE_TIME.add("2024-01-31 00:00:00.0");
    }

    private static final String INSERT_DEF = "insert into %s(ID, XX_DATE) VALUES(?, ?)";

    public SpecialUpsertTest() {
        this.baseOneTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.HASH_BY_VARCHAR
            + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
        this.baseTwoTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.HASH_BY_VARCHAR
            + ExecuteTableName.TWO + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
    }

    private static final String UPSERT_WITH_AFTER_VALUE_DEF =
        "insert into %s(ID, XX_DATE) VALUES(1, '2024-02-27 00:00:00.0') "
            + "on duplicate key update xx_date=values(xx_date);";

    @Test
    public void pushdownUpsertWithPartialAfterTest() {
        final String tableName = "t1_pushdown_upsert11";

        rebuildTable(tableName, TABLE_DEF, PARTITION_DEF);

        prepareData(tableName, 3);

        executeAndTraceUpsert(UPSERT_WITH_AFTER_VALUE_DEF, tableName);

        checkTraceRowCount(1);

        final String sql = String.format("select id, xx_date from %s order by id", tableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void pushdownUpsertWithPartialAfterTest2() {
        final String tableName = "t1_pushdown_upsert12";

        rebuildTable(tableName, TABLE_DOUBLE_UK_DEF, PARTITION_DEF);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        prepareData(tableName, 3);

        executeAndTraceUpsert(
            "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ " + UPSERT_WITH_AFTER_VALUE_DEF,
            tableName);

        checkTraceRowCount(topology.size() + 2);

        final String sql = String.format("select id, xx_date from %s order by id", tableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    private static final String UPSERT_WITH_BEFORE_VALUE_DEF =
        "insert into %s(ID, XX_DATE) VALUES(1, '2024-02-27 00:00:00.0') "
            + "on duplicate key update xx_date=xx_date;";

    @Test
    public void pushdownUpsertWithPartialBeforeTest() {
        final String tableName = "t1_pushdown_upsert21";

        rebuildTable(tableName, TABLE_DEF, PARTITION_DEF);

        prepareData(tableName, 3);

        executeAndTraceUpsert(UPSERT_WITH_BEFORE_VALUE_DEF, tableName);

        checkTraceRowCount(1);

        final String sql = String.format("select id, xx_date from %s order by id", tableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void pushdownUpsertWithPartialBeforeTest2() {
        final String tableName = "t1_pushdown_upsert22";

        rebuildTable(tableName, TABLE_DOUBLE_UK_DEF, PARTITION_DEF);

        prepareData(tableName, 3);

        executeAndTraceUpsert(UPSERT_WITH_BEFORE_VALUE_DEF, tableName);

        checkTraceRowCount(1);

        final String sql = String.format("select id, xx_date from %s order by id", tableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    private static final String UPSERT_WITH_BEFORE_AND_AFTER_VALUE_DEF =
        "insert into %s(ID, XX_DATE) VALUES(1, '2024-02-27 00:00:00.0') "
            + "on duplicate key update id=id, xx_date=values(xx_date);";

    @Test
    public void pushdownUpsertWithBeforeAndAfterTest() {
        final String tableName = "t1_pushdown_upsert31";

        rebuildTable(tableName, TABLE_DEF, PARTITION_DEF);

        prepareData(tableName, 3);

        executeAndTraceUpsert(UPSERT_WITH_BEFORE_AND_AFTER_VALUE_DEF, tableName);

        checkTraceRowCount(1);

        final String sql = String.format("select id, xx_date from %s order by id", tableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    private void executeAndTraceUpsert(String sqlTmpl, String tableName) {
        final String upsertSql = String.format(sqlTmpl, tableName);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            upsertSql,
            "trace " + upsertSql,
            null,
            true);
    }

    private void rebuildTable(String tableName, String tableDef, String partitionDef) {
        final String dropTable = String.format(DROP_TABLE_SQL, tableName);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dropTable, null);

        final String createTable = String.format(tableDef, tableName);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            createTable,
            createTable + partitionDef,
            null,
            false);
    }

    private void prepareData(String tableName, int rowCount) {
        final String insert = String.format(INSERT_DEF, tableName);
        final List<List<Object>> params = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            List<Object> param = new ArrayList<>();
            param.add(i);
            param.add(CREATE_TIME.get(i % CREATE_TIME.size()));
            params.add(param);
        }
        DataOperator.executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, params);
    }

    private void checkTraceRowCount(int rowCount) {
        final List<List<String>> traceUpdate = getTrace(tddlConnection);
        Truth.assertWithMessage(Optional
                .ofNullable(traceUpdate)
                .map(tu -> tu.stream().map(r -> String.join(", ", r)).collect(Collectors.joining("\n")))
                .orElse("show trace result is null"))
            .that(traceUpdate)
            .hasSize(rowCount);
    }
}
