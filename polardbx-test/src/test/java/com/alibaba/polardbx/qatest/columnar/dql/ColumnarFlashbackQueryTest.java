package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.columnar.dql.FullTypeTest.waitForSync;

@RunWith(Parameterized.class)
public class ColumnarFlashbackQueryTest extends ColumnarReadBaseTestCase {
    private final String sourceTable;
    private final String cciName;
    private final String targetName;
    private static final long UPDATE_OFFSET = 1000000;

    // insert select ENUM 非法值导致报错的已知问题，先忽略
    private static final String NOT_SUPPORTED_ENUM = "(id,c_enum) values(null,'00');";

    // bit(64) 类型的已知问题，先忽略
    private static final String NOT_SUPPORTED_C_BIT_64 = "(id,c_bit_64) values(null,18446744073709551615);";

    private final String partDef;
    private final int cciPartCount;
    private final String columnarHint;

    @Parameterized.Parameters(name = "{index}:partDef={0},cciPartCount={1},number={2},autoPosition={3}")
    public static Object[][] data() {
        return new Object[][] {
            {ExecuteTableSelect.DEFAULT_NEW_PARTITIONING_DEFINITION, 1, 0, true},
            {ExecuteTableSelect.DEFAULT_NEW_PARTITIONING_DEFINITION, 4, 1, true},
            {" single", 1, 2, true},
            {" single", 4, 3, true},
            {ExecuteTableSelect.DEFAULT_NEW_PARTITIONING_DEFINITION, 1, 4, false},
            {ExecuteTableSelect.DEFAULT_NEW_PARTITIONING_DEFINITION, 4, 5, false},
            {" single", 1, 6, false},
            {" single", 4, 7, false},
        };
    }

    public ColumnarFlashbackQueryTest(String partDef, int cciPartCount, int caseNumber, boolean autoPosition) {
        this.partDef = partDef;
        this.cciPartCount = cciPartCount;
        this.sourceTable = "insert_select_source_" + caseNumber;
        this.cciName = sourceTable + "_cci";
        this.targetName = "insert_select_target_" + caseNumber;
        this.columnarHint = autoPosition ? "/*+TDDL: cmd_extra(ENABLE_COLUMNAR_SNAPSHOT_AUTO_POSITION=true)*/" :
            "/*+TDDL: cmd_extra(ENABLE_COLUMNAR_SNAPSHOT_AUTO_POSITION=false)*/";
    }

    @Before
    public void prepareTable() {
        // prepare source table
        JdbcUtil.dropTable(tddlConnection, sourceTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            ExecuteTableSelect.getFullTypeTableDef(sourceTable, partDef));

        for (String sql : GsiConstant.buildGsiFullTypeTestInserts(sourceTable)
            .values().stream().flatMap(List::stream).collect(Collectors.toList())) {
            if (sql.contains(NOT_SUPPORTED_ENUM) || sql.contains(NOT_SUPPORTED_C_BIT_64)) {
                continue;
            }
            JdbcUtil.executeUpdate(tddlConnection, sql, true, true);
        }

        ColumnarUtils.createColumnarIndex(tddlConnection,
            cciName, sourceTable, "id", "id", cciPartCount);

        // prepare csv data
        for (String sql : GsiConstant.buildGsiFullTypeTestInserts(sourceTable)
            .values().stream().flatMap(List::stream).collect(Collectors.toList())) {
            if (sql.contains(NOT_SUPPORTED_ENUM) || sql.contains(NOT_SUPPORTED_C_BIT_64)) {
                continue;
            }
            JdbcUtil.executeUpdate(tddlConnection, sql, true, true);
        }

        // prepare target table
        JdbcUtil.dropTable(tddlConnection, targetName);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            ExecuteTableSelect.getFullTypeTableDef(targetName, partDef));
    }

    @Test
    public void testInsertSelect() throws InterruptedException, SQLException {
        long oldTso = ColumnarUtils.columnarFlushAndGetTso(tddlConnection);
        Assert.assertTrue(oldTso > 0, "Failed to flush columnar snapshot");
        JdbcUtil.executeQuery("begin", tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("update %s set c_bigint_64 = id + %d", sourceTable, UPDATE_OFFSET));
        JdbcUtil.executeQuery("commit", tddlConnection);
        waitForSync(tddlConnection);

        checkFlashbackQueryOldResult(oldTso);

        // insert select from source table for old data
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(this.columnarHint + "insert into %s select * from %s as of tso %d force index(%s)",
                targetName, sourceTable, oldTso, cciName));

        DataValidator.selectContentSameAssertWithDiffSql(
            String.format("select * from %s", targetName),
            String.format(this.columnarHint + "select * from %s as of tso %d force index(%s)", sourceTable, oldTso,
                cciName),
            null, tddlConnection, tddlConnection, false, false, false
        );
    }

    @Test
    public void testReplaceSelect() throws InterruptedException, SQLException {
        long oldTso = ColumnarUtils.columnarFlushAndGetTso(tddlConnection);
        Assert.assertTrue(oldTso > 0, "Failed to flush columnar snapshot");
        JdbcUtil.executeQuery("begin", tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("update %s set c_bigint_64 = id + %d", sourceTable, UPDATE_OFFSET));
        JdbcUtil.executeQuery("commit", tddlConnection);
        waitForSync(tddlConnection);

        checkFlashbackQueryOldResult(oldTso);

        // prepare target table for existed data
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s select * from %s", targetName, sourceTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("update %s set c_bigint_64 = %d", targetName, UPDATE_OFFSET));

        // insert select from source table for old data
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(this.columnarHint + "replace into %s select * from %s as of tso %d force index(%s)",
                targetName, sourceTable, oldTso, cciName));

        DataValidator.selectContentSameAssertWithDiffSql(
            String.format("select * from %s", targetName),
            String.format(this.columnarHint + "select * from %s as of tso %d force index(%s)", sourceTable, oldTso,
                cciName),
            null, tddlConnection, tddlConnection, false, false, false
        );
    }

    @After
    public void dropTable() {
        JdbcUtil.dropTable(tddlConnection, sourceTable);
        JdbcUtil.dropTable(tddlConnection, targetName);
    }

    private void checkFlashbackQueryOldResult(long oldTso) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(
            String.format(this.columnarHint + "select c_bigint_64 from %s as of tso %d force index(%s)", sourceTable,
                oldTso,
                cciName),
            tddlConnection
        );
        while (rs.next()) {
            Assert.assertTrue(rs.getLong("c_bigint_64") < UPDATE_OFFSET);
        }
    }
}
