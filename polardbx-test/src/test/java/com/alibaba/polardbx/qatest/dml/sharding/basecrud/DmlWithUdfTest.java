package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.collect.Lists;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.BROADCAST_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_ONE_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

@NotThreadSafe
@FileStoreIgnore
public class DmlWithUdfTest extends CrudBasedLockTestCase {

    private static final String PUSH_FUNC_NAME = "test_push_udf_dml";
    private static final String NON_PUSH_FUNC_NAME = "test_non_push_udf_dml";
    private static final String DROP_FUNCTION = "drop function if exists %s";
    private static final String CREATE_FUNCTION = "create function %s(num int) returns int %s return num + 1";
    private static final String CHECK_RESULT = "select * from %s order by pk";

    @Parameterized.Parameters(name = "{index}:{0},{1}")
    public static List<Object[]> getParameters() {
        return cartesianProduct(
            canPush(),
            table());
    }

    public static List<Object[]> cartesianProduct(Object[]... arrays) {
        List[] lists = Arrays.stream(arrays)
            .map(Arrays::asList)
            .toArray(List[]::new);
        List<List<Object>> result = Lists.cartesianProduct(lists);
        return result.stream()
            .map(List::toArray)
            .collect(Collectors.toList());
    }

    public static Object[] table() {
        return new String[] {
            ExecuteTableName.UPDATE_DELETE_BASE + ONE_DB_ONE_TB_SUFFIX,
            ExecuteTableName.UPDATE_DELETE_BASE + ONE_DB_MUTIL_TB_SUFFIX,
            ExecuteTableName.UPDATE_DELETE_BASE + MULTI_DB_ONE_TB_SUFFIX,
            ExecuteTableName.UPDATE_DELETE_BASE + MUlTI_DB_MUTIL_TB_SUFFIX,
            ExecuteTableName.UPDATE_DELETE_BASE + BROADCAST_TB_SUFFIX
        };
    }

    public static Object[] canPush() {
        return new Object[] {
            Boolean.TRUE,
            Boolean.FALSE
        };
    }

    private final String functionName;
    private final boolean canPush;

    public DmlWithUdfTest(Object canPush, Object tableName) {
        this.canPush = (Boolean) canPush;
        this.functionName = (Boolean) canPush ? PUSH_FUNC_NAME : NON_PUSH_FUNC_NAME;
        this.baseOneTableName = tableName.toString();
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        JdbcUtil.executeSuccess(mysqlConnection, String.format(DROP_FUNCTION, functionName));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_FUNCTION, functionName));
        JdbcUtil.executeSuccess(tddlConnection, "set global log_bin_trust_function_creators = on");
        JdbcUtil.executeSuccess(mysqlConnection, "set global log_bin_trust_function_creators = on");
        JdbcUtil.executeSuccess(mysqlConnection,
            String.format(CREATE_FUNCTION, functionName, canPush ? " no sql " : ""));
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(CREATE_FUNCTION, functionName, canPush ? " no sql " : ""));
    }

    @Test
    public void testInsert() {
        String sql =
            String.format("insert into %s (pk, integer_test) values (100, %s(1))", baseOneTableName, functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);

        sql = String.format("insert into %s (pk, integer_test) values (%s(101), %s(2))", baseOneTableName, functionName,
            functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);

        sql = String.format("insert ignore into %s (pk, integer_test) values (100, %s(2))", baseOneTableName,
            functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);

        sql = String.format("replace into %s (pk, integer_test) values (100, %s(3))", baseOneTableName, functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);

        // TODO insert on duplicate key has some problem now
        if (canPush || baseOneTableName.equalsIgnoreCase(ExecuteTableName.UPDATE_DELETE_BASE + BROADCAST_TB_SUFFIX)) {
            sql = String.format(
                "insert into %s (pk, integer_test) values (100, %s(3)) on duplicate key update integer_test = %s(4)",
                baseOneTableName, functionName, functionName);
            JdbcUtil.executeSuccess(mysqlConnection, sql);
            JdbcUtil.executeSuccess(tddlConnection, sql);
            DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
                mysqlConnection);
        }
    }

    @Test
    public void testUpdate() {
        String sql = String.format("insert ignore into %s (pk, integer_test) values (100, %s(1))", baseOneTableName,
            functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);

        sql = String.format("update %s set integer_test = %s(12) where pk = %s(99)", baseOneTableName, functionName,
            functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);

        sql = String.format("update %s set integer_test = %s(13) where pk = %s(199)", baseOneTableName, functionName,
            functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);

        sql = String.format("update %s set pk = %s(199) where integer_test = %s(12)", baseOneTableName, functionName,
            functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);
    }

    @Test
    public void testDelete() {
        String sql = String.format("insert ignore into %s (pk, integer_test) values (100, %s(1))", baseOneTableName,
            functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);

        sql = String.format("delete from %s where pk = %s(99)", baseOneTableName, functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);

        sql = String.format("delete from %s where pk = %s(99)", baseOneTableName, functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);

        sql = String.format("delete from %s where integer_test = %s(1)", baseOneTableName, functionName);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, baseOneTableName), null, tddlConnection,
            mysqlConnection);
    }
}
