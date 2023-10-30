package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.executor.utils.StringUtils;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class JavaUdfTypeTest extends BaseTestCase {
    private static final String DROP_FUNC = "DROP JAVA FUNCTION IF EXISTS %s";
    private static final String CREATE_FUNC = "CREATE JAVA FUNCTION %s no state "
        + "RETURN_TYPE %s INPUT_TYPES %s \n"
        + "CODE \n"
        + "public class %s extends UserDefinedJavaFunction {\n"
        + "  public Object compute(Object[] args) {\n"
        + "    return  ((%s)args[0]);\n"
        + "  }\n"
        + "} END_CODE";
    protected Connection tddlConnection;
    protected Connection checkConnection;

    @Before
    public void init() throws SQLException {
        this.tddlConnection = getPolardbxConnection();
        this.checkConnection = getPolardbxConnection();
    }

    @Test
    public void testLongText() {
        doTest("test_long_text", "LongText", "String");
    }

    @Test
    public void testText() {
        doTest("test_text", "text", "String");
    }

    @Test
    public void testMediumText() {
        doTest("test_medium_text", "MediumText", "String");
    }

    @Test
    public void testTinyText() {
        doTest("test_tiny_text", "tinytext", "String");
    }

    @Test
    public void testChar() {
        doTest("test_char", "char(20)", "String");
    }

    @Test
    public void testVarChar() {
        doTest("test_varchar", "varchar(255)", "String");
    }

    @Test
    public void testBigIntUnsigned() {
        doTest("test_big_int_unsigned", "bigint unsigned", "java.math.BigInteger");
    }

    @Test
    public void testBigInt() {
        doTest("test_big_int", "bigint", "Long");
    }

    @Test
    public void testIntUnsigned() {
        doTest("test_int_unsigned", "int unsigned", "Long");
    }

    @Test
    public void testInt() {
        doTest("test_int", "int", "Integer");
    }

    @Test
    public void testMediumIntUnsigned() {
        doTest("test_medium_int_unsigned", "MediumInt unsigned", "Integer");
    }

    @Test
    public void testMediumInt() {
        doTest("test_medium_int", "MediumInt", "Integer");
    }

    @Test
    public void testSmallIntUnsigned() {
        doTest("test_small_int_unsigned", "SmallInt unsigned", "Integer");
    }

    @Test
    public void testSmallInt() {
        doTest("test_small_int", "SmallInt", "Short");
    }

    @Test
    public void testTinyIntUnsigned() {
        doTest("test_tiny_int_unsigned", "TinyInt unsigned", "Short");
    }

    @Test
    public void testTinyInt() {
        doTest("test_tiny_int", "TinyInt", "Byte");
    }

    @Test
    public void testTinyBlob() {
        doTest("test_tiny_blob", "TinyBlob", "java.sql.Blob");
    }

    @Test
    public void testBlob() {
        doTest("test_blob", "Blob", "java.sql.Blob");
    }

    @Test
    public void testMediumBlob() {
        doTest("test_medium_blob", "MediumBlob", "java.sql.Blob");
    }

    @Test
    public void testLongBlob() {
        doTest("test_long_blob", "LongBlob", "java.sql.Blob");
    }

    @Ignore("binary can not be cast from bigint now")
    public void testBinary() {
        doTest("test_binary", "binary", "byte[]");
    }

    @Ignore("binary can not be cast from bigint now")
    public void testBinary2() {
        doTest("test_binary_2", "binary(10)", "byte[]");
    }

    @Ignore("binary can not be cast from bigint now")
    public void testVarBinary() {
        doTest("test_var_binary", "VarBinary", "byte[]");
    }

    @Ignore("binary can not be cast from bigint now")
    public void testVarBinary2() {
        doTest("test_var_binary_2", "VarBinary(10)", "byte[]");
    }

    @Test
    public void testDate() {
        doTest("test_date", "Date", "java.sql.Date");
    }

    @Test
    public void testTimestamp() {
        doTest("test_timestamp", "Timestamp", "java.sql.Timestamp");
    }

    @Ignore("datetime can not be cast now")
    public void testDatetime() {
        doTest("test_datetime", "Datetime", "java.sql.Timestamp");
    }

    @Test
    public void testTime() {
        doTest("test_time", "Time", "java.sql.Time");
    }

    private void doTest(String funcName, String type, String javaType) {
        dropFunc(funcName);
        createFunc(funcName, type, javaType);
        // simple call use integer
        JdbcUtil.executeSuccess(tddlConnection, String.format("select %s(1)", funcName));
        // simple call use varchar
        JdbcUtil.executeSuccess(tddlConnection, String.format("select %s(\"1\")", funcName));
        dropFunc(funcName);
    }

    private void createFunc(String funcName, String returnType, String javaType) {
        String className = StringUtils.funcNameToClassName(funcName);
        String inputTypes = returnType;
        String sql = String.format(CREATE_FUNC, funcName, inputTypes, returnType, className, javaType);
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    private void dropFunc(String funcName) {
        String sql = String.format(DROP_FUNC, funcName);
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }
}
