package com.alibaba.polardbx.optimizer.core.expression;

import com.alibaba.polardbx.common.mock.MockDataSource;
import com.alibaba.polardbx.common.mock.MockResultSet;
import com.alibaba.polardbx.gms.metadb.table.UserDefinedJavaFunctionRecord;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.google.common.collect.ImmutableMap;
import junit.framework.TestCase;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Collectors;

public class UserDefinedJavaFunctionManagerTest extends TestCase {

    @Test
    public void testComputeDataType() {

         String dataTypes = "TINYINT，SMALLINT，MEDIUMINT，INTEGER，BIGINT，"
             + "DECIMAL，NUMERIC，FLOAT，DOUBLE，"
             + "CHAR，VARCHAR，"
             + "BINARY，VARBINARY，"
             + "BLOB，TEXT，"
             + "ENUM， DATE，DATETIME，TIMESTAMP，TIME，YEAR，JSON";

         String[] types = dataTypes.split("，");
         for (String type : types) {
             UserDefinedJavaFunctionManager.computeDataType(type.trim());
         }
    }

    @Test
    public void testComputeReturnType() {
        String dataTypes = "TINYINT，SMALLINT，MEDIUMINT，INTEGER，BIGINT，"
            + "DECIMAL，NUMERIC，FLOAT，DOUBLE，"
            + "CHAR，VARCHAR，"
            + "BINARY，VARBINARY，"
            + "BLOB，TEXT，"
            + "ENUM， DATE，DATETIME，TIMESTAMP，TIME，YEAR，JSON";

        String[] types = dataTypes.split("，");
        for (String type : types) {
            UserDefinedJavaFunctionManager.computeReturnType(type.trim());
        }

    }

    @Test
    public void testAddFunctionFromMetaAndDropFunction() {
        final String PACKAGE_NAME = "com.alibaba.polardbx.optimizer.core.function.calc.scalar";
        final String CODE_FORMAT = "package %s;\n" +
            "import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;\n" +
            "import com.alibaba.polardbx.optimizer.core.datatype.DataType;\n" +
            "import java.util.List;\n" +
            "%s\n" +
            "public class %s extends UserDefinedJavaFunction {\n" +
            "        public %s(List<DataType> operandTypes, DataType resultType) {\n" +
            "        super(operandTypes, resultType);\n" +
            "    }\n" +
            "@Override\n" +
            "public String[] getFunctionNames() {\n" +
            "    return new String[] {\"%s\"};\n" +
            "}\n" +
            "@Override\n" +
            "%s" +
            "}";
        String code = String.format(CODE_FORMAT, PACKAGE_NAME, "import java.util.Date;", "Addfour", "Addfour", "Addfour".toUpperCase(),
            "public Object compute(Object[] args) {\n"
                + "        int a = Integer.parseInt(args[0].toString());\n"
                + "        int b = Integer.parseInt(args[1].toString());\n"
                + "        return a + b;\n"
                + "    }");
        UserDefinedJavaFunctionRecord record = new UserDefinedJavaFunctionRecord();
        record.funcName = "addfour";
        record.className = "Addfour";
        record.code = code;
        record.codeLanguage = "JAVA";
        record.inputTypes = "bigint, bigint";
        record.resultType = "bigint";
        UserDefinedJavaFunctionManager.addFunctionFromMeta(record);
        Assert.assertEquals(true, UserDefinedJavaFunctionManager.containsFunction("addfour"));
        ReflectiveSqlOperatorTable table = TddlOperatorTable.instance();
        Assert.assertEquals(false, table.getOperatorList().stream().filter(sqlOperator -> sqlOperator.getName().equals("ADDFOUR")).collect(
            Collectors.toList()).isEmpty());

        //drop
        UserDefinedJavaFunctionManager.dropFunction("ADDFOUR");
        Assert.assertEquals(false, UserDefinedJavaFunctionManager.containsFunction("addfour"));
        ReflectiveSqlOperatorTable table2 = TddlOperatorTable.instance();
        Assert.assertEquals(true, table2.getOperatorList().stream().filter(sqlOperator -> sqlOperator.getName().equals("ADDFOUR")).collect(
            Collectors.toList()).isEmpty());
    }
}