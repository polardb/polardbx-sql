package com.alibaba.polardbx.qatest.NotThreadSafe.udf.sql;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;

public class ProcedureFullTypeTest extends BaseTestCase {
    protected Connection tddlConnection;

    private static final String PRIMARY_TABLE_NAME = "procedure_full_type";

    private static final String FULL_TYPE_TABLE =
        ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME,
            DEFAULT_PARTITIONING_DEFINITION);

    private static final String DATA_COLUMN_NO_GEOM =
        "c_bit_1,c_bit_8,c_bit_16,c_bit_32,c_bit_64,c_tinyint_1,c_tinyint_1_un,c_tinyint_4,c_tinyint_4_un,c_tinyint_8,c_tinyint_8_un,c_smallint_1,c_smallint_16,c_smallint_16_un,c_mediumint_1,c_mediumint_24,c_mediumint_24_un,c_int_1,c_int_32,c_int_32_un,c_bigint_1,c_bigint_64,c_bigint_64_un,c_decimal,c_decimal_pr,c_float,c_float_pr,c_float_un,c_double,c_double_pr,c_double_un,c_date,c_datetime,c_datetime_1,c_datetime_3,c_datetime_6,c_timestamp,c_timestamp_1,c_timestamp_3,c_timestamp_6,c_time,c_time_1,c_time_3,c_time_6,c_year,c_year_4,c_char,c_varchar,c_binary,c_varbinary,c_blob_tiny,c_blob,c_blob_medium,c_blob_long,c_text_tiny,c_text,c_text_medium,c_text_long";

    private static final String INSERT_VALUES = "1|2|2|2|2|" +
        "-1|1|-1|1|-1|1|" +
        "-1|-1|1|" +
        "-1|-1|1|" +
        "-1|-1|1|" +
        "-1|-1|1|" +
        "-100| -100.000|" +
        "100.000|100.003|100.003|100.000|100.003|100.003|" +
        "'2017-12-12'|" +
        "'2017-12-12 23:59:59'|'2017-12-12 23:59:59.1'|'2017-12-12 23:59:59.001'|'2017-12-12 23:59:59.000001'|" +
        "'2017-12-12 23:59:59'|'2017-12-12 23:59:59.1'|'2017-12-12 23:59:59.001'|'2017-12-12 23:59:59.000001'|" +
        "'01:01:01'|'01:01:01.1'|'01:01:01.001'|'01:01:01.000001'|" +
        "'2000'|'2000'|" +
        "'11'|'11'|'11'|'11'|" +
        "'11'|'11'|'11'|'11'|" +
        "'11'|'11'|'11'|'11'";

    private static final String
        COLUMN_TYPES =
        "bit(1)|" + "bit(8)|" + "bit(16)|" + "bit(32)|" + "bit(64)|" + "tinyint(1)|" + "tinyint(1) unsigned|"
            + "tinyint(4)|" + "tinyint(4) unsigned|" + "tinyint(8)|" + "tinyint(8) unsigned|" + "smallint(1)|"
            + "smallint(16)|" + "smallint(16) unsigned|" + "mediumint(1)|" + "mediumint(24)|"
            + "mediumint(24) unsigned|" + "int(1)|" + "int(32)|" + "int(32) unsigned|" + "bigint(1)|" + "bigint(64)|"
            + "bigint(64) unsigned|" + "decimal|" + "decimal(65,30)|" + "float|" + "float(10,3)|"
            + "float(10,3) unsigned|" + "double|" + "double(10,3)|" + "double(10,3) unsigned|" + "date|"
            + "   datetime|" + "datetime(1)|" + "datetime(3)|" + "datetime(6)|" + "timestamp|" + "timestamp(1)|"
            + "timestamp(3)|" + " timestamp(6)|" + "time|" + "time(1)|" + "time(3)|" + "time(6)|" + "year|" + "year(4)|"
            + "char(10)|" + "varchar(10) |" + "binary(10) |" + "varbinary(10) |" + "tinyblob|" + "blob|" + "mediumblob|"
            + "longblob|" + "tinytext|" + "text|" + "mediumtext|" + "longtext";
    //        + "enum(\"a\",\"b\",\"c\")|" + "set(\"a\",\"b\",\"c\")|" + "json";
    private static final String FULL_INSERT_NO_GEOM = "insert into `" + PRIMARY_TABLE_NAME + "` " +
        "(`id`," + DATA_COLUMN_NO_GEOM + ") values (\n" +
        "null,\n" +
        "1,2,2,2,2,\n" +
        "-1,1,-1,1,-1,1,\n" +
        "-1,-1,1,\n" +
        "-1,-1,1,\n" +
        "-1,-1,1,\n" +
        "-1,-1,1,\n" +
        "-100, -100.000,\n" +
        "100.000,100.003,100.003,100.000,100.003,100.003,\n" +
        "'2017-12-12',\n" +
        "'2017-12-12 23:59:59','2017-12-12 23:59:59.1','2017-12-12 23:59:59.001','2017-12-12 23:59:59.000001',\n" +
        "'2017-12-12 23:59:59','2017-12-12 23:59:59.1','2017-12-12 23:59:59.001','2017-12-12 23:59:59.000001',\n" +
        "'01:01:01','01:01:01.1','01:01:01.001','01:01:01.000001',\n" +
        "'2000','2000',\n" +
        "'11','11','11','11',\n" +
        "'11','11','11','11',\n" +
        "'11','11','11','11');\n";
//    + "'a','b,a',\n" +
//        "'{\"k1\": \"v1\", \"k2\": 10}');";

    @Before
    public void initData() throws Exception {
        if (usingNewPartDb()) {
            return;
        }
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP FUNCTION IF EXISTS test_full_type;");
        // Create customized full type table.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS `" + PRIMARY_TABLE_NAME + "`");
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_TABLE);
        System.out.println(FULL_TYPE_TABLE);
        // insert data
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_INSERT_NO_GEOM);
        System.out.println(FULL_INSERT_NO_GEOM);
        // create function
        createStroredFunction();
    }

    private void createStroredFunction() {
        String[] columns = DATA_COLUMN_NO_GEOM.split(",");
        List<String> columnTypes = Splitter.on('|').splitToList(COLUMN_TYPES);
        List<String> variables = Arrays.stream(columns).map(t -> "xx_" + t).collect(Collectors.toList());
        List<String> declareVars = new ArrayList<>(variables.size());
        for (int i = 0; i < variables.size(); ++i) {
            declareVars.add(variables.get(i) + " " + columnTypes.get(i));
        }
        String createFunc = "CREATE FUNCTION test_full_type(" + StringUtils.join(declareVars, ",") + ") \n"
            + "RETURNS INT \n"
            + "begin \n"
            + "return select count(*) from " + PRIMARY_TABLE_NAME + "\n"
            + "where " + "(" + StringUtils.join(columns, ",") + ") \n"
            + "in " + "((" + StringUtils.join(variables, ",") + "));\n"
            + "end;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createFunc);
    }

    @After
    public void cleanup() throws Exception {
        if (usingNewPartDb()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS `" + PRIMARY_TABLE_NAME + "`");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP FUNCTION IF EXISTS test_full_type;");
    }

    @Test
    public void testType() throws Exception {
        if (usingNewPartDb()) {
            return;
        }
        List<String> values = Splitter.on('|').splitToList(INSERT_VALUES);
        String SELECT_SQL = String.format("trace select test_full_type(%s) as matched", StringUtils.join(values, ","));
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, SELECT_SQL);
        if (rs.next()) {
            int matched = rs.getInt("matched");
            if (matched != 1) {
                Assert.fail("Expect one row, but got " + matched);
            }
        } else {
            Assert.fail("Expect one row, but got empty!");
        }
    }
}

