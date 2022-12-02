package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@Ignore
public class PlParameterTest extends BaseTestCase {
    private static final String DATA_COLUMN_NO_GEOM =
        "c_bit_1,c_bit_8,c_bit_16,c_bit_32,c_bit_64,c_tinyint_1,c_tinyint_1_un,c_tinyint_4,c_tinyint_4_un,c_tinyint_8,c_tinyint_8_un,c_smallint_1,c_smallint_16,c_smallint_16_un,c_mediumint_1,c_mediumint_24,c_mediumint_24_un,c_int_1,c_int_32,c_int_32_un,c_bigint_1,c_bigint_64,c_bigint_64_un,c_decimal,c_decimal_pr,c_float,c_float_pr,c_float_un,c_double,c_double_pr,c_double_un,c_date,c_datetime,c_datetime_1,c_datetime_3,c_datetime_6,c_timestamp,c_timestamp_1,c_timestamp_3,c_timestamp_6,c_time,c_time_1,c_time_3,c_time_6,c_year,c_year_4,c_char,c_varchar,c_binary,c_varbinary,c_blob_tiny,c_blob,c_blob_medium,c_blob_long,c_text_tiny,c_text,c_text_medium,c_text_long,c_enum,c_json";

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
            + "longblob|" + "tinytext|" + "text|" + "mediumtext|" + "longtext|"
            + "enum(\"a\",\"b\",\"c\")|"
//            + "set(\"a\",\"b\",\"c\")|"
            + "json";

    protected Connection mysqlConnection;
    protected Connection tddlConnection;

    @Before
    public void getConnection() {
        this.mysqlConnection = getMysqlConnection();
        this.tddlConnection = getPolardbxConnection();
        dropFunctionIfExists();
        createFunction();
    }

    private void dropFunctionIfExists() {
        String dropFunction = "drop function if exists mysql.test_type_parameter";
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropFunction);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, dropFunction);
    }

    private void createFunction() {
        String[] columns = DATA_COLUMN_NO_GEOM.split(",");
        List<String> columnTypes = Splitter.on('|').splitToList(COLUMN_TYPES);
        List<String> variables = Arrays.stream(columns).map(t -> "xx_" + t).collect(Collectors.toList());
        List<String> declareVars = new ArrayList<>(variables.size());
        for (int i = 0; i < variables.size(); ++i) {
            declareVars.add(variables.get(i) + " " + columnTypes.get(i));
        }
        String createFunc = "CREATE FUNCTION mysql.test_type_parameter(" + StringUtils.join(declareVars, ",") + ") \n"
            + "RETURNS INT \n"
            + "begin \n"
            + "return 1;\n"
            + "end;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createFunc);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createFunc);
    }

    @Test
    public void testTypeParameter() {
        for (int i = 1; i < DATA_COLUMN_NO_GEOM.split(",").length; ++i) {
            selectContentSameAssert(
                "select * from information_schema.parameters where SPECIFIC_NAME like \'%test_type_parameter%\' and ORDINAL_POSITION = " + i,
                null, mysqlConnection, tddlConnection, true);
        }
    }
}
