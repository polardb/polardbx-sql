/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.constant;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 数据常量
 *
 * @author zhuoxue.yll
 * <p>
 * 2016年10月25日
 */
public class TableConstant {

    //列名常量
    public static final String PK_COLUMN = "pk";
    public static final String ID_COLUMN = "id";
    public static final String NAME_COLUMN = "name";
    public static final String VARCHAR_TEST_COLUMN = "varchar_test";
    public static final String CHAR_TEST_COLUMN = "char_test";
    public static final String BLOB_TEST_COLUMN = "blob_test";
    public static final String INTEGER_TEST_COLUMN = "integer_test";
    public static final String TINYINT_TEST_COLUMN = "tinyint_test";
    public static final String TINYINT_1BIT_TEST_COLUMN = "tinyint_1bit_test";
    public static final String SMALLINT_TEST_COLUMN = "smallint_test";
    public static final String MEDIUMINT_TEST_COLUMN = "mediumint_test";
    public static final String BIT_TEST_COLUMN = "bit_test";
    public static final String BIGINT_TEST_COLUMN = "bigint_test";
    public static final String FLOAT_TEST_COLUMN = "float_test";
    public static final String DOUBLE_TEST_COLUMN = "double_test";
    public static final String DECIMAL_TEST_COLUMN = "decimal_test";
    public static final String DATE_TEST_COLUMN = "date_test";
    public static final String TIME_TEST_COLUMN = "time_test";
    public static final String DATETIME_TEST_COLUMN = "datetime_test";
    public static final String TIMESTAMP_TEST_COLUMN = "timestamp_test";
    public static final String YEAR_TEST_COLUMN = "year_test";

    //TODO
    protected static String[] allColumnParam = {
        PK_COLUMN, ID_COLUMN, NAME_COLUMN, VARCHAR_TEST_COLUMN, CHAR_TEST_COLUMN, BLOB_TEST_COLUMN,
        INTEGER_TEST_COLUMN, TINYINT_TEST_COLUMN, "tinyint_1bit_test", "smallint_test", "mediumint_test", "bit_test",
        "bigint_test", "float_test", "double_test",
        "decimal_test", "date_test", "time_test", "datetime_test", "timestamp_test", "year_test"};

    public static final String C_ID = "id";
    public static final String C_BIT_1 = "c_bit_1";
    public static final String C_BIT_8 = "c_bit_8";
    public static final String C_BIT_16 = "c_bit_16";
    public static final String C_BIT_32 = "c_bit_32";
    public static final String C_BIT_64 = "c_bit_64";
    public static final String C_TINYINT_1 = "c_tinyint_1";
    public static final String C_TINYINT_1_UN = "c_tinyint_1_un";
    public static final String C_TINYINT_4 = "c_tinyint_4";
    public static final String C_TINYINT_4_UN = "c_tinyint_4_un";
    public static final String C_TINYINT_8 = "c_tinyint_8";
    public static final String C_TINYINT_8_UN = "c_tinyint_8_un";
    public static final String C_SMALLINT_1 = "c_smallint_1";
    public static final String C_SMALLINT_16 = "c_smallint_16";
    public static final String C_SMALLINT_16_UN = "c_smallint_16_un";
    public static final String C_MEDIUMINT_1 = "c_mediumint_1";
    public static final String C_MEDIUMINT_24 = "c_mediumint_24";
    public static final String C_MEDIUMINT_24_UN = "c_mediumint_24_un";
    public static final String C_INT_1 = "c_int_1";
    public static final String C_INT_32 = "c_int_32";
    public static final String C_INT_32_UN = "c_int_32_un";
    public static final String C_BIGINT_1 = "c_bigint_1";
    public static final String C_BIGINT_64 = "c_bigint_64";
    public static final String C_BIGINT_64_UN = "c_bigint_64_un";
    public static final String C_DECIMAL = "c_decimal";
    public static final String C_DECIMAL_PR = "c_decimal_pr";
    public static final String C_FLOAT = "c_float";
    public static final String C_FLOAT_PR = "c_float_pr";
    public static final String C_FLOAT_UN = "c_float_un";
    public static final String C_DOUBLE = "c_double";
    public static final String C_DOUBLE_PR = "c_double_pr";
    public static final String C_DOUBLE_UN = "c_double_un";
    public static final String C_DATE = "c_date";
    public static final String C_DATETIME = "c_datetime";
    public static final String C_DATETIME_1 = "c_datetime_1";
    public static final String C_DATETIME_3 = "c_datetime_3";
    public static final String C_DATETIME_6 = "c_datetime_6";
    public static final String C_TIMESTAMP = "c_timestamp";
    public static final String C_TIMESTAMP_1 = "c_timestamp_1";
    public static final String C_TIMESTAMP_3 = "c_timestamp_3";
    public static final String C_TIMESTAMP_6 = "c_timestamp_6";
    public static final String C_TIME = "c_time";
    public static final String C_TIME_1 = "c_time_1";
    public static final String C_TIME_3 = "c_time_3";
    public static final String C_TIME_6 = "c_time_6";
    public static final String C_YEAR = "c_year";
    public static final String C_YEAR_4 = "c_year_4";
    public static final String C_CHAR = "c_char";
    public static final String C_VARCHAR = "c_varchar";
    public static final String C_BINARY = "c_binary";
    public static final String C_VARBINARY = "c_varbinary";
    public static final String C_BLOB_TINY = "c_blob_tiny";
    public static final String C_BLOB = "c_blob";
    public static final String C_BLOB_MEDIUM = "c_blob_medium";
    public static final String C_BLOB_LONG = "c_blob_long";
    public static final String C_TEXT_TINY = "c_text_tiny";
    public static final String C_TEXT = "c_text";
    public static final String C_TEXT_MEDIUM = "c_text_medium";
    public static final String C_TEXT_LONG = "c_text_long";
    public static final String C_ENUM = "c_enum";
    public static final String C_SET = "c_set";
    public static final String C_JSON = "c_json";
    public static final String C_GEOMETORY = "c_geometory";
    public static final String C_POINT = "c_point";
    public static final String C_LINESTRING = "c_linestring";
    public static final String C_POLYGON = "c_polygon";
    public static final String C_MULTIPOINT = "c_multipoint";
    public static final String C_MULTILINESTRING = "c_multilinestring";
    public static final String C_MULTIPOLYGON = "c_multipolygon";
    public static final String C_GEOMETRYCOLLECTION = "c_geometrycollection";

    public static final String T_ID = "bigint(20) NOT NULL AUTO_INCREMENT";
    public static final String T_ID_PLAIN = "bigint(20) DEFAULT NULL";
    public static final String T_BIT_1 = "bit(1) DEFAULT NULL";
    public static final String T_BIT_8 = "bit(8) DEFAULT NULL";
    public static final String T_BIT_16 = "bit(16) DEFAULT NULL";
    public static final String T_BIT_32 = "bit(32) DEFAULT NULL";
    public static final String T_BIT_64 = "bit(64) DEFAULT NULL";
    public static final String T_TINYINT_1 = "tinyint(1) DEFAULT NULL";
    public static final String T_TINYINT_1_UN = "tinyint(1) unsigned DEFAULT NULL";
    public static final String T_TINYINT_4 = "tinyint(4) DEFAULT NULL";
    public static final String T_TINYINT_4_UN = "tinyint(4) unsigned DEFAULT NULL";
    public static final String T_TINYINT_8 = "tinyint(8) DEFAULT NULL";
    public static final String T_TINYINT_8_UN = "tinyint(8) unsigned DEFAULT NULL";
    public static final String T_SMALLINT_1 = "smallint(1) DEFAULT NULL";
    public static final String T_SMALLINT_16 = "smallint(16) DEFAULT NULL";
    public static final String T_SMALLINT_16_UN = "smallint(16) unsigned DEFAULT NULL";
    public static final String T_MEDIUMINT_1 = "mediumint(1) DEFAULT NULL";
    public static final String T_MEDIUMINT_24 = "mediumint(24) DEFAULT NULL";
    public static final String T_MEDIUMINT_24_UN = "mediumint(24) unsigned DEFAULT NULL";
    public static final String T_INT_1 = "int(1) DEFAULT NULL";
    public static final String T_INT_32 = "int(32) DEFAULT NULL";
    public static final String T_INT_32_UN = "int(32) unsigned DEFAULT NULL";
    public static final String T_BIGINT_1 = "bigint(1) DEFAULT NULL";
    public static final String T_BIGINT_64 = "bigint(64) DEFAULT NULL";
    public static final String T_BIGINT_64_UN = "bigint(64) unsigned DEFAULT NULL";
    public static final String T_DECIMAL = "decimal DEFAULT NULL";
    public static final String T_DECIMAL_PR = "decimal(10,3) DEFAULT NULL";
    public static final String T_FLOAT = "float DEFAULT NULL";
    public static final String T_FLOAT_PR = "float(10,3) DEFAULT NULL";
    public static final String T_FLOAT_UN = "float(10,3) unsigned DEFAULT NULL";
    public static final String T_DOUBLE = "double DEFAULT NULL";
    public static final String T_DOUBLE_PR = "double(10,3) DEFAULT NULL";
    public static final String T_DOUBLE_UN = "double(10,3) unsigned DEFAULT NULL";
    public static final String T_DATE = "date DEFAULT NULL COMMENT \"date\"";
    public static final String T_DATETIME = "datetime DEFAULT NULL";
    public static final String T_DATETIME_1 = "datetime(1) DEFAULT NULL";
    public static final String T_DATETIME_3 = "datetime(3) DEFAULT NULL";
    public static final String T_DATETIME_6 = "datetime(6) DEFAULT NULL";
    public static final String T_TIMESTAMP = "timestamp DEFAULT CURRENT_TIMESTAMP";
    public static final String T_TIMESTAMP_1 = "timestamp(1) DEFAULT \"2000-01-01 00:00:00\"";
    public static final String T_TIMESTAMP_3 = "timestamp(3) DEFAULT \"2000-01-01 00:00:00\"";
    public static final String T_TIMESTAMP_6 = "timestamp(6) DEFAULT \"2000-01-01 00:00:00\"";
    public static final String T_TIME = "time DEFAULT NULL";
    public static final String T_TIME_1 = "time(1) DEFAULT NULL";
    public static final String T_TIME_3 = "time(3) DEFAULT NULL";
    public static final String T_TIME_6 = "time(6) DEFAULT NULL";
    public static final String T_YEAR = "year DEFAULT NULL";
    public static final String T_YEAR_4 = "year(4) DEFAULT NULL";
    public static final String T_CHAR = "char(10) DEFAULT NULL";
    public static final String T_VARCHAR = "varchar(10) DEFAULT NULL";
    public static final String T_BINARY = "binary(10) DEFAULT NULL";
    public static final String T_VARBINARY = "varbinary(10) DEFAULT NULL";
    public static final String T_BLOB_TINY = "tinyblob DEFAULT NULL";
    public static final String T_BLOB = "blob DEFAULT NULL";
    public static final String T_BLOB_MEDIUM = "mediumblob DEFAULT NULL";
    public static final String T_BLOB_LONG = "longblob DEFAULT NULL";
    public static final String T_TEXT_TINY = "tinytext DEFAULT NULL";
    public static final String T_TEXT = "text DEFAULT NULL";
    public static final String T_TEXT_MEDIUM = "mediumtext DEFAULT NULL";
    public static final String T_TEXT_LONG = "longtext DEFAULT NULL";
    public static final String T_ENUM = "enum(\"a\",\"b\",\"c\") DEFAULT NULL";
    public static final String T_SET = "set(\"a\",\"b\",\"c\") DEFAULT NULL";
    public static final String T_JSON = "json DEFAULT NULL";
    public static final String T_GEOMETORY = "geometry DEFAULT NULL";
    public static final String T_POINT = "point DEFAULT NULL";
    public static final String T_LINESTRING = "linestring DEFAULT NULL";
    public static final String T_POLYGON = "polygon DEFAULT NULL";
    public static final String T_MULTIPOINT = "multipoint DEFAULT NULL";
    public static final String T_MULTILINESTRING = "multilinestring DEFAULT NULL";
    public static final String T_MULTIPOLYGON = "multipolygon DEFAULT NULL";
    public static final String T_GEOMETRYCOLLECTION = "geometrycollection DEFAULT NULL";

    public static List<String> FULL_TYPE_TABLE_COLUMNS = ImmutableList.of(C_ID,
        C_BIT_1,
        C_BIT_8,
        C_BIT_16,
        C_BIT_32,
        C_BIT_64,
        C_TINYINT_1,
        C_TINYINT_1_UN,
        C_TINYINT_4,
        C_TINYINT_4_UN,
        C_TINYINT_8,
        C_TINYINT_8_UN,
        C_SMALLINT_1,
        C_SMALLINT_16,
        C_SMALLINT_16_UN,
        C_MEDIUMINT_1,
        C_MEDIUMINT_24,
        C_MEDIUMINT_24_UN,
        C_INT_1,
        C_INT_32,
        C_INT_32_UN,
        C_BIGINT_1,
        C_BIGINT_64,
        C_BIGINT_64_UN,
        C_DECIMAL,
        C_DECIMAL_PR,
        C_FLOAT,
        C_FLOAT_PR,
        C_FLOAT_UN,
        C_DOUBLE,
        C_DOUBLE_PR,
        C_DOUBLE_UN,
        C_DATE,
        C_DATETIME,
        C_DATETIME_1,
        C_DATETIME_3,
        C_DATETIME_6,
        C_TIMESTAMP,
        C_TIMESTAMP_1,
        C_TIMESTAMP_3,
        C_TIMESTAMP_6,
        C_TIME,
        C_TIME_1,
        C_TIME_3,
        C_TIME_6,
        C_YEAR,
        C_YEAR_4,
        C_CHAR,
        C_VARCHAR,
        C_BINARY,
        C_VARBINARY,
        C_BLOB_TINY,
        C_BLOB,
        C_BLOB_MEDIUM,
        C_BLOB_LONG,
        C_TEXT_TINY,
        C_TEXT,
        C_TEXT_MEDIUM,
        C_TEXT_LONG,
        C_ENUM,
        C_SET,
        C_JSON,
        C_GEOMETORY,
        C_POINT,
        C_LINESTRING,
        C_POLYGON,
        C_MULTIPOINT,
        C_MULTILINESTRING,
        C_MULTIPOLYGON);

    public static List<String> FULL_TYPE_TABLE_COLUMNS_TYPE_COMMON = ImmutableList.of(
        T_BIT_1,
        T_BIT_8,
        T_BIT_16,
        T_BIT_32,
        T_BIT_64,
        T_TINYINT_1,
        T_TINYINT_1_UN,
        T_TINYINT_4,
        T_TINYINT_4_UN,
        T_TINYINT_8,
        T_TINYINT_8_UN,
        T_SMALLINT_1,
        T_SMALLINT_16,
        T_SMALLINT_16_UN,
        T_MEDIUMINT_1,
        T_MEDIUMINT_24,
        T_MEDIUMINT_24_UN,
        T_INT_1,
        T_INT_32,
        T_INT_32_UN,
        T_BIGINT_1,
        T_BIGINT_64,
        T_BIGINT_64_UN,
        T_DECIMAL,
        T_DECIMAL_PR,
        T_FLOAT,
        T_FLOAT_PR,
        T_FLOAT_UN,
        T_DOUBLE,
        T_DOUBLE_PR,
        T_DOUBLE_UN,
        T_DATE,
        T_DATETIME,
        T_DATETIME_1,
        T_DATETIME_3,
        T_DATETIME_6,
        T_TIMESTAMP,
        T_TIMESTAMP_1,
        T_TIMESTAMP_3,
        T_TIMESTAMP_6,
        T_TIME,
        T_TIME_1,
        T_TIME_3,
        T_TIME_6,
        T_YEAR,
        T_YEAR_4,
        T_CHAR,
        T_VARCHAR,
        T_BINARY,
        T_VARBINARY,
        T_BLOB_TINY,
        T_BLOB,
        T_BLOB_MEDIUM,
        T_BLOB_LONG,
        T_TEXT_TINY,
        T_TEXT,
        T_TEXT_MEDIUM,
        T_TEXT_LONG,
        T_ENUM,
        T_SET,
        T_JSON,
        T_GEOMETORY,
        T_POINT,
        T_LINESTRING,
        T_POLYGON,
        T_MULTIPOINT,
        T_MULTILINESTRING,
        T_MULTIPOLYGON);

    public static List<String> FULL_TYPE_TABLE_COLUMNS_TYPE = ImmutableList.<String>builder()
        .add(T_ID)
        .addAll(FULL_TYPE_TABLE_COLUMNS_TYPE_COMMON)
        .build();
    public static List<String> FULL_TYPE_TABLE_COLUMNS_TYPE_PLAIN = ImmutableList.<String>builder()
        .add(T_ID_PLAIN)
        .addAll(FULL_TYPE_TABLE_COLUMNS_TYPE_COMMON)
        .build();

    public static List<Pair<String, String>> FULL_TYPE_TABLE_COLUMN_DEFS = new ArrayList<>();

    static {
        for (int i = 0; i < TableConstant.FULL_TYPE_TABLE_COLUMNS.size(); ++i) {
            FULL_TYPE_TABLE_COLUMN_DEFS.add(new Pair<>(TableConstant.FULL_TYPE_TABLE_COLUMNS.get(i),
                TableConstant.FULL_TYPE_TABLE_COLUMNS_TYPE_PLAIN.get(i)));
        }
    }

    public static Set<String> dateType = ImmutableSet.of(C_DATE,
        C_DATETIME,
        C_DATETIME_1,
        C_DATETIME_3,
        C_DATETIME_6,
        C_TIMESTAMP,
        C_TIMESTAMP_1,
        C_TIMESTAMP_3,
        C_TIMESTAMP_6);

    public static Set<String> timeType = ImmutableSet.of(
        C_TIME,
        C_TIME_1,
        C_TIME_3,
        C_TIME_6
    );

    public static Set<String> longDataType = ImmutableSet.of(
        C_BLOB_TINY,
        C_BLOB,
        C_BLOB_MEDIUM,
        C_BLOB_LONG,
        C_TEXT_TINY,
        C_TEXT,
        C_TEXT_MEDIUM,
        C_TEXT_LONG);

    public static Set<String> floatType = ImmutableSet.of(
        C_FLOAT,
        C_FLOAT_PR,
        C_FLOAT_UN,
        C_DOUBLE,
        C_DOUBLE_PR,
        C_DOUBLE_UN);

    public static Set<String> decimalType = ImmutableSet.of(
        C_DECIMAL,
        C_DECIMAL_PR);

    public static List<String> PK_COLUMNS = ImmutableList.of(C_ID,
        C_BIT_1,
        C_BIT_8,
        C_BIT_16,
        C_BIT_32,
        C_BIT_64,
        C_TINYINT_1,
        C_TINYINT_1_UN,
        C_TINYINT_4,
        C_TINYINT_4_UN,
        C_TINYINT_8,
        C_TINYINT_8_UN,
        C_SMALLINT_1,
        C_SMALLINT_16,
        C_SMALLINT_16_UN,
        C_MEDIUMINT_1,
        C_MEDIUMINT_24,
        C_MEDIUMINT_24_UN,
        C_INT_1,
        C_INT_32,
        C_INT_32_UN,
        C_BIGINT_1,
        C_BIGINT_64,
        C_BIGINT_64_UN,
        C_DECIMAL,
        C_DECIMAL_PR,
        C_FLOAT,
        C_FLOAT_PR,
        C_FLOAT_UN,
        C_DOUBLE,
        C_DOUBLE_PR,
        C_DOUBLE_UN,
        C_DATE,
        C_DATETIME,
        C_DATETIME_1,
        C_DATETIME_3,
        C_DATETIME_6,
        C_TIMESTAMP,
        C_TIMESTAMP_1,
        C_TIMESTAMP_3,
        C_TIMESTAMP_6,
        C_TIME,
        C_TIME_1,
        C_TIME_3,
        C_TIME_6,
        C_YEAR,
        C_YEAR_4,
        C_CHAR,
        C_VARCHAR,
        C_BINARY,
        C_VARBINARY,
        C_BLOB_TINY,
        C_BLOB,
        C_BLOB_MEDIUM,
        C_BLOB_LONG,
        C_TEXT_TINY,
        C_TEXT,
        C_TEXT_MEDIUM,
        C_TEXT_LONG,
        C_ENUM,
        C_SET);
}
