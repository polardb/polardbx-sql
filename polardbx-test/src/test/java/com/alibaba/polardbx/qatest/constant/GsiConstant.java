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
import com.google.common.collect.ImmutableMap;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIGINT_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIGINT_64;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIGINT_64_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BINARY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIT_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIT_16;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIT_32;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIT_64;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIT_8;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB_LONG;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB_MEDIUM;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB_TINY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_CHAR;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DATE;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DATETIME;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DATETIME_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DATETIME_3;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DATETIME_6;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DECIMAL;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DECIMAL_PR;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DOUBLE;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DOUBLE_PR;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DOUBLE_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ENUM;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_FLOAT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_FLOAT_PR;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_FLOAT_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_GEOMETORY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_GEOMETRYCOLLECTION;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_INT_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_INT_32;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_INT_32_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_JSON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_LINESTRING;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MEDIUMINT_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MEDIUMINT_24;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MEDIUMINT_24_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTILINESTRING;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTIPOINT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTIPOLYGON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_POINT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_POLYGON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_SET;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_SMALLINT_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_SMALLINT_16;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_SMALLINT_16_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT_LONG;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT_MEDIUM;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT_TINY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIME;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP_3;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP_6;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIME_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIME_3;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIME_6;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TINYINT_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TINYINT_1_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TINYINT_4;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TINYINT_4_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TINYINT_8;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TINYINT_8_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_VARBINARY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_VARCHAR;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_YEAR;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_YEAR_4;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.constant.TableConstant.dateType;

/**
 * @author chenmo.cm
 */
public class GsiConstant {

    private static final String CREATE_GSI_TEMPLATE = "CREATE GLOBAL INDEX {0} ON {1}({2}) COVERING({3}) {4}";
    private static final String ADD_GSI_TEMPLATE = "ALTER TABLE {0} ADD GLOBAL INDEX {1}({2}) COVERING({3}) {4}";
    private static final String HASH_PARTITIONING_TEMPLATE =
        "DBPARTITION BY HASH({0}) TBPARTITION BY HASH({0}) TBPARTITIONS 7";
    private static final String YYYYMM_OPT_PARTITIONING_TEMPLATE =
        "DBPARTITION BY YYYYMM_OPT({0}) TBPARTITION BY YYYYMM_OPT({0}) TBPARTITIONS 7";

    public static ImmutableMap<String, List<String>> buildGsiFullTypeTestInserts(String primaryTableName) {
        final ImmutableMap.Builder<String, List<String>> builder = ImmutableMap.builder();

        builder.put(C_ID,
            ImmutableList.of("insert into " + primaryTableName + "(id) values(null);\n",
                "insert into " + primaryTableName + "(id) values(null);\n",
                "insert into " + primaryTableName + "(id) values(null);\n"));
        builder.put(C_BIT_1,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_bit_1) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_bit_1) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_bit_1) values(null,2);\n"));
        builder.put(C_BIT_8,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_bit_8) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_bit_8) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_bit_8) values(null,2);\n",
                "insert into " + primaryTableName + "(id,c_bit_8) values(null,256);\n"));
        builder.put(C_BIT_16,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_bit_16) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_bit_16) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_bit_16) values(null,2);\n",
                "insert into " + primaryTableName + "(id,c_bit_16) values(null,65535);\n"));
        builder.put(C_BIT_32,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_bit_32) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_bit_32) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_bit_32) values(null,2);\n",
                "insert into " + primaryTableName + "(id,c_bit_32) values(null,4294967296);\n"));
        builder.put(C_BIT_64,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_bit_64) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_bit_64) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_bit_64) values(null,2);\n",
                "insert into " + primaryTableName + "(id,c_bit_64) values(null,18446744073709551615);\n"));
        builder.put(C_TINYINT_1,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_tinyint_1) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_1) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_1) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_1) values(null,127);\n"));
        builder.put(C_TINYINT_1_UN,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_tinyint_1_un) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_1_un) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_1_un) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_1_un) values(null,127);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_1_un) values(null,255);\n"));
        builder.put(C_TINYINT_4,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_tinyint_4) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_4) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_4) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_4) values(null,127);\n"));
        builder.put(C_TINYINT_4_UN,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_tinyint_4_un) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_4_un) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_4_un) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_4_un) values(null,127);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_4_un) values(null,255);\n"));
        builder.put(C_TINYINT_8,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_tinyint_8) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_8) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_8) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_8) values(null,127);\n"));
        builder.put(C_TINYINT_8_UN,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_tinyint_8_un) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_8_un) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_8_un) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_8_un) values(null,127);\n",
                "insert into " + primaryTableName + "(id,c_tinyint_8_un) values(null,255);\n"));
        builder.put(C_SMALLINT_1,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_smallint_1) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_smallint_1) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_smallint_1) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_smallint_1) values(null,65535);\n"));
        builder.put(C_SMALLINT_16,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_smallint_16) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_smallint_16) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_smallint_16) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_smallint_16) values(null,65535);\n"));
        builder.put(C_SMALLINT_16_UN,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_smallint_16_un) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_smallint_16_un) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_smallint_16_un) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_smallint_16_un) values(null,65535);\n"));
        builder.put(C_MEDIUMINT_1,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_mediumint_1) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_mediumint_1) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_mediumint_1) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_mediumint_1) values(null,16777215);\n"));
        builder.put(C_MEDIUMINT_24,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_mediumint_24) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_mediumint_24) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_mediumint_24) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_mediumint_24) values(null,16777215);\n"));
        builder.put(C_MEDIUMINT_24_UN,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_mediumint_24_un) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_mediumint_24_un) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_mediumint_24_un) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_mediumint_24_un) values(null,16777215);\n"));
        builder.put(C_INT_1,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_int_1) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_int_1) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_int_1) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_int_1) values(null,4294967295);\n"));
        builder.put(C_INT_32,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_int_32) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_int_32) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_int_32) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_int_32) values(null,4294967295);\n"));
        builder.put(C_INT_32_UN,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_int_32_un) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_int_32_un) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_int_32_un) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_int_32_un) values(null,4294967295);\n"));
        builder.put(C_BIGINT_1,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_bigint_1) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_bigint_1) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_bigint_1) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_bigint_1) values(null,18446744073709551615);\n"));
        builder.put(C_BIGINT_64,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_bigint_64) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_bigint_64) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_bigint_64) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_bigint_64) values(null,18446744073709551615);\n"));
        builder.put(C_BIGINT_64_UN,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_bigint_64_un) values(null,-1);\n",
                "insert into " + primaryTableName + "(id,c_bigint_64_un) values(null,0);\n",
                "insert into " + primaryTableName + "(id,c_bigint_64_un) values(null,1);\n",
                "insert into " + primaryTableName + "(id,c_bigint_64_un) values(null,18446744073709551615);\n"));
        builder.put(C_DECIMAL,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_decimal) values(null,'100.000');\n",
                "insert into " + primaryTableName + "(id,c_decimal) values(null,'100.003');\n",
                "insert into " + primaryTableName + "(id,c_decimal) values(null,'-100.003');\n",
                "insert into " + primaryTableName + "(id,c_decimal) values(null,'-100.0000001');\n"));
        builder.put(C_DECIMAL_PR,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_decimal_pr) values(null,'100.000');\n",
                "insert into " + primaryTableName + "(id,c_decimal_pr) values(null,'100.003');\n",
                "insert into " + primaryTableName + "(id,c_decimal_pr) values(null,'-100.003');\n",
                "insert into " + primaryTableName + "(id,c_decimal_pr) values(null,'-100.0000001');\n",
                "insert into " + primaryTableName
                    + "(id,c_decimal_pr) values(null,'5.576856765031534000000000000000');\n"));
        builder.put(C_FLOAT,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_float) values(null,'100.000');\n",
                "insert into " + primaryTableName + "(id,c_float) values(null,'100.003');\n",
                "insert into " + primaryTableName + "(id,c_float) values(null,'-100.003');\n",
                "insert into " + primaryTableName + "(id,c_float) values(null,'-100.0000001');\n"));
        builder.put(C_FLOAT_PR,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_float_pr) values(null,'100.000');\n",
                "insert into " + primaryTableName + "(id,c_float_pr) values(null,'100.003');\n",
                "insert into " + primaryTableName + "(id,c_float_pr) values(null,'-100.003');\n",
                "insert into " + primaryTableName + "(id,c_float_pr) values(null,'-100.0000001');\n"));
        builder.put(C_FLOAT_UN,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_float_un) values(null,'100.000');\n",
                "insert into " + primaryTableName + "(id,c_float_un) values(null,'100.003');\n",
                "insert into " + primaryTableName + "(id,c_float_un) values(null,'-100.003');\n",
                "insert into " + primaryTableName + "(id,c_float_un) values(null,'-100.0000001');\n"));
        builder.put(C_DOUBLE,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_double) values(null,'100.000');\n",
                "insert into " + primaryTableName + "(id,c_double) values(null,'100.003');\n",
                "insert into " + primaryTableName + "(id,c_double) values(null,'-100.003');\n",
                "insert into " + primaryTableName + "(id,c_double) values(null,'-100.0000001');\n"));
        builder.put(C_DOUBLE_PR,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_double_pr) values(null,'100.000');\n",
                "insert into " + primaryTableName + "(id,c_double_pr) values(null,'100.003');\n",
                "insert into " + primaryTableName + "(id,c_double_pr) values(null,'-100.003');\n",
                "insert into " + primaryTableName + "(id,c_double_pr) values(null,'-100.0000001');\n"));
        builder.put(C_DOUBLE_UN,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_double_un) values(null,'100.000');\n",
                "insert into " + primaryTableName + "(id,c_double_un) values(null,'100.003');\n",
                "insert into " + primaryTableName + "(id,c_double_un) values(null,'-100.003');\n",
                "insert into " + primaryTableName + "(id,c_double_un) values(null,'-100.0000001');\n"));
        builder.put(C_DATE,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_date) values(null,'0000-00-00');\n",
                "insert into " + primaryTableName + "(id,c_date) values(null,'9999-12-31');\n",
                "insert into " + primaryTableName + "(id,c_date) values(null,'0000-00-00 01:01:01');\n",
                "insert into " + primaryTableName + "(id,c_date) values(null,'1969-09-00');\n",
                "insert into " + primaryTableName + "(id,c_date) values(null,'2018-00-00');\n",
                "insert into " + primaryTableName + "(id,c_date) values(null,'2017-12-12');\n"));
        builder.put(C_DATETIME,
            ImmutableList.of(
                "insert into " + primaryTableName + "(id,c_datetime) values(null,'0000-00-00 00:00:00');\n",
                "insert into " + primaryTableName + "(id,c_datetime) values(null,'9999-12-31 23:59:59');\n",
                "insert into " + primaryTableName + "(id,c_datetime) values(null,'0000-00-00 01:01:01');\n",
                "insert into " + primaryTableName + "(id,c_datetime) values(null,'1969-09-00 23:59:59');\n",
                "insert into " + primaryTableName + "(id,c_datetime) values(null,'2018-00-00 00:00:00');\n",
                "insert into " + primaryTableName + "(id,c_datetime) values(null,'2017-12-12 23:59:59');\n"));
        builder.put(C_DATETIME_1,
            ImmutableList.of(
                "insert into " + primaryTableName + "(id,c_datetime_1) values(null,'0000-00-00 00:00:00.0');\n",
                "insert into " + primaryTableName + "(id,c_datetime_1) values(null,'9999-12-31 23:59:59.9');\n",
                "insert into " + primaryTableName + "(id,c_datetime_1) values(null,'0000-00-00 01:01:01.12');\n",
                "insert into " + primaryTableName + "(id,c_datetime_1) values(null,'1969-09-00 23:59:59.06');\n",
                "insert into " + primaryTableName + "(id,c_datetime_1) values(null,'2018-00-00 00:00:00.04');\n",
                "insert into " + primaryTableName + "(id,c_datetime_1) values(null,'2017-12-12 23:59:59.045');\n"));
        builder.put(C_DATETIME_3,
            ImmutableList.of(
                "insert into " + primaryTableName + "(id,c_datetime_3) values(null,'0000-00-00 00:00:00.000');\n",
                "insert into " + primaryTableName + "(id,c_datetime_3) values(null,'9999-12-31 23:59:59.999');\n",
                "insert into " + primaryTableName + "(id,c_datetime_3) values(null,'0000-00-00 01:01:01.121');\n",
                "insert into " + primaryTableName + "(id,c_datetime_3) values(null,'1969-09-00 23:59:59.0006');\n",
                "insert into " + primaryTableName + "(id,c_datetime_3) values(null,'2018-00-00 00:00:00.0004');\n",
                "insert into " + primaryTableName + "(id,c_datetime_3) values(null,'2017-12-12 23:59:59.00045');\n"));
        builder.put(C_DATETIME_6,
            ImmutableList.of(
                "insert into " + primaryTableName + "(id,c_datetime_6) values(null,'0000-00-00 00:00:00.000000');\n",
                "insert into " + primaryTableName + "(id,c_datetime_6) values(null,'9999-12-31 23:59:59.999999');\n",
                "insert into " + primaryTableName + "(id,c_datetime_6) values(null,'0000-00-00 01:01:01.121121');\n",
                "insert into " + primaryTableName + "(id,c_datetime_6) values(null,'1969-09-00 23:59:59.0000006');\n",
                "insert into " + primaryTableName + "(id,c_datetime_6) values(null,'2018-00-00 00:00:00.0000004');\n",
                "insert into " + primaryTableName
                    + "(id,c_datetime_6) values(null,'2017-12-12 23:59:59.00000045');\n"));
        builder.put(C_TIMESTAMP,
            ImmutableList.of(
                "insert into " + primaryTableName + "(id,c_timestamp) values(null,'0000-00-00 00:00:00');\n",
                "insert into " + primaryTableName + "(id,c_timestamp) values(null,'9999-12-31 23:59:59');\n",
                "insert into " + primaryTableName + "(id,c_timestamp) values(null,'0000-00-00 01:01:01');\n",
                "insert into " + primaryTableName + "(id,c_timestamp) values(null,'1969-09-00 23:59:59');\n",
                "insert into " + primaryTableName + "(id,c_timestamp) values(null,'2018-00-00 00:00:00');\n",
                "insert into " + primaryTableName + "(id,c_timestamp) values(null,'2017-12-12 23:59:59');\n"));
        builder.put(C_TIMESTAMP_1,
            ImmutableList.of(
                "insert into " + primaryTableName + "(id,c_timestamp_1) values(null,'0000-00-00 00:00:00.0');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_1) values(null,'9999-12-31 23:59:59.9');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_1) values(null,'0000-00-00 01:01:01.12');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_1) values(null,'1969-09-00 23:59:59.06');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_1) values(null,'2018-00-00 00:00:00.04');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_1) values(null,'2017-12-12 23:59:59.045');\n"));
        builder.put(C_TIMESTAMP_3,
            ImmutableList.of(
                "insert into " + primaryTableName + "(id,c_timestamp_3) values(null,'0000-00-00 00:00:00.000');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_3) values(null,'9999-12-31 23:59:59.999');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_3) values(null,'0000-00-00 01:01:01.121');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_3) values(null,'1969-09-00 23:59:59.0006');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_3) values(null,'2018-00-00 00:00:00.0004');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_3) values(null,'2017-12-12 23:59:59.00045');\n"));
        builder.put(C_TIMESTAMP_6,
            ImmutableList.of(
                "insert into " + primaryTableName + "(id,c_timestamp_6) values(null,'0000-00-00 00:00:00.000000');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_6) values(null,'9999-12-31 23:59:59.999999');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_6) values(null,'0000-00-00 01:01:01.121121');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_6) values(null,'1969-09-00 23:59:59.0000006');\n",
                "insert into " + primaryTableName + "(id,c_timestamp_6) values(null,'2018-00-00 00:00:00.0000004');\n",
                "insert into " + primaryTableName
                    + "(id,c_timestamp_6) values(null,'2017-12-12 23:59:59.00000045');\n"));
        builder.put(C_TIME,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_time) values(null,'-838:59:59');\n",
                "insert into " + primaryTableName + "(id,c_time) values(null,'838:59:59');\n",
                "insert into " + primaryTableName + "(id,c_time) values(null,'00:00:00');\n",
                "insert into " + primaryTableName + "(id,c_time) values(null,'01:01:01');\n",
                "insert into " + primaryTableName + "(id,c_time) values(null,'-01:01:01');\n",
                "insert into " + primaryTableName + "(id,c_time) values(null,'23:59:59');\n"));
        builder.put(C_TIME_1,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_time_1) values(null,'-838:59:59.9');\n",
                "insert into " + primaryTableName + "(id,c_time_1) values(null,'838:59:59.9');\n",
                "insert into " + primaryTableName + "(id,c_time_1) values(null,'00:00:00.1');\n",
                "insert into " + primaryTableName + "(id,c_time_1) values(null,'01:01:01.6');\n",
                "insert into " + primaryTableName + "(id,c_time_1) values(null,'-01:01:01.4');\n",
                "insert into " + primaryTableName + "(id,c_time_1) values(null,'23:59:59.45');\n"));
        builder.put(C_TIME_3,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_time_3) values(null,'-838:59:59.999');\n",
                "insert into " + primaryTableName + "(id,c_time_3) values(null,'838:59:59.999');\n",
                "insert into " + primaryTableName + "(id,c_time_3) values(null,'00:00:00.111');\n",
                "insert into " + primaryTableName + "(id,c_time_3) values(null,'01:01:01.106');\n",
                "insert into " + primaryTableName + "(id,c_time_3) values(null,'-01:01:01.0004');\n",
                "insert into " + primaryTableName + "(id,c_time_3) values(null,'23:59:59.00045');\n"));
        builder.put(C_TIME_6,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_time_6) values(null,'-838:59:59.999999');\n",
                "insert into " + primaryTableName + "(id,c_time_6) values(null,'838:59:59.999999');\n",
                "insert into " + primaryTableName + "(id,c_time_6) values(null,'00:00:00.111111');\n",
                "insert into " + primaryTableName + "(id,c_time_6) values(null,'01:01:01.106106');\n",
                "insert into " + primaryTableName + "(id,c_time_6) values(null,'-01:01:01.0000004');\n",
                "insert into " + primaryTableName + "(id,c_time_6) values(null,'23:59:59.00000045');\n"));
        builder.put(C_YEAR,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_year) values(null,'0000');\n",
                "insert into " + primaryTableName + "(id,c_year) values(null,'9999');\n",
                "insert into " + primaryTableName + "(id,c_year) values(null,'1970');\n",
                "insert into " + primaryTableName + "(id,c_year) values(null,'2000');\n"));
        builder.put(C_YEAR_4,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_year_4) values(null,'0000');\n",
                "insert into " + primaryTableName + "(id,c_year_4) values(null,'9999');\n",
                "insert into " + primaryTableName + "(id,c_year_4) values(null,'1970');\n",
                "insert into " + primaryTableName + "(id,c_year_4) values(null,'2000');\n"));
        builder.put(C_CHAR,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_char) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_char) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_char) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_char) values(null,x'313233616263');\n"));
        builder.put(C_VARCHAR,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_varchar) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_varchar) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_varchar) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_varchar) values(null,x'313233616263');\n"));
        builder.put(C_BINARY,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_binary) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_binary) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_binary) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_binary) values(null,x'0A08080E10011894AB0E');\n"));
        builder.put(C_VARBINARY,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_varbinary) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_varbinary) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_varbinary) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_varbinary) values(null,x'0A08080E10011894AB0E');\n"));
        builder.put(C_BLOB_TINY,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_blob_tiny) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_blob_tiny) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_blob_tiny) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_blob_tiny) values(null,x'0A08080E10011894AB0E');\n"));
        builder.put(C_BLOB,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_blob) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_blob) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_blob) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_blob) values(null,x'0A08080E10011894AB0E');\n"));
        builder.put(C_BLOB_MEDIUM,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_blob_medium) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_blob_medium) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_blob_medium) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_blob_medium) values(null,x'0A08080E10011894AB0E');\n"));
        builder.put(C_BLOB_LONG,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_blob_long) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_blob_long) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_blob_long) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_blob_long) values(null,x'0A08080E10011894AB0E');\n"));
        builder.put(C_TEXT_TINY,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_text_tiny) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_text_tiny) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_text_tiny) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_text_tiny) values(null,x'313233616263');\n"));
        builder.put(C_TEXT,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_text) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_text) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_text) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_text) values(null,x'313233616263');\n"));
        builder.put(C_TEXT_MEDIUM,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_text_medium) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_text_medium) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_text_medium) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_text_medium) values(null,x'313233616263');\n"));
        builder.put(C_TEXT_LONG,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_text_long) values(null,'11');\n",
                "insert into " + primaryTableName + "(id,c_text_long) values(null,'99');\n",
                "insert into " + primaryTableName + "(id,c_text_long) values(null,'a中国a');\n",
                "insert into " + primaryTableName + "(id,c_text_long) values(null,x'313233616263');\n"));
        builder.put(C_ENUM,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_enum) values(null,'a');\n",
                "insert into " + primaryTableName + "(id,c_enum) values(null,'b');\n",
                "insert into " + primaryTableName + "(id,c_enum) values(null,NULL);\n"));
        builder.put(C_SET,
            ImmutableList.of("insert into " + primaryTableName + "(id,c_set) values(null,'a');\n",
                "insert into " + primaryTableName + "(id,c_set) values(null,'b,a');\n",
                "insert into " + primaryTableName + "(id,c_set) values(null,'b,c,a');\n",
                "insert into " + primaryTableName + "(id,c_set) values(null,'d');\n",
                "insert into " + primaryTableName + "(id,c_set) values(null,NULL);\n"));
        builder.put(C_JSON,
            ImmutableList.of(
                "insert into " + primaryTableName + "(id,c_json) values(null,'{\"k1\": \"v1\", \"k2\": 10}');\n",
                "insert into " + primaryTableName + "(id,c_json) values(null,'{\"k1\": \"v1\", \"k2\": [10, 20]}');\n",
                "insert into " + primaryTableName + "(id,c_json) values(null,NULL);\n"));
        builder.put(C_POINT,
            ImmutableList.of(
                "insert into " + primaryTableName + "(id,c_point) VALUE (null,ST_POINTFROMTEXT('POINT(15 20)'));\n"));
        builder.put(C_LINESTRING,
            ImmutableList
                .of("insert into " + primaryTableName
                    + "(id,c_linestring) VALUE (null,ST_GEOMFROMTEXT('LINESTRING(0 0, 10 10, 20 25, 50 60)'));\n"));
        builder.put(C_POLYGON,
            ImmutableList
                .of("insert into " + primaryTableName
                    + "(id,c_polygon) VALUE (null,ST_GEOMFROMTEXT('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))'));\n"));
        builder.put(C_GEOMETORY,
            ImmutableList.of("insert into " + primaryTableName
                + "(id,c_geometory) VALUE (null,ST_POINTFROMTEXT('POINT(15 20)'));\n"));
        builder.put(C_MULTIPOINT,
            ImmutableList.of(
                "insert into " + primaryTableName
                    + "(id,c_multipoint) VALUE (null,ST_GEOMFROMTEXT('MULTIPOINT(0 0, 15 25, 45 65)'));\n"));
        builder.put(C_MULTILINESTRING,
            ImmutableList
                .of("insert into " + primaryTableName
                    + "(id,c_multilinestring) VALUE (null,ST_GEOMFROMTEXT('MULTILINESTRING((10 10, 20 20), (15 15, 30 15))'));\n"));
        builder.put(C_MULTIPOLYGON,
            ImmutableList
                .of("insert into " + primaryTableName
                    + "(id,c_multipolygon) VALUE (null,ST_GEOMFROMTEXT('MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))'));\n"));
        builder.put(C_GEOMETRYCOLLECTION,
            ImmutableList.of("insert into " + primaryTableName
                + "(id,c_geometrycollection) VALUE (null,ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))'));\n"));

        return builder.build();
    }

    /**
     * Build full type insert with sharding key of index table
     *
     * @param sk sharding key of index table
     * @return insert sql list
     */
    public static List<String> getInsertWithShardKey(String PRIMARY_TABLE_NAME, String pk, String sk) {
        ImmutableMap<String, List<String>> base = ImmutableMap.<String, List<String>>builder()
            .put(C_ID, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ") values(null               , 0  );\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ") values(null               , 1  );\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ") values(null               , 2  );\n "))
            .put(C_BIT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_1) values(null            , 4  , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_1) values(null            , 5  , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_1) values(null            , 6  , 2);\n "))
            .put(C_BIT_8, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_8) values(null            , 8  , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_8) values(null            , 9  , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_8) values(null            , 10 , 2);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_bit_8) values(null            , 11 , 256);\n "))
            .put(C_BIT_16, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_16) values(null           , 13 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_16) values(null           , 14 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_16) values(null           , 15 , 2);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_bit_16) values(null           , 16 , 65535);\n "))
            .put(C_BIT_32, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_32) values(null           , 18 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_32) values(null           , 19 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_32) values(null           , 20 , 2);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_bit_32) values(null           , 21 , 4294967296);\n "))
            .put(C_BIT_64, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_64) values(null           , 23 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_64) values(null           , 24 , 1);\n ",
                // unsupported by drds
                //"insert into "+PRIMARY_TABLE_NAME+"(id, " + sk + ", c_bit_64) values(null           , 26 , 18446744073709551615);\n "                                                       ,
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_64) values(null           , 25 , 2);\n "))
            .put(C_TINYINT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1) values(null        , 28 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1) values(null        , 29 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1) values(null        , 30 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_tinyint_1) values(null        , 31 , 127);\n "))
            .put(C_TINYINT_1_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1_un) values(null     , 33 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1_un) values(null     , 34 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1_un) values(null     , 35 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_tinyint_1_un) values(null     , 36 , 127);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_tinyint_1_un) values(null     , 37 , 255);\n "))
            .put(C_TINYINT_4, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4) values(null        , 39 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4) values(null        , 40 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4) values(null        , 41 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_tinyint_4) values(null        , 42 , 127);\n "))
            .put(C_TINYINT_4_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4_un) values(null     , 44 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4_un) values(null     , 45 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4_un) values(null     , 46 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_tinyint_4_un) values(null     , 47 , 127);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_tinyint_4_un) values(null     , 48 , 255);\n "))
            .put(C_TINYINT_8, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8) values(null        , 50 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8) values(null        , 51 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8) values(null        , 52 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_tinyint_8) values(null        , 53 , 127);\n "))
            .put(C_TINYINT_8_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8_un) values(null     , 55 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8_un) values(null     , 56 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8_un) values(null     , 57 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_tinyint_8_un) values(null     , 58 , 127);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_tinyint_8_un) values(null     , 59 , 255);\n "))
            .put(C_SMALLINT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_1) values(null       , 61 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_1) values(null       , 62 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_1) values(null       , 63 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_smallint_1) values(null       , 64 , 65535);\n "))
            .put(C_SMALLINT_16, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16) values(null      , 66 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16) values(null      , 67 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16) values(null      , 68 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_smallint_16) values(null      , 69 , 65535);\n "))
            .put(C_SMALLINT_16_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16_un) values(null   , 71 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16_un) values(null   , 72 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16_un) values(null   , 73 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_smallint_16_un) values(null   , 74 , 65535);\n "))
            .put(C_MEDIUMINT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_1) values(null      , 76 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_1) values(null      , 77 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_1) values(null      , 78 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_mediumint_1) values(null      , 79 , 16777215);\n "))
            .put(C_MEDIUMINT_24, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24) values(null     , 81 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24) values(null     , 82 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24) values(null     , 83 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_mediumint_24) values(null     , 84 , 16777215);\n "))
            .put(C_MEDIUMINT_24_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24_un) values(null  , 86 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24_un) values(null  , 87 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24_un) values(null  , 88 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_mediumint_24_un) values(null  , 89 , 16777215);\n "))
            .put(C_INT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_1) values(null            , 91 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_1) values(null            , 92 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_1) values(null            , 93 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_int_1) values(null            , 94 , 4294967295);\n "))
            .put(C_INT_32, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32) values(null           , 96 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32) values(null           , 97 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32) values(null           , 98 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_int_32) values(null           , 99 , 4294967295);\n "))
            .put(C_INT_32_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32_un) values(null        , 101, -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32_un) values(null        , 102, 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32_un) values(null        , 103, 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_int_32_un) values(null        , 104, 4294967295);\n "))
            .put(C_BIGINT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_1) values(null         , 106, -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_1) values(null         , 107, 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_1) values(null         , 108, 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_bigint_1) values(null         , 109, 18446744073709551615);\n "))
            .put(C_BIGINT_64, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64) values(null        , 111, -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64) values(null        , 112, 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64) values(null        , 113, 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_bigint_64) values(null        , 114, 18446744073709551615);\n "))
            .put(C_BIGINT_64_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64_un) values(null     , 116, -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64_un) values(null     , 117, 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64_un) values(null     , 118, 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_bigint_64_un) values(null     , 119, 18446744073709551615);\n "))
            .put(C_DECIMAL, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_decimal) values(null          , 121, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_decimal) values(null          , 122, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_decimal) values(null          , 123, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_decimal) values(null          , 124, '-100.0000001');\n "))
            .put(C_DECIMAL_PR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_decimal_pr) values(null       , 126, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_decimal_pr) values(null       , 127, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_decimal_pr) values(null       , 128, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_decimal_pr) values(null       , 129, '-100.0000001');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_decimal_pr) values(null       , 129, '5.576856765031534000000000000000');\n "))
            .put(C_FLOAT, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float) values(null            , 131, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float) values(null            , 132, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float) values(null            , 133, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float) values(null            , 134, '-100.0000001');\n "))
            .put(C_FLOAT_PR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float_pr) values(null         , 136, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float_pr) values(null         , 137, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float_pr) values(null         , 138, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float_pr) values(null         , 139, '-100.0000001');\n "))
            .put(C_FLOAT_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float_un) values(null         , 141, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float_un) values(null         , 142, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float_un) values(null         , 143, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_float_un) values(null         , 144, '-100.0000001');\n "))
            .put(C_DOUBLE, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double) values(null           , 146, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double) values(null           , 147, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double) values(null           , 148, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double) values(null           , 149, '-100.0000001');\n "))
            .put(C_DOUBLE_PR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double_pr) values(null        , 151, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double_pr) values(null        , 152, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double_pr) values(null        , 153, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double_pr) values(null        , 154, '-100.0000001');\n "))
            .put(C_DOUBLE_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double_un) values(null        , 156, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double_un) values(null        , 157, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double_un) values(null        , 158, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_double_un) values(null        , 159, '-100.0000001');\n "))
            .put(C_DATE, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_date) values(null             , 161, '0000-00-00');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_date) values(null             , 162, '9999-12-31');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_date) values(null             , 163, '0000-00-00 01:01:01');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_date) values(null             , 164, '1969-09-00');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_date) values(null             , 165, '2018-00-00');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_date) values(null             , 166, '2017-12-12');\n "))
            .put(C_DATETIME, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime) values(null         , 168, '0000-00-00 00:00:00');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime) values(null         , 169, '9999-12-31 23:59:59');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime) values(null         , 170, '0000-00-00 01:01:01');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime) values(null         , 171, '1969-09-00 23:59:59');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime) values(null         , 172, '2018-00-00 00:00:00');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime) values(null         , 173, '2017-12-12 23:59:59');\n "))
            .put(C_DATETIME_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_1) values(null       , 175, '0000-00-00 00:00:00.0');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_1) values(null       , 176, '9999-12-31 23:59:59.9');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_1) values(null       , 177, '0000-00-00 01:01:01.12');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_1) values(null       , 178, '1969-09-00 23:59:59.06');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_1) values(null       , 179, '2018-00-00 00:00:00.04');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_1) values(null       , 180, '2017-12-12 23:59:59.045');\n "))
            .put(C_DATETIME_3, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_3) values(null       , 182, '0000-00-00 00:00:00.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_3) values(null       , 183, '9999-12-31 23:59:59.999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_3) values(null       , 184, '0000-00-00 01:01:01.121');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_3) values(null       , 185, '1969-09-00 23:59:59.0006');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_3) values(null       , 186, '2018-00-00 00:00:00.0004');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_3) values(null       , 187, '2017-12-12 23:59:59.00045');\n "))
            .put(C_DATETIME_6, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_6) values(null       , 189, '0000-00-00 00:00:00.000000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_6) values(null       , 190, '9999-12-31 23:59:59.999999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_6) values(null       , 191, '0000-00-00 01:01:01.121121');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_6) values(null       , 192, '1969-09-00 23:59:59.0000006');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_6) values(null       , 193, '2018-00-00 00:00:00.0000004');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_datetime_6) values(null       , 194, '2017-12-12 23:59:59.00000045');\n "))
            .put(C_TIMESTAMP, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp) values(null        , 196, '0000-00-00 00:00:00');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp) values(null        , 197, '9999-12-31 23:59:59');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp) values(null        , 198, '0000-00-00 01:01:01');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp) values(null        , 199, '1969-09-00 23:59:59');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp) values(null        , 200, '2018-00-00 00:00:00');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp) values(null        , 201, '2017-12-12 23:59:59');\n "))
            .put(C_TIMESTAMP_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_1) values(null      , 203, '0000-00-00 00:00:00.0');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_1) values(null      , 204, '9999-12-31 23:59:59.9');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_1) values(null      , 205, '0000-00-00 01:01:01.12');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_1) values(null      , 206, '1969-09-00 23:59:59.06');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_1) values(null      , 207, '2018-00-00 00:00:00.04');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_1) values(null      , 208, '2017-12-12 23:59:59.045');\n "))
            .put(C_TIMESTAMP_3, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_3) values(null      , 210, '0000-00-00 00:00:00.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_3) values(null      , 211, '9999-12-31 23:59:59.999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_3) values(null      , 212, '0000-00-00 01:01:01.121');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_3) values(null      , 213, '1969-09-00 23:59:59.0006');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_3) values(null      , 214, '2018-00-00 00:00:00.0004');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_3) values(null      , 215, '2017-12-12 23:59:59.00045');\n "))
            .put(C_TIMESTAMP_6, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_6) values(null      , 217, '0000-00-00 00:00:00.000000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_6) values(null      , 218, '9999-12-31 23:59:59.999999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_6) values(null      , 219, '0000-00-00 01:01:01.121121');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_6) values(null      , 220, '1969-09-00 23:59:59.0000006');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_6) values(null      , 221, '2018-00-00 00:00:00.0000004');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_timestamp_6) values(null      , 222, '2017-12-12 23:59:59.00000045');\n "))
            .put(C_TIME, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time) values(null             , 224, '-838:59:59');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time) values(null             , 225, '838:59:59');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time) values(null             , 226, '00:00:00');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time) values(null             , 227, '01:01:01');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time) values(null             , 228, '-01:01:01');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time) values(null             , 229, '23:59:59');\n "))
            .put(C_TIME_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_1) values(null           , 231, '-838:59:59.9');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_1) values(null           , 232, '838:59:59.9');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_1) values(null           , 233, '00:00:00.1');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_1) values(null           , 234, '01:01:01.6');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_1) values(null           , 235, '-01:01:01.4');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_1) values(null           , 236, '23:59:59.45');\n "))
            .put(C_TIME_3, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_3) values(null           , 238, '-838:59:59.999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_3) values(null           , 239, '838:59:59.999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_3) values(null           , 240, '00:00:00.111');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_3) values(null           , 241, '01:01:01.106');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_3) values(null           , 242, '-01:01:01.0004');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_3) values(null           , 243, '23:59:59.00045');\n "))
            .put(C_TIME_6, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_6) values(null           , 245, '-838:59:59.999999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_6) values(null           , 246, '838:59:59.999999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_6) values(null           , 247, '00:00:00.111111');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_6) values(null           , 248, '01:01:01.106106');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_6) values(null           , 249, '-01:01:01.0000004');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_time_6) values(null           , 250, '23:59:59.00000045');\n "))
            .put(C_YEAR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_year) values(null             , 252, '0000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_year) values(null             , 253, '9999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_year) values(null             , 254, '1970');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_year) values(null             , 255, '2000');\n "))
            .put(C_YEAR_4, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_year_4) values(null           , 257, '0000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_year_4) values(null           , 258, '9999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_year_4) values(null           , 259, '1970');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_year_4) values(null           , 260, '2000');\n "))
            .put(C_CHAR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_char) values(null             , 262, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_char) values(null             , 263, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_char) values(null             , 264, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_char) values(null             , 265, x'313233616263');\n "))
            .put(C_VARCHAR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_varchar) values(null          , 266, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_varchar) values(null          , 267, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_varchar) values(null          , 268, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_varchar) values(null             , 269, x'313233616263');\n "))
            .put(C_BINARY, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_binary) values(null           , 270, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_binary) values(null           , 271, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_binary) values(null           , 272, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_binary) values(null             , 273, x'0A08080E10011894AB0E');\n "))
            .put(C_VARBINARY, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_varbinary) values(null        , 274, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_varbinary) values(null        , 275, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_varbinary) values(null        , 276, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_varbinary) values(null             , 277, x'0A08080E10011894AB0E');\n "))
            .put(C_BLOB_TINY, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_tiny) values(null        , 278, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_tiny) values(null        , 279, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_tiny) values(null        , 280, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_tiny) values(null             , 281, x'0A08080E10011894AB0E');\n "))
            .put(C_BLOB, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob) values(null             , 282, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob) values(null             , 283, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob) values(null             , 284, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob) values(null             , 285, x'0A08080E10011894AB0E');\n "))
            .put(C_BLOB_MEDIUM, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_medium) values(null      , 286, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_medium) values(null      , 287, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_medium) values(null      , 288, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_medium) values(null             , 289, x'0A08080E10011894AB0E');\n "))
            .put(C_BLOB_LONG, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_long) values(null        , 290, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_long) values(null        , 291, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_long) values(null        , 292, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_blob_long) values(null             , 293, x'0A08080E10011894AB0E');\n "))
            .put(C_TEXT_TINY, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_tiny) values(null        , 294, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_tiny) values(null        , 295, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_tiny) values(null        , 296, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_tiny) values(null             , 297, x'313233616263');\n "))
            .put(C_TEXT, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text) values(null             , 298, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text) values(null             , 299, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text) values(null             , 300, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text) values(null             , 301, x'313233616263');\n "))
            .put(C_TEXT_MEDIUM, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_medium) values(null      , 302, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_medium) values(null      , 303, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_medium) values(null      , 304, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_medium) values(null             , 305, x'313233616263');\n "))
            .put(C_TEXT_LONG, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_long) values(null        , 306, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_long) values(null        , 307, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_long) values(null        , 308, 'a中国a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_text_long) values(null             , 309, x'313233616263');\n "))
            .put(C_ENUM, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_enum) values(null             , 310, 'a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_enum) values(null             , 311, 'b');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_enum) values(null             , 312, NULL);\n "))
            .put(C_SET, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_set) values(null              , 314, 'a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_set) values(null              , 315, 'b, a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_set) values(null              , 316, 'b, c, a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_set) values(null              , 317, 'd');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_set) values(null              , 318, NULL);\n "))
            .put(C_JSON, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_json) values(null             , 320, '{\"k1\": \"v1\", \"k2\": 10}');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_json) values(null             , 321, '{\"k1\": \"v1\", \"k2\": [10, 20]}');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_json) values(null             , 322, NULL);\n "))
            .put(C_POINT, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_point) VALUE (null            , 324, ST_POINTFROMTEXT('POINT(15 20)'));\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_linestring) VALUE (null       , 325, ST_POINTFROMTEXT('LINESTRING(0 0,10 10,20 25,50 60)'));\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_polygon) VALUE (null          , 326, ST_POINTFROMTEXT('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7,5 5))'));\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk
                    + ", c_geometory) VALUE (null        , 327, ST_POINTFROMTEXT('POINT(15 20)'));\n "))
            .build();

        return base.entrySet()
            .stream()
            .filter(e -> !e.getKey().equalsIgnoreCase(sk))
            .filter(e -> !e.getKey().equalsIgnoreCase(pk))
            .flatMap(e -> e.getValue().stream())
            .collect(Collectors.toList());

    }

    public static ImmutableMap<String, List<String>> buildGsiFullTypeTestValues() {
        final ImmutableMap.Builder<String, List<String>> builder = ImmutableMap.builder();

        builder.put(C_ID,
            ImmutableList.of("null"));
        builder.put(C_BIT_1,
            ImmutableList.of("0",
                "1",
                "2"));
        builder.put(C_BIT_8,
            ImmutableList.of("0",
                "1",
                "2",
                "256"));
        builder.put(C_BIT_16,
            ImmutableList.of("0",
                "1",
                "2",
                "65535"));
        builder.put(C_BIT_32,
            ImmutableList.of("0",
                "1",
                "2",
                "4294967296"));
        builder.put(C_BIT_64,
            ImmutableList.of("0",
                "1",
                "2",
                "18446744073709551615"));
        builder.put(C_TINYINT_1,
            ImmutableList.of("-1",
                "0",
                "1",
                "127"));
        builder.put(C_TINYINT_1_UN,
            ImmutableList.of("-1",
                "0",
                "1",
                "127",
                "255"));
        builder.put(C_TINYINT_4,
            ImmutableList.of("-1",
                "0",
                "1",
                "127"));
        builder.put(C_TINYINT_4_UN,
            ImmutableList.of("-1",
                "0",
                "1",
                "127",
                "255"));
        builder.put(C_TINYINT_8,
            ImmutableList.of("-1",
                "0",
                "1",
                "127"));
        builder.put(C_TINYINT_8_UN,
            ImmutableList.of("-1",
                "0",
                "1",
                "127",
                "255"));
        builder.put(C_SMALLINT_1,
            ImmutableList.of("-1",
                "0",
                "1",
                "65535"));
        builder.put(C_SMALLINT_16,
            ImmutableList.of("-1",
                "0",
                "1",
                "65535"));
        builder.put(C_SMALLINT_16_UN,
            ImmutableList.of("-1",
                "0",
                "1",
                "65535"));
        builder.put(C_MEDIUMINT_1,
            ImmutableList.of("-1",
                "0",
                "1",
                "16777215"));
        builder.put(C_MEDIUMINT_24,
            ImmutableList.of("-1",
                "0",
                "1",
                "16777215"));
        builder.put(C_MEDIUMINT_24_UN,
            ImmutableList.of("-1",
                "0",
                "1",
                "16777215"));
        builder.put(C_INT_1,
            ImmutableList.of("-1",
                "0",
                "1",
                "4294967295"));
        builder.put(C_INT_32,
            ImmutableList.of("-1",
                "0",
                "1",
                "4294967295"));
        builder.put(C_INT_32_UN,
            ImmutableList.of("-1",
                "0",
                "1",
                "4294967295"));
        builder.put(C_BIGINT_1,
            ImmutableList.of("-1",
                "0",
                "1",
                "18446744073709551615"));
        builder.put(C_BIGINT_64,
            ImmutableList.of("-1",
                "0",
                "1",
                "18446744073709551615"));
        builder.put(C_BIGINT_64_UN,
            ImmutableList.of("-1",
                "0",
                "1",
                "18446744073709551615"));
        builder.put(C_DECIMAL,
            ImmutableList.of("'100.000'",
                "'100.003'",
                "'-100.003'",
                "'-100.0000001'"));
        builder.put(C_DECIMAL_PR,
            ImmutableList.of("'100.000'",
                "'100.003'",
                "'-100.003'",
                "'-100.0000001'",
                "5.576856765031534000000000000000"));
        builder.put(C_FLOAT,
            ImmutableList.of("'100.000'",
                "'100.003'",
                "'-100.003'",
                "'-100.0000001'"));
        builder.put(C_FLOAT_PR,
            ImmutableList.of("'100.000'",
                "'100.003'",
                "'-100.003'",
                "'-100.0000001'"));
        builder.put(C_FLOAT_UN,
            ImmutableList.of("'100.000'",
                "'100.003'",
                "'-100.003'",
                "'-100.0000001'"));
        builder.put(C_DOUBLE,
            ImmutableList.of("'100.000'",
                "'100.003'",
                "'-100.003'",
                "'-100.0000001'"));
        builder.put(C_DOUBLE_PR,
            ImmutableList.of("'100.000'",
                "'100.003'",
                "'-100.003'",
                "'-100.0000001'"));
        builder.put(C_DOUBLE_UN,
            ImmutableList.of("'100.000'",
                "'100.003'",
                "'-100.003'",
                "'-100.0000001'"));
        builder.put(C_DATE,
            ImmutableList.of("'0000-00-00'",
                "'9999-12-31'",
                "'0000-00-00 01:01:01'",
                "'1969-09-00'",
                "'2018-00-00'",
                "'2017-12-12'"));
        builder.put(C_DATETIME,
            ImmutableList.of(
                "'0000-00-00 00:00:00'",
                "'9999-12-31 23:59:59'",
                "'0000-00-00 01:01:01'",
                "'1969-09-00 23:59:59'",
                "'2018-00-00 00:00:00'",
                "'2017-12-12 23:59:59'"));
        builder.put(C_DATETIME_1,
            ImmutableList.of(
                "'0000-00-00 00:00:00.0'",
                "'9999-12-31 23:59:59.9'",
                "'0000-00-00 01:01:01.12'",
                "'1969-09-00 23:59:59.06'",
                "'2018-00-00 00:00:00.04'",
                "'2017-12-12 23:59:59.045'"));
        builder.put(C_DATETIME_3,
            ImmutableList.of(
                "'0000-00-00 00:00:00.000'",
                "'9999-12-31 23:59:59.999'",
                "'0000-00-00 01:01:01.121'",
                "'1969-09-00 23:59:59.0006'",
                "'2018-00-00 00:00:00.0004'",
                "'2017-12-12 23:59:59.00045'"));
        builder.put(C_DATETIME_6,
            ImmutableList.of(
                "'0000-00-00 00:00:00.000000'",
                "'9999-12-31 23:59:59.999999'",
                "'0000-00-00 01:01:01.121121'",
                "'1969-09-00 23:59:59.0000006'",
                "'2018-00-00 00:00:00.0000004'",
                "'2017-12-12 23:59:59.00000045'"));
        builder.put(C_TIMESTAMP,
            ImmutableList.of(
                "'0000-00-00 00:00:00'",
                "'9999-12-31 23:59:59'",
                "'0000-00-00 01:01:01'",
                "'1969-09-00 23:59:59'",
                "'2018-00-00 00:00:00'",
                "'2017-12-12 23:59:59'"));
        builder.put(C_TIMESTAMP_1,
            ImmutableList.of(
                "'0000-00-00 00:00:00.0'",
                "'9999-12-31 23:59:59.9'",
                "'0000-00-00 01:01:01.12'",
                "'1969-09-00 23:59:59.06'",
                "'2018-00-00 00:00:00.04'",
                "'2017-12-12 23:59:59.045'"));
        builder.put(C_TIMESTAMP_3,
            ImmutableList.of(
                "'0000-00-00 00:00:00.000'",
                "'9999-12-31 23:59:59.999'",
                "'0000-00-00 01:01:01.121'",
                "'1969-09-00 23:59:59.0006'",
                "'2018-00-00 00:00:00.0004'",
                "'2017-12-12 23:59:59.00045'"));
        builder.put(C_TIMESTAMP_6,
            ImmutableList.of(
                "'0000-00-00 00:00:00.000000'",
                "'9999-12-31 23:59:59.999999'",
                "'0000-00-00 01:01:01.121121'",
                "'1969-09-00 23:59:59.0000006'",
                "'2018-00-00 00:00:00.0000004'",
                "'2017-12-12 23:59:59.00000045'"));
        builder.put(C_TIME,
            ImmutableList.of("'-838:59:59'",
                "'838:59:59'",
                "'00:00:00'",
                "'01:01:01'",
                "'-01:01:01'",
                "'23:59:59'"));
        builder.put(C_TIME_1,
            ImmutableList.of("'-838:59:59.9'",
                "'838:59:59.9'",
                "'00:00:00.1'",
                "'01:01:01.6'",
                "'-01:01:01.4'",
                "'23:59:59.45'"));
        builder.put(C_TIME_3,
            ImmutableList.of("'-838:59:59.999'",
                "'838:59:59.999'",
                "'00:00:00.111'",
                "'01:01:01.106'",
                "'-01:01:01.0004'",
                "'23:59:59.00045'"));
        builder.put(C_TIME_6,
            ImmutableList.of("'-838:59:59.999999'",
                "'838:59:59.999999'",
                "'00:00:00.111111'",
                "'01:01:01.106106'",
                "'-01:01:01.0000004'",
                "'23:59:59.00000045'"));
        builder.put(C_YEAR,
            ImmutableList.of("'0000'",
                "'9999'",
                "'1970'",
                "'2000'"));
        builder.put(C_YEAR_4,
            ImmutableList.of("'0000'",
                "'9999'",
                "'1970'",
                "'2000'"));
        builder.put(C_CHAR,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'313233616263'"));
        builder.put(C_VARCHAR,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'313233616263'"));
        builder.put(C_BINARY,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'0A08080E10011894AB0E'"));
        builder.put(C_VARBINARY,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'0A08080E10011894AB0E'"));
        builder.put(C_BLOB_TINY,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'0A08080E10011894AB0E'"));
        builder.put(C_BLOB,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'0A08080E10011894AB0E'"));
        builder.put(C_BLOB_MEDIUM,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'0A08080E10011894AB0E'"));
        builder.put(C_BLOB_LONG,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'0A08080E10011894AB0E'"));
        builder.put(C_TEXT_TINY,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'313233616263'"));
        builder.put(C_TEXT,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'313233616263'"));
        builder.put(C_TEXT_MEDIUM,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'313233616263'"));
        builder.put(C_TEXT_LONG,
            ImmutableList.of("'11'",
                "'99'",
                "'a中国a'",
                "x'313233616263'"));
        builder.put(C_ENUM,
            ImmutableList.of("'a'",
                "'b'",
                "NULL"));
        builder.put(C_SET,
            ImmutableList.of("'a'",
                "'b,a'",
                "'b,c,a'",
                "'d'",
                "NULL"));
        builder.put(C_JSON,
            ImmutableList.of(
                "'{\"k1\": \"v1\", \"k2\": 10}'",
                "'{\"k1\": \"v1\", \"k2\": [10, 20]}'",
                "NULL"));
        builder.put(C_POINT,
            ImmutableList.of(
                "ST_POINTFROMTEXT('POINT(15 20)')"));
        builder.put(C_LINESTRING,
            ImmutableList
                .of("ST_POINTFROMTEXT('LINESTRING(0 0, 10 10, 20 25, 50 60)')"));
        builder.put(C_POLYGON,
            ImmutableList
                .of("ST_POINTFROMTEXT('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))')"));
        builder.put(C_GEOMETORY,
            ImmutableList.of("ST_POINTFROMTEXT('POINT(15 20)')"));
        builder.put(C_MULTIPOINT,
            ImmutableList.of(
                "ST_POINTFROMTEXT('MULTIPOINT(0 0, 15 25, 45 65)')"));
        builder.put(C_MULTILINESTRING,
            ImmutableList
                .of("ST_POINTFROMTEXT('MULTILINESTRING((10 10, 20 20), (15 15, 30 15))')"));
        builder.put(C_MULTIPOLYGON,
            ImmutableList
                .of("ST_POINTFROMTEXT('MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))')"));
        builder.put(C_GEOMETRYCOLLECTION,
            ImmutableList
                .of("ST_POINTFROMTEXT('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))')"));

        return builder.build();
    }

    public static ImmutableMap<String, List<String>> FULL_TYPE_TEST_VALUES = buildGsiFullTypeTestValues();

    public static String genRandomInsert(String primaryTable, List<String> columns) {
        String cols = String.join(", ", columns);
        String vals = columns.stream()
            .map(col -> FULL_TYPE_TEST_VALUES.get(col)
                .get(ThreadLocalRandom.current().nextInt(FULL_TYPE_TEST_VALUES.get(col).size())))
            .collect(Collectors.joining(", "));
        return "insert into `" + primaryTable + "` (" + cols + ") VALUE (" + vals + ");";
    }

    public static String genRandomUpdate(String primaryTable, String column) {
        List<String> values = new ArrayList<>(FULL_TYPE_TEST_VALUES.get(column));
        String from = values.get(ThreadLocalRandom.current().nextInt(values.size()));
        String to = values.get(ThreadLocalRandom.current().nextInt(values.size()));
        String cmp = from.equalsIgnoreCase("null") ? column + " is null" : column + " = " + from;
        return "update `" + primaryTable + "` set " + column + " = " + to + " where " + cmp + ";";
    }

    public static String genRandomDelete(String primaryTable, String column) {
        List<String> values = new ArrayList<>(FULL_TYPE_TEST_VALUES.get(column));
        String val = values.get(ThreadLocalRandom.current().nextInt(values.size()));
        String cmp = val.equalsIgnoreCase("null") ? column + " is null" : column + " = " + val;
        return "delete from `" + primaryTable + "` where " + cmp + ";";
    }

    public static String getCoveringColumns(final String primarySk, final String indexSk) {
        return FULL_TYPE_TABLE_COLUMNS.stream()
            // do not support covering column with default current_timestamp
            .filter(column -> !column.equalsIgnoreCase(C_TIMESTAMP))
            .filter(column -> !column.equalsIgnoreCase(primarySk))
            .filter(column -> !column.equalsIgnoreCase(indexSk))
            .collect(Collectors.joining(", "));
    }

    public static String getCoveringColumns(final String primarySk, final List<String> indexSk) {
        return FULL_TYPE_TABLE_COLUMNS.stream()
            // do not support covering column with default current_timestamp
            .filter(column -> !column.equalsIgnoreCase(C_TIMESTAMP))
            .filter(column -> !column.equalsIgnoreCase(primarySk))
            .filter(column -> indexSk.stream().noneMatch(column::equalsIgnoreCase))
            .collect(Collectors.joining(", "));
    }

    public static String partitioning(String sk) {
        if (dateType.contains(sk)) {
            return yyyyMmOptPartitioning(sk);
        }

        return hashPartitioning(sk);
    }

    public static String hashPartitioning(String sk) {
        return MessageFormat.format(HASH_PARTITIONING_TEMPLATE, sk, sk);
    }

    private static String yyyyMmOptPartitioning(String sk) {
        return MessageFormat.format(YYYYMM_OPT_PARTITIONING_TEMPLATE, sk, sk);
    }

    public static String getCreateGsi(String primary, String index, String indexSk, String covering,
                                      String partitioning) {
        final String result = MessageFormat
            .format(CREATE_GSI_TEMPLATE, index, primary, indexSk, covering, partitioning);

//        System.out.println("Initialize GSI with: ");
//        System.out.println(result);

        return result;
    }

    public static String getAddGsi(String primary, String index, String indexSk, String covering,
                                   String partitioning) {
        final String result = MessageFormat.format(ADD_GSI_TEMPLATE, primary, index, indexSk, covering, partitioning);

//        System.out.println("Initialize GSI with: ");
//        System.out.println(result);

        return result;
    }

    public static Map<String, String> COLUMN_DEF_MAP = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    public static Map<String, String> COLUMN_TYPE_MAP = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    public static Map<String, String> PK_COLUMN_DEF_MAP = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    public static Map<String, String> PK_DEF_MAP = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    static {
        COLUMN_DEF_MAP.put("id", "  `id` bigint(20) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_bit_1", "  `c_bit_1` bit(1) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_bit_8", "  `c_bit_8` bit(8) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_bit_16", "  `c_bit_16` bit(16) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_bit_32", "  `c_bit_32` bit(32) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_bit_64", "  `c_bit_64` bit(64) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_tinyint_1", "  `c_tinyint_1` tinyint(1) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_tinyint_1_un", "  `c_tinyint_1_un` tinyint(1) unsigned DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_tinyint_4", "  `c_tinyint_4` tinyint(4) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_tinyint_4_un", "  `c_tinyint_4_un` tinyint(4) unsigned DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_tinyint_8", "  `c_tinyint_8` tinyint(8) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_tinyint_8_un", "  `c_tinyint_8_un` tinyint(8) unsigned DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_smallint_1", "  `c_smallint_1` smallint(1) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_smallint_16", "  `c_smallint_16` smallint(16) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_smallint_16_un", "  `c_smallint_16_un` smallint(16) unsigned DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_mediumint_1", "  `c_mediumint_1` mediumint(1) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_mediumint_24", "  `c_mediumint_24` mediumint(24) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_mediumint_24_un", "  `c_mediumint_24_un` mediumint(24) unsigned DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_int_1", "  `c_int_1` int(1) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_int_32", "  `c_int_32` int(32) DEFAULT NULL ,\n");
        COLUMN_DEF_MAP.put("c_int_32_un", "  `c_int_32_un` int(32) unsigned DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_bigint_1", "  `c_bigint_1` bigint(1) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_bigint_64", "  `c_bigint_64` bigint(64) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_bigint_64_un", "  `c_bigint_64_un` bigint(64) unsigned DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_decimal", "  `c_decimal` decimal DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_decimal_pr", "  `c_decimal_pr` decimal(65,30) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_float", "  `c_float` float DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_float_pr", "  `c_float_pr` float(10,3) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_float_un", "  `c_float_un` float(10,3) unsigned DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_double", "  `c_double` double DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_double_pr", "  `c_double_pr` double(10,3) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_double_un", "  `c_double_un` double(10,3) unsigned DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_date", "  `c_date` date DEFAULT NULL COMMENT \"date\",\n");
        COLUMN_DEF_MAP.put("c_datetime", "  `c_datetime` datetime DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_datetime_1", "  `c_datetime_1` datetime(1) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_datetime_3", "  `c_datetime_3` datetime(3) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_datetime_6", "  `c_datetime_6` datetime(6) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_timestamp", "  `c_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,\n");
        COLUMN_DEF_MAP.put("c_timestamp_1", "  `c_timestamp_1` timestamp(1) DEFAULT \"2000-01-01 00:00:00\",\n");
        COLUMN_DEF_MAP.put("c_timestamp_3", "  `c_timestamp_3` timestamp(3) DEFAULT \"2000-01-01 00:00:00\",\n");
        COLUMN_DEF_MAP.put("c_timestamp_6", "  `c_timestamp_6` timestamp(6) DEFAULT \"2000-01-01 00:00:00\",\n");
        COLUMN_DEF_MAP.put("c_time", "  `c_time` time DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_time_1", "  `c_time_1` time(1) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_time_3", "  `c_time_3` time(3) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_time_6", "  `c_time_6` time(6) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_year", "  `c_year` year DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_year_4", "  `c_year_4` year(4) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_char", "  `c_char` char(10) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_varchar", "  `c_varchar` varchar(10) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_binary", "  `c_binary` binary(10) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_varbinary", "  `c_varbinary` varbinary(10) DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_blob_tiny", "  `c_blob_tiny` tinyblob DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_blob", "  `c_blob` blob DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_blob_medium", "  `c_blob_medium` mediumblob DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_blob_long", "  `c_blob_long` longblob DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_text_tiny", "  `c_text_tiny` tinytext DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_text", "  `c_text` text DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_text_medium", "  `c_text_medium` mediumtext DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_text_long", "  `c_text_long` longtext DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_enum", "  `c_enum` enum(\"a\",\"b\",\"c\") DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_set", "  `c_set` set(\"a\",\"b\",\"c\") DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_json", "  `c_json` json DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_geometory", "  `c_geometory` geometry DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_point", "  `c_point` point DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_linestring", "  `c_linestring` linestring DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_polygon", "  `c_polygon` polygon DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_multipoint", "  `c_multipoint` multipoint DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_multilinestring", "  `c_multilinestring` multilinestring DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_multipolygon", "  `c_multipolygon` multipolygon DEFAULT NULL,\n");
        COLUMN_DEF_MAP.put("c_geometrycollection", "  `c_geometrycollection` geometrycollection DEFAULT NULL,\n");

        PK_COLUMN_DEF_MAP.put("id",
            "  `id` bigint(20)                            NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_bit_1",
            "  `c_bit_1` bit(1)                           NOT NULL DEFAULT 0                                        , \n ");
        PK_COLUMN_DEF_MAP.put("c_bit_8",
            "  `c_bit_8` bit(8)                           NOT NULL DEFAULT 0                                        , \n ");
        PK_COLUMN_DEF_MAP.put("c_bit_16",
            "  `c_bit_16` bit(16)                         NOT NULL DEFAULT 0                                        , \n ");
        PK_COLUMN_DEF_MAP.put("c_bit_32",
            "  `c_bit_32` bit(32)                         NOT NULL DEFAULT 0                                        , \n ");
        PK_COLUMN_DEF_MAP.put("c_bit_64",
            "  `c_bit_64` bit(64)                         NOT NULL DEFAULT 0                                        , \n ");
        PK_COLUMN_DEF_MAP.put("c_tinyint_1",
            "  `c_tinyint_1` tinyint(1)                   NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_tinyint_1_un",
            "  `c_tinyint_1_un` tinyint(1) unsigned       NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_tinyint_4",
            "  `c_tinyint_4` tinyint(4)                   NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_tinyint_4_un",
            "  `c_tinyint_4_un` tinyint(4) unsigned       NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_tinyint_8",
            "  `c_tinyint_8` tinyint(8)                   NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_tinyint_8_un",
            "  `c_tinyint_8_un` tinyint(8) unsigned       NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_smallint_1",
            "  `c_smallint_1` smallint(1)                 NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_smallint_16",
            "  `c_smallint_16` smallint(16)               NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_smallint_16_un",
            "  `c_smallint_16_un` smallint(16) unsigned   NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_mediumint_1",
            "  `c_mediumint_1` mediumint(1)               NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_mediumint_24",
            "  `c_mediumint_24` mediumint(24)             NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_mediumint_24_un",
            "  `c_mediumint_24_un` mediumint(24) unsigned NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_int_1",
            "  `c_int_1` int(1)                           NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_int_32",
            "  `c_int_32` int(32)                         NOT NULL AUTO_INCREMENT COMMENT \"For multi pk.\"         , \n ");
        PK_COLUMN_DEF_MAP.put("c_int_32_un",
            "  `c_int_32_un` int(32) unsigned             NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_bigint_1",
            "  `c_bigint_1` bigint(1)                     NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_bigint_64",
            "  `c_bigint_64` bigint(64)                   NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_bigint_64_un",
            "  `c_bigint_64_un` bigint(64) unsigned       NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_decimal",
            "  `c_decimal` decimal                        NOT NULL DEFAULT 0                                        , \n ");
        PK_COLUMN_DEF_MAP.put("c_decimal_pr",
            "  `c_decimal_pr` decimal(65,30)              NOT NULL DEFAULT 0                                        , \n ");
        PK_COLUMN_DEF_MAP.put("c_float",
            "  `c_float` float                            NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_float_pr",
            "  `c_float_pr` float(10,3)                   NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_float_un",
            "  `c_float_un` float(10,3) unsigned          NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_double",
            "  `c_double` double                          NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_double_pr",
            "  `c_double_pr` double(10,3)                 NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_double_un",
            "  `c_double_un` double(10,3) unsigned        NOT NULL AUTO_INCREMENT                                   , \n ");
        PK_COLUMN_DEF_MAP.put("c_date",
            "  `c_date` date                              NOT NULL DEFAULT \"2020-01-01 00:00:00\" COMMENT \"date\" , \n ");
        PK_COLUMN_DEF_MAP.put("c_datetime",
            "  `c_datetime` datetime                      NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_datetime_1",
            "  `c_datetime_1` datetime(1)                 NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_datetime_3",
            "  `c_datetime_3` datetime(3)                 NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_datetime_6",
            "  `c_datetime_6` datetime(6)                 NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_timestamp",
            "  `c_timestamp` timestamp                    NOT NULL DEFAULT CURRENT_TIMESTAMP                        , \n ");
        PK_COLUMN_DEF_MAP.put("c_timestamp_1",
            "  `c_timestamp_1` timestamp(1)               NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_timestamp_3",
            "  `c_timestamp_3` timestamp(3)               NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_timestamp_6",
            "  `c_timestamp_6` timestamp(6)               NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_time",
            "  `c_time` time                              NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_time_1",
            "  `c_time_1` time(1)                         NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_time_3",
            "  `c_time_3` time(3)                         NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_time_6",
            "  `c_time_6` time(6)                         NOT NULL DEFAULT \"2020-01-01 00:00:00\"                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_year",
            "  `c_year` year                              NOT NULL DEFAULT \"2020\"                                 , \n ");
        PK_COLUMN_DEF_MAP.put("c_year_4",
            "  `c_year_4` year(4)                         NOT NULL DEFAULT \"2020\"                                 , \n ");
        PK_COLUMN_DEF_MAP.put("c_char",
            "  `c_char` char(10)                          NOT NULL DEFAULT \"\"                                     , \n ");
        PK_COLUMN_DEF_MAP.put("c_varchar",
            "  `c_varchar` varchar(10)                    NOT NULL DEFAULT \"\"                                     , \n ");
        PK_COLUMN_DEF_MAP.put("c_binary",
            "  `c_binary` binary(10)                      NOT NULL DEFAULT \"\"                                     , \n ");
        PK_COLUMN_DEF_MAP.put("c_varbinary",
            "  `c_varbinary` varbinary(10)                NOT NULL DEFAULT \"\"                                     , \n ");
        PK_COLUMN_DEF_MAP.put("c_blob_tiny",
            "  `c_blob_tiny` tinyblob                     NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_blob",
            "  `c_blob` blob                              NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_blob_medium",
            "  `c_blob_medium` mediumblob                 NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_blob_long",
            "  `c_blob_long` longblob                     NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_text_tiny",
            "  `c_text_tiny` tinytext                     NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_text",
            "  `c_text` text                              NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_text_medium",
            "  `c_text_medium` mediumtext                 NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_text_long",
            "  `c_text_long` longtext                     NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_enum",
            "  `c_enum` enum(\"a\",\"b\",\"c\")           NOT NULL DEFAULT \"a\"                                    , \n ");
        PK_COLUMN_DEF_MAP.put("c_set",
            "  `c_set` set(\"a\",\"b\",\"c\")             NOT NULL DEFAULT \"a\"                                    , \n ");
        PK_COLUMN_DEF_MAP.put("c_json",
            "  `c_json` json                              NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_geometory",
            "  `c_geometory` geometry                     NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_point",
            "  `c_point` point                            NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_linestring",
            "  `c_linestring` linestring                  NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_polygon",
            "  `c_polygon` polygon                        NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_multipoint",
            "  `c_multipoint` multipoint                  NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_multilinestring",
            "  `c_multilinestring` multilinestring        NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_multipolygon",
            "  `c_multipolygon` multipolygon              NOT NULL                                                  , \n ");
        PK_COLUMN_DEF_MAP.put("c_geometrycollection",
            "  `c_geometrycollection` geometrycollection  NOT NULL                                                  , \n ");

        PK_DEF_MAP.put("id", " PRIMARY KEY(`id`)                     ");
        PK_DEF_MAP.put("c_bit_1", " PRIMARY KEY(`c_bit_1`)                ");
        PK_DEF_MAP.put("c_bit_8", " PRIMARY KEY(`c_bit_8`)                ");
        PK_DEF_MAP.put("c_bit_16", " PRIMARY KEY(`c_bit_16`)               ");
        PK_DEF_MAP.put("c_bit_32", " PRIMARY KEY(`c_bit_32`)               ");
        PK_DEF_MAP.put("c_bit_64", " PRIMARY KEY(`c_bit_64`)               ");
        PK_DEF_MAP.put("c_tinyint_1", " PRIMARY KEY(`c_tinyint_1`)            ");
        PK_DEF_MAP.put("c_tinyint_1_un", " PRIMARY KEY(`c_tinyint_1_un`)         ");
        PK_DEF_MAP.put("c_tinyint_4", " PRIMARY KEY(`c_tinyint_4`)            ");
        PK_DEF_MAP.put("c_tinyint_4_un", " PRIMARY KEY(`c_tinyint_4_un`)         ");
        PK_DEF_MAP.put("c_tinyint_8", " PRIMARY KEY(`c_tinyint_8`)            ");
        PK_DEF_MAP.put("c_tinyint_8_un", " PRIMARY KEY(`c_tinyint_8_un`)         ");
        PK_DEF_MAP.put("c_smallint_1", " PRIMARY KEY(`c_smallint_1`)           ");
        PK_DEF_MAP.put("c_smallint_16", " PRIMARY KEY(`c_smallint_16`)          ");
        PK_DEF_MAP.put("c_smallint_16_un", " PRIMARY KEY(`c_smallint_16_un`)       ");
        PK_DEF_MAP.put("c_mediumint_1", " PRIMARY KEY(`c_mediumint_1`)          ");
        PK_DEF_MAP.put("c_mediumint_24", " PRIMARY KEY(`c_mediumint_24`)         ");
        PK_DEF_MAP.put("c_mediumint_24_un", " PRIMARY KEY(`c_mediumint_24_un`)      ");
        PK_DEF_MAP.put("c_int_1", " PRIMARY KEY(`c_int_1`)                ");
        PK_DEF_MAP.put("c_int_32", " PRIMARY KEY(`c_int_32`)               ");
        PK_DEF_MAP.put("c_int_32_un", " PRIMARY KEY(`c_int_32_un`)            ");
        PK_DEF_MAP.put("c_bigint_1", " PRIMARY KEY(`c_bigint_1`)             ");
        PK_DEF_MAP.put("c_bigint_64", " PRIMARY KEY(`c_bigint_64`)            ");
        PK_DEF_MAP.put("c_bigint_64_un", " PRIMARY KEY(`c_bigint_64_un`)         ");
        PK_DEF_MAP.put("c_decimal", " PRIMARY KEY(`c_decimal`)              ");
        PK_DEF_MAP.put("c_decimal_pr", " PRIMARY KEY(`c_decimal_pr`)           ");
        PK_DEF_MAP.put("c_float", " PRIMARY KEY(`c_float`)                ");
        PK_DEF_MAP.put("c_float_pr", " PRIMARY KEY(`c_float_pr`)             ");
        PK_DEF_MAP.put("c_float_un", " PRIMARY KEY(`c_float_un`)             ");
        PK_DEF_MAP.put("c_double", " PRIMARY KEY(`c_double`)               ");
        PK_DEF_MAP.put("c_double_pr", " PRIMARY KEY(`c_double_pr`)            ");
        PK_DEF_MAP.put("c_double_un", " PRIMARY KEY(`c_double_un`)            ");
        PK_DEF_MAP.put("c_date", " PRIMARY KEY(`c_date`)                 ");
        PK_DEF_MAP.put("c_datetime", " PRIMARY KEY(`c_datetime`)             ");
        PK_DEF_MAP.put("c_datetime_1", " PRIMARY KEY(`c_datetime_1`)           ");
        PK_DEF_MAP.put("c_datetime_3", " PRIMARY KEY(`c_datetime_3`)           ");
        PK_DEF_MAP.put("c_datetime_6", " PRIMARY KEY(`c_datetime_6`)           ");
        PK_DEF_MAP.put("c_timestamp", " PRIMARY KEY(`c_timestamp`)            ");
        PK_DEF_MAP.put("c_timestamp_1", " PRIMARY KEY(`c_timestamp_1`)          ");
        PK_DEF_MAP.put("c_timestamp_3", " PRIMARY KEY(`c_timestamp_3`)          ");
        PK_DEF_MAP.put("c_timestamp_6", " PRIMARY KEY(`c_timestamp_6`)          ");
        PK_DEF_MAP.put("c_time", " PRIMARY KEY(`c_time`)                 ");
        PK_DEF_MAP.put("c_time_1", " PRIMARY KEY(`c_time_1`)               ");
        PK_DEF_MAP.put("c_time_3", " PRIMARY KEY(`c_time_3`)               ");
        PK_DEF_MAP.put("c_time_6", " PRIMARY KEY(`c_time_6`)               ");
        PK_DEF_MAP.put("c_year", " PRIMARY KEY(`c_year`)                 ");
        PK_DEF_MAP.put("c_year_4", " PRIMARY KEY(`c_year_4`)               ");
        PK_DEF_MAP.put("c_char", " PRIMARY KEY(`c_char`)                 ");
        PK_DEF_MAP.put("c_varchar", " PRIMARY KEY(`c_varchar`)              ");
        PK_DEF_MAP.put("c_binary", " PRIMARY KEY(`c_binary`)               ");
        PK_DEF_MAP.put("c_varbinary", " PRIMARY KEY(`c_varbinary`)            ");
        PK_DEF_MAP.put("c_blob_tiny", " PRIMARY KEY(`c_blob_tiny`(255))       ");
        PK_DEF_MAP.put("c_blob", " PRIMARY KEY(`c_blob`(255))            ");
        PK_DEF_MAP.put("c_blob_medium", " PRIMARY KEY(`c_blob_medium`(255))     ");
        PK_DEF_MAP.put("c_blob_long", " PRIMARY KEY(`c_blob_long`(255))       ");
        PK_DEF_MAP.put("c_text_tiny", " PRIMARY KEY(`c_text_tiny`(63))       ");
        PK_DEF_MAP.put("c_text", " PRIMARY KEY(`c_text`(255))            ");
        PK_DEF_MAP.put("c_text_medium", " PRIMARY KEY(`c_text_medium`(255))     ");
        PK_DEF_MAP.put("c_text_long", " PRIMARY KEY(`c_text_long`(255))       ");
        PK_DEF_MAP.put("c_enum", " PRIMARY KEY(`c_enum`)                 ");
        PK_DEF_MAP.put("c_set", " PRIMARY KEY(`c_set`)                  ");
        PK_DEF_MAP.put("c_json", " PRIMARY KEY(`c_json`(255))            ");
        PK_DEF_MAP.put("c_geometory", " PRIMARY KEY(`c_geometory`(255))       ");
        PK_DEF_MAP.put("c_point", " PRIMARY KEY(`c_point`(255))           ");
        PK_DEF_MAP.put("c_linestring", " PRIMARY KEY(`c_linestring`(255))      ");
        PK_DEF_MAP.put("c_polygon", " PRIMARY KEY(`c_polygon`(255))         ");
        PK_DEF_MAP.put("c_multipoint", " PRIMARY KEY(`c_multipoint`(255))      ");
        PK_DEF_MAP.put("c_multilinestring", " PRIMARY KEY(`c_multilinestring`(255)) ");
        PK_DEF_MAP.put("c_multipolygon", " PRIMARY KEY(`c_multipolygon`(255))    ");
        PK_DEF_MAP.put("c_geometrycollection", " PRIMARY KEY(`c_geometrycollection`)   ");

        COLUMN_TYPE_MAP.put("id", "  bigint(20)");
        COLUMN_TYPE_MAP.put("c_bit_1", "  bit(1)");
        COLUMN_TYPE_MAP.put("c_bit_8", "  bit(8)");
        COLUMN_TYPE_MAP.put("c_bit_16", "  bit(16)");
        COLUMN_TYPE_MAP.put("c_bit_32", "  bit(32)");
        COLUMN_TYPE_MAP.put("c_bit_64", "  bit(64)");
        COLUMN_TYPE_MAP.put("c_tinyint_1", "  tinyint(1)");
        COLUMN_TYPE_MAP.put("c_tinyint_1_un", "  tinyint(1) unsigned");
        COLUMN_TYPE_MAP.put("c_tinyint_4", "  tinyint(4)");
        COLUMN_TYPE_MAP.put("c_tinyint_4_un", "  tinyint(4) unsigned");
        COLUMN_TYPE_MAP.put("c_tinyint_8", "  tinyint(8)");
        COLUMN_TYPE_MAP.put("c_tinyint_8_un", "  tinyint(8) unsigned");
        COLUMN_TYPE_MAP.put("c_smallint_1", "  smallint(1)");
        COLUMN_TYPE_MAP.put("c_smallint_16", "  smallint(16)");
        COLUMN_TYPE_MAP.put("c_smallint_16_un", "  smallint(16) unsigned");
        COLUMN_TYPE_MAP.put("c_mediumint_1", "  mediumint(1)");
        COLUMN_TYPE_MAP.put("c_mediumint_24", "  mediumint(24)");
        COLUMN_TYPE_MAP.put("c_mediumint_24_un", "  mediumint(24) unsigned");
        COLUMN_TYPE_MAP.put("c_int_1", "  int(1)");
        COLUMN_TYPE_MAP.put("c_int_32", "  int(32)");
        COLUMN_TYPE_MAP.put("c_int_32_un", "  int(32) unsigned");
        COLUMN_TYPE_MAP.put("c_bigint_1", "  bigint(1)");
        COLUMN_TYPE_MAP.put("c_bigint_64", "  bigint(64)");
        COLUMN_TYPE_MAP.put("c_bigint_64_un", "  bigint(64) unsigned");
        COLUMN_TYPE_MAP.put("c_decimal", "  decimal");
        COLUMN_TYPE_MAP.put("c_decimal_pr", "  decimal(65,30)");
        COLUMN_TYPE_MAP.put("c_float", "  float");
        COLUMN_TYPE_MAP.put("c_float_pr", "  float(10,3)");
        COLUMN_TYPE_MAP.put("c_float_un", "  float(10,3) unsigned");
        COLUMN_TYPE_MAP.put("c_double", "  double");
        COLUMN_TYPE_MAP.put("c_double_pr", "  double(10,3)");
        COLUMN_TYPE_MAP.put("c_double_un", "  double(10,3) unsigned");
        COLUMN_TYPE_MAP.put("c_date", "  date");
        COLUMN_TYPE_MAP.put("c_datetime", "  datetime");
        COLUMN_TYPE_MAP.put("c_datetime_1", "  datetime(1)");
        COLUMN_TYPE_MAP.put("c_datetime_3", "  datetime(3)");
        COLUMN_TYPE_MAP.put("c_datetime_6", "  datetime(6)");
        COLUMN_TYPE_MAP.put("c_timestamp", "  timestamp");
        COLUMN_TYPE_MAP.put("c_timestamp_1", "  timestamp(1)");
        COLUMN_TYPE_MAP.put("c_timestamp_3", "  timestamp(3)");
        COLUMN_TYPE_MAP.put("c_timestamp_6", "  timestamp(6)");
        COLUMN_TYPE_MAP.put("c_time", "  time");
        COLUMN_TYPE_MAP.put("c_time_1", "  time(1)");
        COLUMN_TYPE_MAP.put("c_time_3", "  time(3)");
        COLUMN_TYPE_MAP.put("c_time_6", "  time(6)");
        COLUMN_TYPE_MAP.put("c_year", "  year");
        COLUMN_TYPE_MAP.put("c_year_4", "  year(4)");
        COLUMN_TYPE_MAP.put("c_char", "  char(10)");
        COLUMN_TYPE_MAP.put("c_varchar", "  varchar(10)");
        COLUMN_TYPE_MAP.put("c_binary", "  binary(10)");
        COLUMN_TYPE_MAP.put("c_varbinary", "  varbinary(10)");
        COLUMN_TYPE_MAP.put("c_blob_tiny", "  tinyblob");
        COLUMN_TYPE_MAP.put("c_blob", "  blob");
        COLUMN_TYPE_MAP.put("c_blob_medium", "  mediumblob");
        COLUMN_TYPE_MAP.put("c_blob_long", "  longblob");
        COLUMN_TYPE_MAP.put("c_text_tiny", "  tinytext");
        COLUMN_TYPE_MAP.put("c_text", "  text");
        COLUMN_TYPE_MAP.put("c_text_medium", "  mediumtext");
        COLUMN_TYPE_MAP.put("c_text_long", "  longtext");
        COLUMN_TYPE_MAP.put("c_enum", "  enum(\"a\",\"b\",\"c\")");
        COLUMN_TYPE_MAP.put("c_set", "  set(\"a\",\"b\",\"c\")");
        COLUMN_TYPE_MAP.put("c_json", "  json");
        COLUMN_TYPE_MAP.put("c_geometory", "  geometry");
        COLUMN_TYPE_MAP.put("c_point", "  point");
        COLUMN_TYPE_MAP.put("c_linestring", "  linestring");
        COLUMN_TYPE_MAP.put("c_polygon", "  polygon");
        COLUMN_TYPE_MAP.put("c_multipoint", "  multipoint");
        COLUMN_TYPE_MAP.put("c_multilinestring", "  multilinestring");
        COLUMN_TYPE_MAP.put("c_multipolygon", "  multipolygon");
        COLUMN_TYPE_MAP.put("c_geometrycollection", "  geometrycollection");
    }
}
