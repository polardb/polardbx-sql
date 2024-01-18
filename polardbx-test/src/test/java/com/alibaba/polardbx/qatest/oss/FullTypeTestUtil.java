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

package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.truth.Truth.assertWithMessage;

public class FullTypeTestUtil {

    private static String TABLE_DEFINITION = "CREATE TABLE `%s` (\n" +
        "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
        "\t`c_bit_1` bit(1) DEFAULT NULL,\n" +
        "\t`c_bit_8` bit(8) DEFAULT NULL,\n" +
        "\t`c_bit_16` bit(16) DEFAULT NULL,\n" +
        "\t`c_bit_32` bit(32) DEFAULT NULL,\n" +
        "\t`c_bit_64` bit(64) DEFAULT NULL,\n" +
        "\t`c_tinyint_1` tinyint(1) DEFAULT NULL,\n" +
        "\t`c_tinyint_1_un` tinyint(1) UNSIGNED DEFAULT NULL,\n" +
        "\t`c_tinyint_4` tinyint(4) DEFAULT NULL,\n" +
        "\t`c_tinyint_4_un` tinyint(4) UNSIGNED DEFAULT NULL,\n" +
        "\t`c_tinyint_8` tinyint(8) DEFAULT NULL,\n" +
        "\t`c_tinyint_8_un` tinyint(8) UNSIGNED DEFAULT NULL,\n" +
        "\t`c_smallint_1` smallint(1) DEFAULT NULL,\n" +
        "\t`c_smallint_16` smallint(16) DEFAULT NULL,\n" +
        "\t`c_smallint_16_un` smallint(16) UNSIGNED DEFAULT NULL,\n" +
        "\t`c_mediumint_1` mediumint(1) DEFAULT NULL,\n" +
        "\t`c_mediumint_24` mediumint(24) DEFAULT NULL,\n" +
        "\t`c_mediumint_24_un` mediumint(24) UNSIGNED DEFAULT NULL,\n" +
        "\t`c_int_1` int(1) DEFAULT NULL,\n" +
        "\t`c_int_32` int(32) NOT NULL DEFAULT '0' COMMENT 'For multi pk.',\n" +
        "\t`c_int_32_un` int(32) UNSIGNED DEFAULT NULL,\n" +
        "\t`c_bigint_1` bigint(1) DEFAULT NULL,\n" +
        "\t`c_bigint_64` bigint(64) DEFAULT NULL,\n" +
        "\t`c_bigint_64_un` bigint(64) UNSIGNED DEFAULT NULL,\n" +
        "\t`c_decimal` decimal(10, 0) DEFAULT NULL,\n" +
        "\t`c_decimal_pr` decimal(65, 30) DEFAULT NULL,\n" +
        "\t`c_float` float DEFAULT NULL,\n" +
        "\t`c_float_pr` float(10, 3) DEFAULT NULL,\n" +
        "\t`c_float_un` float(10, 3) UNSIGNED DEFAULT NULL,\n" +
        "\t`c_double` double DEFAULT NULL,\n" +
        "\t`c_double_pr` double(10, 3) DEFAULT NULL,\n" +
        "\t`c_double_un` double(10, 3) UNSIGNED DEFAULT NULL,\n" +
        "\t`c_date` date DEFAULT NULL COMMENT 'date',\n" +
        "\t`c_datetime` datetime DEFAULT NULL,\n" +
        "\t`c_datetime_1` datetime(1) DEFAULT NULL,\n" +
        "\t`c_datetime_3` datetime(3) DEFAULT NULL,\n" +
        "\t`c_datetime_6` datetime(6) DEFAULT NULL,\n" +
        "\t`c_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
        "\t`c_timestamp_1` timestamp(1) NOT NULL DEFAULT '1999-12-31 12:00:00.0',\n" +
        "\t`c_timestamp_3` timestamp(3) NOT NULL DEFAULT '1999-12-31 12:00:00.000',\n" +
        "\t`c_timestamp_6` timestamp(6) NOT NULL DEFAULT '1999-12-31 12:00:00.000000',\n" +
        "\t`c_time` time DEFAULT NULL,\n" +
        "\t`c_time_1` time(1) DEFAULT NULL,\n" +
        "\t`c_time_3` time(3) DEFAULT NULL,\n" +
        "\t`c_time_6` time(6) DEFAULT NULL,\n" +
        "\t`c_year` year(4) DEFAULT NULL,\n" +
        "\t`c_year_4` year(4) DEFAULT NULL,\n" +
        "\t`c_char` char(10) DEFAULT NULL,\n" +
        "\t`c_varchar` varchar(10) DEFAULT NULL,\n" +
        "\t`c_binary` binary(10) DEFAULT NULL,\n" +
        "\t`c_varbinary` varbinary(10) DEFAULT NULL,\n" +
        "\t`c_blob_tiny` tinyblob,\n" +
        "\t`c_blob` blob,\n" +
        "\t`c_blob_medium` mediumblob,\n" +
        "\t`c_blob_long` longblob,\n" +
        "\t`c_text_tiny` tinytext,\n" +
        "\t`c_text` text,\n" +
        "\t`c_text_medium` mediumtext,\n" +
        "\t`c_text_long` longtext,\n" +
        "\t`c_enum` enum('a', 'b', 'c') DEFAULT NULL,\n" +
//            "\t`c_set` set('a', 'b', 'c') DEFAULT NULL,\n" +
        "\t`c_json` json DEFAULT NULL,\n" +
        "\t`c_geometory` geometry DEFAULT NULL,\n" +
        "\t`c_point` point DEFAULT NULL,\n" +
        "\t`c_linestring` linestring DEFAULT NULL,\n" +
        "\t`c_polygon` polygon DEFAULT NULL,\n" +
        "\t`c_multipoint` multipoint DEFAULT NULL,\n" +
        "\t`c_multilinestring` multilinestring DEFAULT NULL,\n" +
        "\t`c_multipolygon` multipolygon DEFAULT NULL,\n" +
        "\t`c_geometrycollection` geometrycollection DEFAULT NULL,\n" +
        "\tPRIMARY KEY (`id`),\n" +
        "\tKEY `idx_c_double` (`c_double`),\n" +
        "\tKEY `idx_c_float` (`c_float`)\n" +
        ") ENGINE = 'INNODB' AUTO_INCREMENT = 100281 DEFAULT CHARSET = utf8mb4 COMMENT '10000000'\n" +
        "PARTITION BY KEY(`id`)\n" +
        "PARTITIONS 3;";

    public static void createInnodbTableWithData(Connection connection, String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(TABLE_DEFINITION, tableName));
            ImmutableMap<String, List<String>> map = buildGsiFullTypeTestInserts(tableName);
            for (List<String> sqls : map.values()) {
                for (String sql : sqls) {
                    try {
                        statement.execute(sql);
                    } catch (SQLException sqlException) {
                        // ignore
                    }
                }
            }
        }
    }

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
//        builder.put(C_SET,
//                ImmutableList.of("insert into " + primaryTableName + "(id,c_set) values(null,'a');\n",
//                        "insert into " + primaryTableName + "(id,c_set) values(null,'b,a');\n",
//                        "insert into " + primaryTableName + "(id,c_set) values(null,'b,c,a');\n",
//                        "insert into " + primaryTableName + "(id,c_set) values(null,'d');\n",
//                        "insert into " + primaryTableName + "(id,c_set) values(null,NULL);\n"));
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

    static public long count(Connection conn, String tableName) throws SQLException {
        try (ResultSet resultSet = JdbcUtil.executeQuery("select count(*) from " + tableName, conn)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    static public void prepareInnoTable(Connection conn, String innodbTestTableName, int size) {
        prepareInnoTable(conn, innodbTestTableName, 300, size);
    }

    static public void prepareInnoTable(Connection conn, String innodbTestTableName, int limit, int size) {
        LocalDate now = LocalDate.now();
        LocalDate startWithDate = now.minusMonths(12L);
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    id bigint NOT NULL AUTO_INCREMENT,\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATE NOT NULL,\n"
            + "    PRIMARY KEY (gmt_modified, id),\n"
            + "    index(c2, c3)\n"
            + ")\n"
            + "PARTITION BY HASH(id)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 3\n"
            + "PRE ALLOCATE 3\n"
            + ";", innodbTestTableName, startWithDate);
        JdbcUtil.executeSuccess(conn, createTableSql);
        LocalDate dateIterator = startWithDate.minusMonths(1);
        int i = 0;
        Random r1 = new Random();
        String insertSql = "INSERT INTO " + innodbTestTableName +
            " (c1, c2, c3, gmt_modified) VALUES (?, ?, ?, ?)";

        while (dateIterator.isBefore(now) || dateIterator.equals(now)) {
            for (int j = 0; j < size; j = j + limit) {
                List<List<Object>> params = new ArrayList<>();
                for (int k = 0; k < limit; k++) {
                    List<Object> para = new ArrayList<>();
                    para.add(r1.nextInt());
                    para.add(r1.nextInt());
                    para.add(r1.nextInt());
                    para.add(dateIterator);
                    params.add(para);
                }
                JdbcUtil.updateDataBatch(conn, insertSql, params);
            }
            dateIterator = startWithDate.plusMonths(++i);
        }
    }

    public static void pauseSchedule(Connection conn, String schema, String innodbTestTableName) throws SQLException {
        // pause the schedule job
        String findTTL = String.format("select schedule_id from metadb.scheduled_jobs where table_schema = '%s' "
                + "and table_name = '%s' and executor_type = '%s' "
            , schema, innodbTestTableName, "LOCAL_PARTITION");
        try (ResultSet resultSet = JdbcUtil.executeQuery(findTTL, conn)) {
            Assert.assertTrue(resultSet.next());
            long id = resultSet.getLong(1);
            JdbcUtil.executeQuery("pause schedule " + id, conn).close();
        }
    }

    public static void continueSchedule(Connection conn, String schema, String innodbTestTableName)
        throws SQLException {
        // pause the schedule job
        String findTTL = String.format("select schedule_id from metadb.scheduled_jobs where table_schema = '%s' "
                + "and table_name = '%s' and executor_type = '%s' "
            , schema, innodbTestTableName, "LOCAL_PARTITION");
        try (ResultSet resultSet = JdbcUtil.executeQuery(findTTL, conn)) {
            Assert.assertTrue(resultSet.next());
            long id = resultSet.getLong(1);
            JdbcUtil.executeQuery("continue schedule " + id, conn).close();
        }
    }

    /**
     * create oss table from innodb table, schedule expire ttl is closed by default
     */
    static public void prepareTTLTable(Connection conn, String ossSchema, String ossTestTableName,
                                       String innoSchema, String innodbTestTableName,
                                       Engine engine) throws SQLException {
        JdbcUtil.executeSuccess(conn,
            String.format("create table %s like %s.%s engine = '%s' archive_mode = 'ttl';", ossTestTableName,
                innoSchema, innodbTestTableName, engine.name()));
        pauseSchedule(conn, innoSchema, innodbTestTableName);
        assertWithMessage("table " + ossSchema + "." + ossTestTableName + " is not empty").that(
            FullTypeTestUtil.count(conn, ossTestTableName)).isEqualTo(0);
    }

    static public void prepareTTLTable(Connection conn, String schema,
                                       String ossTestTableName, String innodbTestTableName,
                                       Engine engine) throws SQLException {
        JdbcUtil.executeSuccess(conn,
            String.format("create table %s like %s engine = '%s' archive_mode = 'ttl';", ossTestTableName,
                innodbTestTableName, engine.name()));
        pauseSchedule(conn, schema, innodbTestTableName);
        assertWithMessage("table " + ossTestTableName + " is not empty").that(
            FullTypeTestUtil.count(conn, ossTestTableName)).isEqualTo(0);
    }

    static public long fetchJobId(Connection conn, String schema, String table) throws SQLException {
        ResultSet resultSet = JdbcUtil.executeQuery(
            String.format("select job_id from metadb.ddl_engine where schema_name = '%s' and object_name = '%s'",
                schema,
                table), conn);
        Assert.assertTrue(resultSet.next());
        long id = resultSet.getLong(1);
        resultSet.close();
        return id;
    }
}
