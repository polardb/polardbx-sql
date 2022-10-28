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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FullTypeSeparatedTestUtil {
    private static String TABLE_DEFINITION_FORMAT = "CREATE TABLE `%s` (\n" +
        "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
        "%s" +
        "\tPRIMARY KEY (`id`)\n" +
        ") ENGINE = 'INNODB' AUTO_INCREMENT = 100281 DEFAULT CHARSET = utf8mb4 COMMENT '10000000'\n" +
        "PARTITION BY KEY(`id`)\n" +
        "PARTITIONS 3;";

    private static ImmutableMap<String, List<String>> allInsertionFormats =
        FullTypeTestUtil.buildGsiFullTypeTestInserts("%s");

    private static ImmutableMap<String, String> allColumnDefinitions = ImmutableMap.<String, String>builder()
        .put("c_bit_1", "\t`c_bit_1` bit(1) DEFAULT NULL,\n")
        .put("c_bit_8", "\t`c_bit_8` bit(8) DEFAULT NULL,\n")
        .put("c_bit_16", "\t`c_bit_16` bit(16) DEFAULT NULL,\n")
        .put("c_bit_32", "\t`c_bit_32` bit(32) DEFAULT NULL,\n")
        .put("c_bit_64", "\t`c_bit_64` bit(64) DEFAULT NULL,\n")
        .put("c_tinyint_1", "\t`c_tinyint_1` tinyint(1) DEFAULT NULL,\n")
        .put("c_tinyint_1_un", "\t`c_tinyint_1_un` tinyint(1) UNSIGNED DEFAULT NULL,\n")
        .put("c_tinyint_4", "\t`c_tinyint_4` tinyint(4) DEFAULT NULL,\n")
        .put("c_tinyint_4_un", "\t`c_tinyint_4_un` tinyint(4) UNSIGNED DEFAULT NULL,\n")
        .put("c_tinyint_8", "\t`c_tinyint_8` tinyint(8) DEFAULT NULL,\n")
        .put("c_tinyint_8_un", "\t`c_tinyint_8_un` tinyint(8) UNSIGNED DEFAULT NULL,\n")
        .put("c_smallint_1", "\t`c_smallint_1` smallint(1) DEFAULT NULL,\n")
        .put("c_smallint_16", "\t`c_smallint_16` smallint(16) DEFAULT NULL,\n")
        .put("c_smallint_16_un", "\t`c_smallint_16_un` smallint(16) UNSIGNED DEFAULT NULL,\n")
        .put("c_mediumint_1", "\t`c_mediumint_1` mediumint(1) DEFAULT NULL,\n")
        .put("c_mediumint_24", "\t`c_mediumint_24` mediumint(24) DEFAULT NULL,\n")
        .put("c_mediumint_24_un", "\t`c_mediumint_24_un` mediumint(24) UNSIGNED DEFAULT NULL,\n")
        .put("c_int_1", "\t`c_int_1` int(1) DEFAULT NULL,\n")
        .put("c_int_32", "\t`c_int_32` int(32) NOT NULL DEFAULT '0' COMMENT 'For multi pk.',\n")
        .put("c_int_32_un", "\t`c_int_32_un` int(32) UNSIGNED DEFAULT NULL,\n")
        .put("c_bigint_1", "\t`c_bigint_1` bigint(1) DEFAULT NULL,\n")
        .put("c_bigint_64", "\t`c_bigint_64` bigint(64) DEFAULT NULL,\n")
        .put("c_bigint_64_un", "\t`c_bigint_64_un` bigint(64) UNSIGNED DEFAULT NULL,\n")
        .put("c_decimal", "\t`c_decimal` decimal(10, 0) DEFAULT NULL,\n")
        .put("c_decimal_pr", "\t`c_decimal_pr` decimal(65, 30) DEFAULT NULL,\n")
        .put("c_float", "\t`c_float` float DEFAULT NULL,\n")
        .put("c_float_pr", "\t`c_float_pr` float(10, 3) DEFAULT NULL,\n")
        .put("c_float_un", "\t`c_float_un` float(10, 3) UNSIGNED DEFAULT NULL,\n")
        .put("c_double", "\t`c_double` double DEFAULT NULL,\n")
        .put("c_double_pr", "\t`c_double_pr` double(10, 3) DEFAULT NULL,\n")
        .put("c_double_un", "\t`c_double_un` double(10, 3) UNSIGNED DEFAULT NULL,\n")
        .put("c_date", "\t`c_date` date DEFAULT NULL COMMENT 'date',\n")
        .put("c_datetime", "\t`c_datetime` datetime DEFAULT NULL,\n")
        .put("c_datetime_1", "\t`c_datetime_1` datetime(1) DEFAULT NULL,\n")
        .put("c_datetime_3", "\t`c_datetime_3` datetime(3) DEFAULT NULL,\n")
        .put("c_datetime_6", "\t`c_datetime_6` datetime(6) DEFAULT NULL,\n")
        .put("c_timestamp", "\t`c_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n")
        .put("c_timestamp_1", "\t`c_timestamp_1` timestamp(1) NOT NULL DEFAULT '1999-12-31 12:00:00.0',\n")
        .put("c_timestamp_3", "\t`c_timestamp_3` timestamp(3) NOT NULL DEFAULT '1999-12-31 12:00:00.000',\n")
        .put("c_timestamp_6", "\t`c_timestamp_6` timestamp(6) NOT NULL DEFAULT '1999-12-31 12:00:00.000000',\n")
        .put("c_time", "\t`c_time` time DEFAULT NULL,\n")
        .put("c_time_1", "\t`c_time_1` time(1) DEFAULT NULL,\n")
        .put("c_time_3", "\t`c_time_3` time(3) DEFAULT NULL,\n")
        .put("c_time_6", "\t`c_time_6` time(6) DEFAULT NULL,\n")
        .put("c_year", "\t`c_year` year(4) DEFAULT NULL,\n")
        .put("c_year_4", "\t`c_year_4` year(4) DEFAULT NULL,\n")
        .put("c_char", "\t`c_char` char(10) DEFAULT NULL,\n")
        .put("c_varchar", "\t`c_varchar` varchar(10) DEFAULT NULL,\n")
        .put("c_binary", "\t`c_binary` binary(10) DEFAULT NULL,\n")
        .put("c_varbinary", "\t`c_varbinary` varbinary(10) DEFAULT NULL,\n")
        .put("c_blob_tiny", "\t`c_blob_tiny` tinyblob,\n")
        .put("c_blob", "\t`c_blob` blob,\n")
        .put("c_blob_medium", "\t`c_blob_medium` mediumblob,\n")
        .put("c_blob_long", "\t`c_blob_long` longblob,\n")
        .put("c_text_tiny", "\t`c_text_tiny` tinytext,\n")
        .put("c_text", "\t`c_text` text,\n")
        .put("c_text_medium", "\t`c_text_medium` mediumtext,\n")
        .put("c_text_long", "\t`c_text_long` longtext,\n")
        .put("c_enum", "\t`c_enum` enum('a', 'b', 'c') DEFAULT NULL,\n")
//        .put("c_set", "\t`c_set` set('a', 'b', 'c') DEFAULT NULL,\n")
        .put("c_json", "\t`c_json` json DEFAULT NULL,\n")
        .put("c_geometory", "\t`c_geometory` geometry DEFAULT NULL,\n")
        .put("c_point", "\t`c_point` point DEFAULT NULL,\n")
        .put("c_linestring", "\t`c_linestring` linestring DEFAULT NULL,\n")
        .put("c_polygon", "\t`c_polygon` polygon DEFAULT NULL,\n")
        .put("c_multipoint", "\t`c_multipoint` multipoint DEFAULT NULL,\n")
        .put("c_multilinestring", "\t`c_multilinestring` multilinestring DEFAULT NULL,\n")
        .put("c_multipolygon", "\t`c_multipolygon` multipolygon DEFAULT NULL,\n")
        .put("c_geometrycollection", "\t`c_geometrycollection` geometrycollection DEFAULT NULL,\n")
        .build();

    private static List<String> allColumns = ImmutableList.of(
        "c_bit_1",
        "c_bit_8",
        "c_bit_16",
        "c_bit_32",
        "c_bit_64",
        "c_tinyint_1",
        "c_tinyint_1_un",
        "c_tinyint_4",
        "c_tinyint_4_un",
        "c_tinyint_8",
        "c_tinyint_8_un",
        "c_smallint_1",
        "c_smallint_16",
        "c_smallint_16_un",
        "c_mediumint_1",
        "c_mediumint_24",
        "c_mediumint_24_un",
        "c_int_1",
        "c_int_32",
        "c_int_32_un",
        "c_bigint_1",
        "c_bigint_64",
        "c_bigint_64_un",
        "c_decimal",
        "c_decimal_pr",
        "c_float",
        "c_float_pr",
        "c_float_un",
        "c_double",
        "c_double_pr",
        "c_double_un",
        "c_date",
        "c_datetime",
        "c_datetime_1",
        "c_datetime_3",
        "c_datetime_6",
        "c_timestamp",
        "c_timestamp_1",
        "c_timestamp_3",
        "c_timestamp_6",
        "c_time",
        "c_time_1",
        "c_time_3",
        "c_time_6",
        "c_year",
        "c_year_4",
        "c_char",
        "c_varchar",
        "c_binary",
        "c_varbinary",
        "c_blob_tiny",
        "c_blob",
        "c_blob_medium",
        "c_blob_long",
        "c_text_tiny",
        "c_text",
        "c_text_medium",
        "c_text_long",
        "c_enum",
//        "c_set",
        "c_json",
        "c_geometory",
        "c_point",
        "c_linestring",
        "c_polygon",
        "c_multipoint",
        "c_multilinestring",
        "c_multipolygon",
        "c_geometrycollection"
    );

    private static Map<String, String> allTableDefinitions =
        allColumns.stream()
            .collect(
                Collectors.toMap(c -> c,
                    c -> String.format(TABLE_DEFINITION_FORMAT, tableNameByColumn(c), allColumnDefinitions.get(c))));

    private static Map<String, List<String>> allInsertions =
        allColumns.stream().collect(
            Collectors.toMap(c -> c,
                c -> allInsertionFormats.get(c).stream().map(format -> String.format(format, tableNameByColumn(c)))
                    .collect(Collectors.toList())));

    public static String tableNameByColumn(String columnName) {
        return "t_" + columnName;
    }

    public static void createInnodbTableWithData(Connection connection, TypeItem typeItem) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(typeItem.getTableDefinition());

            List<String> insertionList = typeItem.getDataInsertion();
            for (String sql: insertionList) {
                try {
                    statement.execute(sql);
                } catch (SQLException sqlException) {
                    // ignore
                }
            }
        }
    }

    enum TypeItem {
        C_BIT_1(FullTypeTestUtil.C_BIT_1, allTableDefinitions.get(FullTypeTestUtil.C_BIT_1),
            allInsertions.get(FullTypeTestUtil.C_BIT_1)),
        C_BIT_8(FullTypeTestUtil.C_BIT_8, allTableDefinitions.get(FullTypeTestUtil.C_BIT_8),
            allInsertions.get(FullTypeTestUtil.C_BIT_8)),
        C_BIT_16(FullTypeTestUtil.C_BIT_16, allTableDefinitions.get(FullTypeTestUtil.C_BIT_16),
            allInsertions.get(FullTypeTestUtil.C_BIT_16)),
        C_BIT_32(FullTypeTestUtil.C_BIT_32, allTableDefinitions.get(FullTypeTestUtil.C_BIT_32),
            allInsertions.get(FullTypeTestUtil.C_BIT_32)),
        C_BIT_64(FullTypeTestUtil.C_BIT_64, allTableDefinitions.get(FullTypeTestUtil.C_BIT_64),
            allInsertions.get(FullTypeTestUtil.C_BIT_64)),
        C_TINYINT_1(FullTypeTestUtil.C_TINYINT_1, allTableDefinitions.get(FullTypeTestUtil.C_TINYINT_1),
            allInsertions.get(FullTypeTestUtil.C_TINYINT_1)),
        C_TINYINT_1_UN(FullTypeTestUtil.C_TINYINT_1_UN, allTableDefinitions.get(FullTypeTestUtil.C_TINYINT_1_UN),
            allInsertions.get(FullTypeTestUtil.C_TINYINT_1_UN)),
        C_TINYINT_4(FullTypeTestUtil.C_TINYINT_4, allTableDefinitions.get(FullTypeTestUtil.C_TINYINT_4),
            allInsertions.get(FullTypeTestUtil.C_TINYINT_4)),
        C_TINYINT_4_UN(FullTypeTestUtil.C_TINYINT_4_UN, allTableDefinitions.get(FullTypeTestUtil.C_TINYINT_4_UN),
            allInsertions.get(FullTypeTestUtil.C_TINYINT_4_UN)),
        C_TINYINT_8(FullTypeTestUtil.C_TINYINT_8, allTableDefinitions.get(FullTypeTestUtil.C_TINYINT_8),
            allInsertions.get(FullTypeTestUtil.C_TINYINT_8)),
        C_TINYINT_8_UN(FullTypeTestUtil.C_TINYINT_8_UN, allTableDefinitions.get(FullTypeTestUtil.C_TINYINT_8_UN),
            allInsertions.get(FullTypeTestUtil.C_TINYINT_8_UN)),
        C_SMALLINT_1(FullTypeTestUtil.C_SMALLINT_1, allTableDefinitions.get(FullTypeTestUtil.C_SMALLINT_1),
            allInsertions.get(FullTypeTestUtil.C_SMALLINT_1)),
        C_SMALLINT_16(FullTypeTestUtil.C_SMALLINT_16, allTableDefinitions.get(FullTypeTestUtil.C_SMALLINT_16),
            allInsertions.get(FullTypeTestUtil.C_SMALLINT_16)),
        C_SMALLINT_16_UN(FullTypeTestUtil.C_SMALLINT_16_UN, allTableDefinitions.get(FullTypeTestUtil.C_SMALLINT_16_UN),
            allInsertions.get(FullTypeTestUtil.C_SMALLINT_16_UN)),
        C_MEDIUMINT_1(FullTypeTestUtil.C_MEDIUMINT_1, allTableDefinitions.get(FullTypeTestUtil.C_MEDIUMINT_1),
            allInsertions.get(FullTypeTestUtil.C_MEDIUMINT_1)),
        C_MEDIUMINT_24(FullTypeTestUtil.C_MEDIUMINT_24, allTableDefinitions.get(FullTypeTestUtil.C_MEDIUMINT_24),
            allInsertions.get(FullTypeTestUtil.C_MEDIUMINT_24)),
        C_MEDIUMINT_24_UN(FullTypeTestUtil.C_MEDIUMINT_24_UN,
            allTableDefinitions.get(FullTypeTestUtil.C_MEDIUMINT_24_UN),
            allInsertions.get(FullTypeTestUtil.C_MEDIUMINT_24_UN)),
        C_INT_1(FullTypeTestUtil.C_INT_1, allTableDefinitions.get(FullTypeTestUtil.C_INT_1),
            allInsertions.get(FullTypeTestUtil.C_INT_1)),
        C_INT_32(FullTypeTestUtil.C_INT_32, allTableDefinitions.get(FullTypeTestUtil.C_INT_32),
            allInsertions.get(FullTypeTestUtil.C_INT_32)),
        C_INT_32_UN(FullTypeTestUtil.C_INT_32_UN, allTableDefinitions.get(FullTypeTestUtil.C_INT_32_UN),
            allInsertions.get(FullTypeTestUtil.C_INT_32_UN)),
        C_BIGINT_1(FullTypeTestUtil.C_BIGINT_1, allTableDefinitions.get(FullTypeTestUtil.C_BIGINT_1),
            allInsertions.get(FullTypeTestUtil.C_BIGINT_1)),
        C_BIGINT_64(FullTypeTestUtil.C_BIGINT_64, allTableDefinitions.get(FullTypeTestUtil.C_BIGINT_64),
            allInsertions.get(FullTypeTestUtil.C_BIGINT_64)),
        C_BIGINT_64_UN(FullTypeTestUtil.C_BIGINT_64_UN, allTableDefinitions.get(FullTypeTestUtil.C_BIGINT_64_UN),
            allInsertions.get(FullTypeTestUtil.C_BIGINT_64_UN)),
        C_DECIMAL(FullTypeTestUtil.C_DECIMAL, allTableDefinitions.get(FullTypeTestUtil.C_DECIMAL),
            allInsertions.get(FullTypeTestUtil.C_DECIMAL)),
        C_DECIMAL_PR(FullTypeTestUtil.C_DECIMAL_PR, allTableDefinitions.get(FullTypeTestUtil.C_DECIMAL_PR),
            allInsertions.get(FullTypeTestUtil.C_DECIMAL_PR)),
        C_FLOAT(FullTypeTestUtil.C_FLOAT, allTableDefinitions.get(FullTypeTestUtil.C_FLOAT),
            allInsertions.get(FullTypeTestUtil.C_FLOAT)),
        C_FLOAT_PR(FullTypeTestUtil.C_FLOAT_PR, allTableDefinitions.get(FullTypeTestUtil.C_FLOAT_PR),
            allInsertions.get(FullTypeTestUtil.C_FLOAT_PR)),
        C_FLOAT_UN(FullTypeTestUtil.C_FLOAT_UN, allTableDefinitions.get(FullTypeTestUtil.C_FLOAT_UN),
            allInsertions.get(FullTypeTestUtil.C_FLOAT_UN)),
        C_DOUBLE(FullTypeTestUtil.C_DOUBLE, allTableDefinitions.get(FullTypeTestUtil.C_DOUBLE),
            allInsertions.get(FullTypeTestUtil.C_DOUBLE)),
        C_DOUBLE_PR(FullTypeTestUtil.C_DOUBLE_PR, allTableDefinitions.get(FullTypeTestUtil.C_DOUBLE_PR),
            allInsertions.get(FullTypeTestUtil.C_DOUBLE_PR)),
        C_DOUBLE_UN(FullTypeTestUtil.C_DOUBLE_UN, allTableDefinitions.get(FullTypeTestUtil.C_DOUBLE_UN),
            allInsertions.get(FullTypeTestUtil.C_DOUBLE_UN)),
        C_DATE(FullTypeTestUtil.C_DATE, allTableDefinitions.get(FullTypeTestUtil.C_DATE),
            allInsertions.get(FullTypeTestUtil.C_DATE)),
        C_DATETIME(FullTypeTestUtil.C_DATETIME, allTableDefinitions.get(FullTypeTestUtil.C_DATETIME),
            allInsertions.get(FullTypeTestUtil.C_DATETIME)),
        C_DATETIME_1(FullTypeTestUtil.C_DATETIME_1, allTableDefinitions.get(FullTypeTestUtil.C_DATETIME_1),
            allInsertions.get(FullTypeTestUtil.C_DATETIME_1)),
        C_DATETIME_3(FullTypeTestUtil.C_DATETIME_3, allTableDefinitions.get(FullTypeTestUtil.C_DATETIME_3),
            allInsertions.get(FullTypeTestUtil.C_DATETIME_3)),
        C_DATETIME_6(FullTypeTestUtil.C_DATETIME_6, allTableDefinitions.get(FullTypeTestUtil.C_DATETIME_6),
            allInsertions.get(FullTypeTestUtil.C_DATETIME_6)),
        C_TIMESTAMP(FullTypeTestUtil.C_TIMESTAMP, allTableDefinitions.get(FullTypeTestUtil.C_TIMESTAMP),
            allInsertions.get(FullTypeTestUtil.C_TIMESTAMP)),
        C_TIMESTAMP_1(FullTypeTestUtil.C_TIMESTAMP_1, allTableDefinitions.get(FullTypeTestUtil.C_TIMESTAMP_1),
            allInsertions.get(FullTypeTestUtil.C_TIMESTAMP_1)),
        C_TIMESTAMP_3(FullTypeTestUtil.C_TIMESTAMP_3, allTableDefinitions.get(FullTypeTestUtil.C_TIMESTAMP_3),
            allInsertions.get(FullTypeTestUtil.C_TIMESTAMP_3)),
        C_TIMESTAMP_6(FullTypeTestUtil.C_TIMESTAMP_6, allTableDefinitions.get(FullTypeTestUtil.C_TIMESTAMP_6),
            allInsertions.get(FullTypeTestUtil.C_TIMESTAMP_6)),
        C_TIME(FullTypeTestUtil.C_TIME, allTableDefinitions.get(FullTypeTestUtil.C_TIME),
            allInsertions.get(FullTypeTestUtil.C_TIME)),
        C_TIME_1(FullTypeTestUtil.C_TIME_1, allTableDefinitions.get(FullTypeTestUtil.C_TIME_1),
            allInsertions.get(FullTypeTestUtil.C_TIME_1)),
        C_TIME_3(FullTypeTestUtil.C_TIME_3, allTableDefinitions.get(FullTypeTestUtil.C_TIME_3),
            allInsertions.get(FullTypeTestUtil.C_TIME_3)),
        C_TIME_6(FullTypeTestUtil.C_TIME_6, allTableDefinitions.get(FullTypeTestUtil.C_TIME_6),
            allInsertions.get(FullTypeTestUtil.C_TIME_6)),
        C_YEAR(FullTypeTestUtil.C_YEAR, allTableDefinitions.get(FullTypeTestUtil.C_YEAR),
            allInsertions.get(FullTypeTestUtil.C_YEAR)),
        C_YEAR_4(FullTypeTestUtil.C_YEAR_4, allTableDefinitions.get(FullTypeTestUtil.C_YEAR_4),
            allInsertions.get(FullTypeTestUtil.C_YEAR_4)),
        C_CHAR(FullTypeTestUtil.C_CHAR, allTableDefinitions.get(FullTypeTestUtil.C_CHAR),
            allInsertions.get(FullTypeTestUtil.C_CHAR)),
        C_VARCHAR(FullTypeTestUtil.C_VARCHAR, allTableDefinitions.get(FullTypeTestUtil.C_VARCHAR),
            allInsertions.get(FullTypeTestUtil.C_VARCHAR)),
        C_BINARY(FullTypeTestUtil.C_BINARY, allTableDefinitions.get(FullTypeTestUtil.C_BINARY),
            allInsertions.get(FullTypeTestUtil.C_BINARY)),
        C_VARBINARY(FullTypeTestUtil.C_VARBINARY, allTableDefinitions.get(FullTypeTestUtil.C_VARBINARY),
            allInsertions.get(FullTypeTestUtil.C_VARBINARY)),
        C_BLOB_TINY(FullTypeTestUtil.C_BLOB_TINY, allTableDefinitions.get(FullTypeTestUtil.C_BLOB_TINY),
            allInsertions.get(FullTypeTestUtil.C_BLOB_TINY)),
        C_BLOB(FullTypeTestUtil.C_BLOB, allTableDefinitions.get(FullTypeTestUtil.C_BLOB),
            allInsertions.get(FullTypeTestUtil.C_BLOB)),
        C_BLOB_MEDIUM(FullTypeTestUtil.C_BLOB_MEDIUM, allTableDefinitions.get(FullTypeTestUtil.C_BLOB_MEDIUM),
            allInsertions.get(FullTypeTestUtil.C_BLOB_MEDIUM)),
        C_BLOB_LONG(FullTypeTestUtil.C_BLOB_LONG, allTableDefinitions.get(FullTypeTestUtil.C_BLOB_LONG),
            allInsertions.get(FullTypeTestUtil.C_BLOB_LONG)),
        C_TEXT_TINY(FullTypeTestUtil.C_TEXT_TINY, allTableDefinitions.get(FullTypeTestUtil.C_TEXT_TINY),
            allInsertions.get(FullTypeTestUtil.C_TEXT_TINY)),
        C_TEXT(FullTypeTestUtil.C_TEXT, allTableDefinitions.get(FullTypeTestUtil.C_TEXT),
            allInsertions.get(FullTypeTestUtil.C_TEXT)),
        C_TEXT_MEDIUM(FullTypeTestUtil.C_TEXT_MEDIUM, allTableDefinitions.get(FullTypeTestUtil.C_TEXT_MEDIUM),
            allInsertions.get(FullTypeTestUtil.C_TEXT_MEDIUM)),
        C_TEXT_LONG(FullTypeTestUtil.C_TEXT_LONG, allTableDefinitions.get(FullTypeTestUtil.C_TEXT_LONG),
            allInsertions.get(FullTypeTestUtil.C_TEXT_LONG)),
        C_ENUM(FullTypeTestUtil.C_ENUM, allTableDefinitions.get(FullTypeTestUtil.C_ENUM),
            allInsertions.get(FullTypeTestUtil.C_ENUM)),
//        C_SET(FullTypeTestUtil.C_SET, allTableDefinitions.get(FullTypeTestUtil.C_SET),
//            allInsertions.get(FullTypeTestUtil.C_SET)),
        C_JSON(FullTypeTestUtil.C_JSON, allTableDefinitions.get(FullTypeTestUtil.C_JSON),
            allInsertions.get(FullTypeTestUtil.C_JSON)),
        C_GEOMETORY(FullTypeTestUtil.C_GEOMETORY, allTableDefinitions.get(FullTypeTestUtil.C_GEOMETORY),
            allInsertions.get(FullTypeTestUtil.C_GEOMETORY)),
        C_POINT(FullTypeTestUtil.C_POINT, allTableDefinitions.get(FullTypeTestUtil.C_POINT),
            allInsertions.get(FullTypeTestUtil.C_POINT)),
        C_LINESTRING(FullTypeTestUtil.C_LINESTRING, allTableDefinitions.get(FullTypeTestUtil.C_LINESTRING),
            allInsertions.get(FullTypeTestUtil.C_LINESTRING)),
        C_POLYGON(FullTypeTestUtil.C_POLYGON, allTableDefinitions.get(FullTypeTestUtil.C_POLYGON),
            allInsertions.get(FullTypeTestUtil.C_POLYGON)),
        C_MULTIPOINT(FullTypeTestUtil.C_MULTIPOINT, allTableDefinitions.get(FullTypeTestUtil.C_MULTIPOINT),
            allInsertions.get(FullTypeTestUtil.C_MULTIPOINT)),
        C_MULTILINESTRING(FullTypeTestUtil.C_MULTILINESTRING,
            allTableDefinitions.get(FullTypeTestUtil.C_MULTILINESTRING),
            allInsertions.get(FullTypeTestUtil.C_MULTILINESTRING)),
        C_MULTIPOLYGON(FullTypeTestUtil.C_MULTIPOLYGON, allTableDefinitions.get(FullTypeTestUtil.C_MULTIPOLYGON),
            allInsertions.get(FullTypeTestUtil.C_MULTIPOLYGON)),
        C_GEOMETRYCOLLECTION(FullTypeTestUtil.C_GEOMETRYCOLLECTION,
            allTableDefinitions.get(FullTypeTestUtil.C_GEOMETRYCOLLECTION),
            allInsertions.get(FullTypeTestUtil.C_GEOMETRYCOLLECTION));

        private String key;
        private String tableDefinition;
        private List<String> dataInsertion;

        TypeItem(String key, String tableDefinition, List<String> dataInsertion) {
            this.key = key;
            this.tableDefinition = tableDefinition;
            this.dataInsertion = dataInsertion;
        }

        public String getKey() {
            return key;
        }

        public String getTableDefinition() {
            return tableDefinition;
        }

        public List<String> getDataInsertion() {
            return dataInsertion;
        }

        @Override
        public String toString() {
            return "TypeItem{" +
                "key='" + key + '\'' +
                ", tableDefinition='" + tableDefinition + '\'' +
                ", dataInsertion=" + dataInsertion +
                '}';
        }
    }

//    public static void main(String[] args) {
//        for (TypeItem item : TypeItem.values()) {
//            System.out.println(item);
//        }
//    }
}
