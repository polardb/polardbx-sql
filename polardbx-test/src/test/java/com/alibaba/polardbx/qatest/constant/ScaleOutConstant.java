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
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_INT_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_INT_32;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_INT_32_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_JSON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MEDIUMINT_1;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MEDIUMINT_24;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MEDIUMINT_24_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_POINT;
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

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ScaleOutConstant {
    public static Map<String, String> COLUMN_DEF_MAP = GsiConstant.COLUMN_DEF_MAP;
    public static Map<String, String> PK_COLUMN_DEF_MAP = GsiConstant.PK_COLUMN_DEF_MAP;
    public static Map<String, String> PK_DEF_MAP = GsiConstant.PK_DEF_MAP;
    public static String createIndex = "CREATE GLOBAL INDEX gsi_{0} ON {1} ({2},{3}) dbpartition by hash({4});\n";
    static java.util.Random random = new java.util.Random();

    public static ImmutableMap<String, List<String>> buildFullTypeTestInserts(String primaryTableName) {
        return GsiConstant.buildGsiFullTypeTestInserts(primaryTableName);
    }

    public static List<String> getInsertWithShardKey(String PRIMARY_TABLE_NAME, String sk, String pk) {

        List<String> result;
        ImmutableMap<String, List<String>> base = ImmutableMap.<String, List<String>>builder()
            .put(C_ID, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ") values(" + (random.nextInt(20) + 10000)
                    + "               , 0  );\n "
                ,
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ") values(" + (random.nextInt(20) + 10000)
                    + "               , 1  );\n "
                ,
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ") values(" + (random.nextInt(20) + 10000)
                    + "               , 2  );\n "
            ))
            .put(C_BIT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_1) values(" + (random.nextInt(20) + 10000)
                    + "            , 4  , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_1) values(" + (random.nextInt(20) + 10000)
                    + "            , 5  , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_1) values(" + (random.nextInt(20) + 10000)
                    + "            , 6  , 1);\n "))
            .put(C_BIT_8, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_8) values(" + (random.nextInt(20) + 10000)
                    + "            , 8  , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_8) values(" + (random.nextInt(20) + 10000)
                    + "            , 9  , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_8) values(" + (random.nextInt(20) + 10000)
                    + "            , 10 , 2);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_8) values(" + (random.nextInt(20) + 10000)
                    + "            , 11 , 256);\n "))
            .put(C_BIT_16, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_16) values(" + (random.nextInt(20)
                    + 10000) + "           , 13 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_16) values(" + (random.nextInt(20)
                    + 10000) + "           , 14 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_16) values(" + (random.nextInt(20)
                    + 10000) + "           , 15 , 2);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_16) values(" + (random.nextInt(20)
                    + 10000) + "           , 16 , 65535);\n "))
            .put(C_BIT_32, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_32) values(" + (random.nextInt(20)
                    + 10000) + "           , 18 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_32) values(" + (random.nextInt(20)
                    + 10000) + "           , 19 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_32) values(" + (random.nextInt(20)
                    + 10000) + "           , 20 , 2);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_32) values(" + (random.nextInt(20)
                    + 10000) + "           , 21 , 4294967296);\n "))
            .put(C_BIT_64, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_64) values(" + (random.nextInt(20)
                    + 10000) + "           , 23 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_64) values(" + (random.nextInt(20)
                    + 10000) + "           , 24 , 1);\n ",
                // unsupported by drds
                // "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_64) values("+String.valueOf(random.nextInt(20) + 10000)+"           , 26 , 18446744073709551615);\n "                                                       ,
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bit_64) values(" + (random.nextInt(20)
                    + 10000) + "           , 25 , 2);\n "))
            .put(C_TINYINT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1) values(" + (random.nextInt(20)
                    + 10000) + "        , 28 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1) values(" + (random.nextInt(20)
                    + 10000) + "        , 29 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1) values(" + (random.nextInt(20)
                    + 10000) + "        , 30 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1) values(" + (random.nextInt(20)
                    + 10000) + "        , 31 , 127);\n "))
            .put(C_TINYINT_1_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 33 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 34 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 35 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 36 , 127);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_1_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 37 , 255);\n "))
            .put(C_TINYINT_4, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4) values(" + (random.nextInt(20)
                    + 10000) + "        , 39 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4) values(" + (random.nextInt(20)
                    + 10000) + "        , 40 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4) values(" + (random.nextInt(20)
                    + 10000) + "        , 41 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4) values(" + (random.nextInt(20)
                    + 10000) + "        , 42 , 127);\n "))
            .put(C_TINYINT_4_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 44 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 45 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 46 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 47 , 127);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_4_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 48 , 255);\n "))
            .put(C_TINYINT_8, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8) values(" + (random.nextInt(20)
                    + 10000) + "        , 50 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8) values(" + (random.nextInt(20)
                    + 10000) + "        , 51 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8) values(" + (random.nextInt(20)
                    + 10000) + "        , 52 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8) values(" + (random.nextInt(20)
                    + 10000) + "        , 53 , 127);\n "))
            .put(C_TINYINT_8_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 55 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 56 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 57 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 58 , 127);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_tinyint_8_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 59 , 255);\n "))
            .put(C_SMALLINT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_1) values(" + (random.nextInt(20)
                    + 10000) + "       , 61 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_1) values(" + (random.nextInt(20)
                    + 10000) + "       , 62 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_1) values(" + (random.nextInt(20)
                    + 10000) + "       , 63 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_1) values(" + (random.nextInt(20)
                    + 10000) + "       , 64 , 65535);\n "))
            .put(C_SMALLINT_16, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16) values(" + (random.nextInt(20)
                    + 10000) + "      , 66 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16) values(" + (random.nextInt(20)
                    + 10000) + "      , 67 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16) values(" + (random.nextInt(20)
                    + 10000) + "      , 68 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16) values(" + (random.nextInt(20)
                    + 10000) + "      , 69 , 65535);\n "))
            .put(C_SMALLINT_16_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16_un) values(" + (random.nextInt(20)
                    + 10000) + "   , 71 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16_un) values(" + (random.nextInt(20)
                    + 10000) + "   , 72 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16_un) values(" + (random.nextInt(20)
                    + 10000) + "   , 73 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_smallint_16_un) values(" + (random.nextInt(20)
                    + 10000) + "   , 74 , 65535);\n "))
            .put(C_MEDIUMINT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_1) values(" + (random.nextInt(20)
                    + 10000) + "      , 76 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_1) values(" + (random.nextInt(20)
                    + 10000) + "      , 77 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_1) values(" + (random.nextInt(20)
                    + 10000) + "      , 78 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_1) values(" + (random.nextInt(20)
                    + 10000) + "      , 79 , 16777215);\n "))
            .put(C_MEDIUMINT_24, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24) values(" + (random.nextInt(20)
                    + 10000) + "     , 81 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24) values(" + (random.nextInt(20)
                    + 10000) + "     , 82 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24) values(" + (random.nextInt(20)
                    + 10000) + "     , 83 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24) values(" + (random.nextInt(20)
                    + 10000) + "     , 84 , 16777215);\n "))
            .put(C_MEDIUMINT_24_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24_un) values(" + (
                    random.nextInt(20) + 10000) + "  , 86 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24_un) values(" + (
                    random.nextInt(20) + 10000) + "  , 87 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24_un) values(" + (
                    random.nextInt(20) + 10000) + "  , 88 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_mediumint_24_un) values(" + (
                    random.nextInt(20) + 10000) + "  , 89 , 16777215);\n "))
            .put(C_INT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_1) values(" + (random.nextInt(20) + 10000)
                    + "            , 91 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_1) values(" + (random.nextInt(20) + 10000)
                    + "            , 92 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_1) values(" + (random.nextInt(20) + 10000)
                    + "            , 93 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_1) values(" + (random.nextInt(20) + 10000)
                    + "            , 94 , 2147483647);\n "))
            .put(C_INT_32, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32) values(" + (random.nextInt(20)
                    + 10000) + "           , 96 , -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32) values(" + (random.nextInt(20)
                    + 10000) + "           , 97 , 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32) values(" + (random.nextInt(20)
                    + 10000) + "           , 98 , 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32) values(" + (random.nextInt(20)
                    + 10000) + "           , 99 , 2147483647);\n "))
            .put(C_INT_32_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32_un) values(" + (random.nextInt(20)
                    + 10000) + "        , 101, -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32_un) values(" + (random.nextInt(20)
                    + 10000) + "        , 102, 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32_un) values(" + (random.nextInt(20)
                    + 10000) + "        , 103, 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_int_32_un) values(" + (random.nextInt(20)
                    + 10000) + "        , 104, 4294967295);\n "))
            .put(C_BIGINT_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_1) values(" + (random.nextInt(20)
                    + 10000) + "         , 106, -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_1) values(" + (random.nextInt(20)
                    + 10000) + "         , 107, 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_1) values(" + (random.nextInt(20)
                    + 10000) + "         , 108, 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_1) values(" + (random.nextInt(20)
                    + 10000) + "         , 109, 18446744073709551615);\n "))
            .put(C_BIGINT_64, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64) values(" + (random.nextInt(20)
                    + 10000) + "        , 111, -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64) values(" + (random.nextInt(20)
                    + 10000) + "        , 112, 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64) values(" + (random.nextInt(20)
                    + 10000) + "        , 113, 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64) values(" + (random.nextInt(20)
                    + 10000) + "        , 114, 18446744073709551615);\n "))
            .put(C_BIGINT_64_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 116, -1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 117, 0);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 118, 1);\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_bigint_64_un) values(" + (random.nextInt(20)
                    + 10000) + "     , 119, 18446744073709551615);\n "))
            .put(C_DECIMAL, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_decimal) values(" + (random.nextInt(20)
                    + 10000) + "          , 121, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_decimal) values(" + (random.nextInt(20)
                    + 10000) + "          , 122, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_decimal) values(" + (random.nextInt(20)
                    + 10000) + "          , 123, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_decimal) values(" + (random.nextInt(20)
                    + 10000) + "          , 124, '-100.0000001');\n "))
            .put(C_DECIMAL_PR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_decimal_pr) values(" + (random.nextInt(20)
                    + 10000) + "       , 126, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_decimal_pr) values(" + (random.nextInt(20)
                    + 10000) + "       , 127, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_decimal_pr) values(" + (random.nextInt(20)
                    + 10000) + "       , 128, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_decimal_pr) values(" + (random.nextInt(20)
                    + 10000) + "       , 129, '-100.0000001');\n "))
            .put(C_FLOAT, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float) values(" + (random.nextInt(20) + 10000)
                    + "            , 131, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float) values(" + (random.nextInt(20) + 10000)
                    + "            , 132, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float) values(" + (random.nextInt(20) + 10000)
                    + "            , 133, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float) values(" + (random.nextInt(20) + 10000)
                    + "            , 134, '-100.0000001');\n "))
            .put(C_FLOAT_PR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float_pr) values(" + (random.nextInt(20)
                    + 10000) + "         , 136, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float_pr) values(" + (random.nextInt(20)
                    + 10000) + "         , 137, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float_pr) values(" + (random.nextInt(20)
                    + 10000) + "         , 138, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float_pr) values(" + (random.nextInt(20)
                    + 10000) + "         , 139, '-100.0000001');\n "))
            .put(C_FLOAT_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float_un) values(" + (random.nextInt(20)
                    + 10000) + "         , 141, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float_un) values(" + (random.nextInt(20)
                    + 10000) + "         , 142, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float_un) values(" + (random.nextInt(20)
                    + 10000) + "         , 143, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_float_un) values(" + (random.nextInt(20)
                    + 10000) + "         , 144, '-100.0000001');\n "))
            .put(C_DOUBLE, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double) values(" + (random.nextInt(20)
                    + 10000) + "           , 146, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double) values(" + (random.nextInt(20)
                    + 10000) + "           , 147, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double) values(" + (random.nextInt(20)
                    + 10000) + "           , 148, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double) values(" + (random.nextInt(20)
                    + 10000) + "           , 149, '-100.0000001');\n "))
            .put(C_DOUBLE_PR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double_pr) values(" + (random.nextInt(20)
                    + 10000) + "        , 151, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double_pr) values(" + (random.nextInt(20)
                    + 10000) + "        , 152, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double_pr) values(" + (random.nextInt(20)
                    + 10000) + "        , 153, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double_pr) values(" + (random.nextInt(20)
                    + 10000) + "        , 154, '-100.0000001');\n "))
            .put(C_DOUBLE_UN, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double_un) values(" + (random.nextInt(20)
                    + 10000) + "        , 156, '100.000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double_un) values(" + (random.nextInt(20)
                    + 10000) + "        , 157, '100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double_un) values(" + (random.nextInt(20)
                    + 10000) + "        , 158, '-100.003');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_double_un) values(" + (random.nextInt(20)
                    + 10000) + "        , 159, '-100.0000001');\n "))
            .put(C_DATE, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_date) values(" + (random.nextInt(20) + 10000)
                    + "             , 161, '0000-00-00');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_date) values(" + (random.nextInt(20) + 10000)
                    + "             , 162, '9999-12-31');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_date) values(" + (random.nextInt(20) + 10000)
                    + "             , 163, '0000-10-01 01:01:01');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_date) values(" + (random.nextInt(20) + 10000)
                    + "             , 164, '1969-09-01');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_date) values(" + (random.nextInt(20) + 10000)
                    + "             , 165, '2018-00-00');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_date) values(" + (random.nextInt(20) + 10000)
                    + "             , 166, '2017-12-12');\n "))
            .put(C_DATETIME, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime) values(" + (random.nextInt(20)
                    + 10000) + "         , 168, '0000-00-00 00:00:00');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime) values(" + (random.nextInt(20)
                    + 10000) + "         , 169, '9999-12-31 23:59:59');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime) values(" + (random.nextInt(20)
                    + 10000) + "         , 170, '0000-00-00 01:01:01');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime) values(" + (random.nextInt(20)
                    + 10000) + "         , 171, '1969-09-00 23:59:59');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime) values(" + (random.nextInt(20)
                    + 10000) + "         , 172, '2018-00-00 00:00:00');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime) values(" + (random.nextInt(20)
                    + 10000) + "         , 173, '2017-12-12 23:59:59');\n"))
            .put(C_DATETIME_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_1) values(" + (random.nextInt(20)
                    + 10000) + "       , 175, '0000-00-00 00:00:00.0');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_1) values(" + (random.nextInt(20)
                    + 10000) + "       , 176, '9999-12-31 23:59:59.9');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_1) values(" + (random.nextInt(20)
                    + 10000) + "       , 177, '0000-00-00 01:01:01.12');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_1) values(" + (random.nextInt(20)
                    + 10000) + "       , 178, '1969-09-00 23:59:59.06');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_1) values(" + (random.nextInt(20)
                    + 10000) + "       , 179, '2018-00-00 00:00:00.04');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_1) values(" + (random.nextInt(20)
                    + 10000) + "       , 180, '2017-12-12 23:59:59.045');\n"))
            .put(C_DATETIME_3, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_3) values(" + (random.nextInt(20)
                    + 10000) + "       , 182, '0000-00-00 00:00:00.000');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_3) values(" + (random.nextInt(20)
                    + 10000) + "       , 183, '9999-12-31 23:59:59.999');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_3) values(" + (random.nextInt(20)
                    + 10000) + "       , 184, '0000-00-00 01:01:01.121');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_3) values(" + (random.nextInt(20)
                    + 10000) + "       , 185, '1969-09-00 23:59:59.0006');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_3) values(" + (random.nextInt(20)
                    + 10000) + "       , 186, '2018-00-00 00:00:00.0004');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_3) values(" + (random.nextInt(20)
                    + 10000) + "       , 187, '2017-12-12 23:59:59.00045');\n"))
            .put(C_DATETIME_6, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_6) values(" + (random.nextInt(20)
                    + 10000) + "       , 189, '0000-00-00 00:00:00.000000');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_6) values(" + (random.nextInt(20)
                    + 10000) + "       , 190, '9999-12-31 23:59:59.999999');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_6) values(" + (random.nextInt(20)
                    + 10000) + "       , 191, '0000-00-00 01:01:01.121121');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_6) values(" + (random.nextInt(20)
                    + 10000) + "       , 192, '1969-09-00 23:59:59.0000006');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_6) values(" + (random.nextInt(20)
                    + 10000) + "       , 193, '2018-00-00 00:00:00.0000004');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_datetime_6) values(" + (random.nextInt(20)
                    + 10000) + "       , 194, '2017-12-12 23:59:59.00000045');\n"))
            .put(C_TIMESTAMP, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp) values(" + (random.nextInt(20)
                    + 10000) + "        , 196, '0000-00-00 00:00:00');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp) values(" + (random.nextInt(20)
                    + 10000) + "        , 197, '9999-12-31 23:59:59');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp) values(" + (random.nextInt(20)
                    + 10000) + "        , 198, '0000-00-00 01:01:01');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp) values(" + (random.nextInt(20)
                    + 10000) + "        , 199, '1969-09-00 23:59:59');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp) values(" + (random.nextInt(20)
                    + 10000) + "        , 200, '2018-00-00 00:00:00');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp) values(" + (random.nextInt(20)
                    + 10000) + "        , 201, '2017-12-12 23:59:59');\n"))
            .put(C_TIMESTAMP_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_1) values(" + (random.nextInt(20)
                    + 10000) + "      , 203, '0000-00-00 00:00:00.0');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_1) values(" + (random.nextInt(20)
                    + 10000) + "      , 204, '9999-12-31 23:59:59.9');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_1) values(" + (random.nextInt(20)
                    + 10000) + "      , 205, '0000-00-00 01:01:01.12');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_1) values(" + (random.nextInt(20)
                    + 10000) + "      , 206, '1969-09-00 23:59:59.06');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_1) values(" + (random.nextInt(20)
                    + 10000) + "      , 207, '2018-00-00 00:00:00.04');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_1) values(" + (random.nextInt(20)
                    + 10000) + "      , 208, '2017-12-12 23:59:59.045');\n"))
            .put(C_TIMESTAMP_3, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_3) values(" + (random.nextInt(20)
                    + 10000) + "      , 210, '0000-00-00 00:00:00.000');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_3) values(" + (random.nextInt(20)
                    + 10000) + "      , 211, '9999-12-31 23:59:59.999');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_3) values(" + (random.nextInt(20)
                    + 10000) + "      , 212, '0000-00-00 01:01:01.121');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_3) values(" + (random.nextInt(20)
                    + 10000) + "      , 213, '1969-09-00 23:59:59.0006');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_3) values(" + (random.nextInt(20)
                    + 10000) + "      , 214, '2018-00-00 00:00:00.0004');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_3) values(" + (random.nextInt(20)
                    + 10000) + "      , 215, '2017-12-12 23:59:59.00045');\n"))
            .put(C_TIMESTAMP_6, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_6) values(" + (random.nextInt(20)
                    + 10000) + "      , 217, '0000-00-00 00:00:00.000000');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_6) values(" + (random.nextInt(20)
                    + 10000) + "      , 218, '9999-12-31 23:59:59.999999');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_6) values(" + (random.nextInt(20)
                    + 10000) + "      , 219, '0000-00-00 01:01:01.121121');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_6) values(" + (random.nextInt(20)
                    + 10000) + "      , 220, '1969-09-00 23:59:59.0000006');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_6) values(" + (random.nextInt(20)
                    + 10000) + "      , 221, '2018-00-00 00:00:00.0000004');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_timestamp_6) values(" + (random.nextInt(20)
                    + 10000) + "      , 222, '2017-12-12 23:59:59.00000045');\n"))
            .put(C_TIME, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time) values(" + (random.nextInt(20) + 10000)
                    + "             , 224, '-838:59:59');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time) values(" + (random.nextInt(20) + 10000)
                    + "             , 225, '838:59:59');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time) values(" + (random.nextInt(20) + 10000)
                    + "             , 226, '00:00:00');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time) values(" + (random.nextInt(20) + 10000)
                    + "             , 227, '01:01:01');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time) values(" + (random.nextInt(20) + 10000)
                    + "             , 228, '-01:01:01');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time) values(" + (random.nextInt(20) + 10000)
                    + "             , 229, '23:59:59');\n"))
            .put(C_TIME_1, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_1) values(" + (random.nextInt(20)
                    + 10000) + "           , 231, '-828:59:59.9');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_1) values(" + (random.nextInt(20)
                    + 10000) + "           , 232, '838:59:59.9');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_1) values(" + (random.nextInt(20)
                    + 10000) + "           , 233, '00:00:00.1');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_1) values(" + (random.nextInt(20)
                    + 10000) + "           , 234, '01:01:01.6');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_1) values(" + (random.nextInt(20)
                    + 10000) + "           , 235, '-01:01:01.4');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_1) values(" + (random.nextInt(20)
                    + 10000) + "           , 236, '23:59:59.45');\n"))
            .put(C_TIME_3, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_3) values(" + (random.nextInt(20)
                    + 10000) + "           , 238, '-838:59:59.999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_3) values(" + (random.nextInt(20)
                    + 10000) + "           , 239, '838:59:59.999');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_3) values(" + (random.nextInt(20)
                    + 10000) + "           , 240, '00:00:00.111');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_3) values(" + (random.nextInt(20)
                    + 10000) + "           , 241, '01:01:01.106');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_3) values(" + (random.nextInt(20)
                    + 10000) + "           , 242, '-01:01:01.0004');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_3) values(" + (random.nextInt(20)
                    + 10000) + "           , 243, '23:59:59.00045');\n"))
            .put(C_TIME_6, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_6) values(" + (random.nextInt(20)
                    + 10000) + "           , 245, '-838:59:59.999999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_6) values(" + (random.nextInt(20)
                    + 10000) + "           , 246, '838:59:59.999999');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_6) values(" + (random.nextInt(20)
                    + 10000) + "           , 247, '00:00:00.111111');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_6) values(" + (random.nextInt(20)
                    + 10000) + "           , 248, '01:01:01.106106');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_6) values(" + (random.nextInt(20)
                    + 10000) + "           , 249, '-01:01:01.0000004');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_time_6) values(" + (random.nextInt(20)
                    + 10000) + "           , 250, '23:59:59.00000045');\n"))
            .put(C_YEAR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_year) values(" + (random.nextInt(20) + 10000)
                    + "             , 252, '0000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_year) values(" + (random.nextInt(20) + 10000)
                    + "             , 253, '9999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_year) values(" + (random.nextInt(20) + 10000)
                    + "             , 254, '1970');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_year) values(" + (random.nextInt(20) + 10000)
                    + "             , 255, '2000');\n "))
            .put(C_YEAR_4, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_year_4) values(" + (random.nextInt(20)
                    + 10000) + "           , 257, '0000');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_year_4) values(" + (random.nextInt(20)
                    + 10000) + "           , 258, '9999');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_year_4) values(" + (random.nextInt(20)
                    + 10000) + "           , 259, '1970');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_year_4) values(" + (random.nextInt(20)
                    + 10000) + "           , 260, '2000');\n "))
            .put(C_CHAR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_char) values(" + (random.nextInt(20) + 10000)
                    + "             , 262, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_char) values(" + (random.nextInt(20) + 10000)
                    + "             , 263, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_char) values(" + (random.nextInt(20) + 10000)
                    + "             , 264, 'a中国a');\n "))
            .put(C_VARCHAR, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_varchar) values(" + (random.nextInt(20)
                    + 10000) + "          , 266, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_varchar) values(" + (random.nextInt(20)
                    + 10000) + "          , 267, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_varchar) values(" + (random.nextInt(20)
                    + 10000) + "          , 268, 'a中国a');\n "))
            .put(C_BINARY, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_binary) values(" + (random.nextInt(20)
                    + 10000) + "           , 270, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_binary) values(" + (random.nextInt(20)
                    + 10000) + "           , 271, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_binary) values(" + (random.nextInt(20)
                    + 10000) + "           , 272, 'a中国a');\n "))
            .put(C_VARBINARY, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_varbinary) values(" + (random.nextInt(20)
                    + 10000) + "        , 274, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_varbinary) values(" + (random.nextInt(20)
                    + 10000) + "        , 275, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_varbinary) values(" + (random.nextInt(20)
                    + 10000) + "        , 276, 'a中国a');\n "))
            .put(C_BLOB_TINY, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob_tiny) values(" + (random.nextInt(20)
                    + 10000) + "        , 278, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob_tiny) values(" + (random.nextInt(20)
                    + 10000) + "        , 279, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob_tiny) values(" + (random.nextInt(20)
                    + 10000) + "        , 280, 'a中国a');\n "))
            .put(C_BLOB, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob) values(" + (random.nextInt(20) + 10000)
                    + "             , 282, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob) values(" + (random.nextInt(20) + 10000)
                    + "             , 283, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob) values(" + (random.nextInt(20) + 10000)
                    + "             , 284, 'a中国a');\n "))
            .put(C_BLOB_MEDIUM, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob_medium) values(" + (random.nextInt(20)
                    + 10000) + "      , 286, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob_medium) values(" + (random.nextInt(20)
                    + 10000) + "      , 287, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob_medium) values(" + (random.nextInt(20)
                    + 10000) + "      , 288, 'a中国a');\n "))
            .put(C_BLOB_LONG, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob_long) values(" + (random.nextInt(20)
                    + 10000) + "        , 290, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob_long) values(" + (random.nextInt(20)
                    + 10000) + "        , 291, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_blob_long) values(" + (random.nextInt(20)
                    + 10000) + "        , 292, 'a中国a');\n "))
            .put(C_TEXT_TINY, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text_tiny) values(" + (random.nextInt(20)
                    + 10000) + "        , 294, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text_tiny) values(" + (random.nextInt(20)
                    + 10000) + "        , 295, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text_tiny) values(" + (random.nextInt(20)
                    + 10000) + "        , 296, 'a中国a');\n "))
            .put(C_TEXT, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text) values(" + (random.nextInt(20) + 10000)
                    + "             , 298, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text) values(" + (random.nextInt(20) + 10000)
                    + "             , 299, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text) values(" + (random.nextInt(20) + 10000)
                    + "             , 300, 'a中国a');\n "))
            .put(C_TEXT_MEDIUM, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text_medium) values(" + (random.nextInt(20)
                    + 10000) + "      , 302, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text_medium) values(" + (random.nextInt(20)
                    + 10000) + "      , 303, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text_medium) values(" + (random.nextInt(20)
                    + 10000) + "      , 304, 'a中国a');\n "))
            .put(C_TEXT_LONG, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text_long) values(" + (random.nextInt(20)
                    + 10000) + "        , 306, '11');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text_long) values(" + (random.nextInt(20)
                    + 10000) + "        , 307, '99');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_text_long) values(" + (random.nextInt(20)
                    + 10000) + "        , 308, 'a中国a');\n "))
            .put(C_ENUM, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_enum) values(" + (random.nextInt(20) + 10000)
                    + "             , 310, 'a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_enum) values(" + (random.nextInt(20) + 10000)
                    + "             , 311, 'b');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_enum) values(" + (random.nextInt(20) + 10000)
                    + "             , 312, NULL);\n "))
            .put(C_SET, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_set) values(" + (random.nextInt(20) + 10000)
                    + "              , 314, 'a');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_set) values(" + (random.nextInt(20) + 10000)
                    + "              , 315, 'b,a');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_set) values(" + (random.nextInt(20) + 10000)
                    + "              , 316, 'b,c,a');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_set) values(" + (random.nextInt(20) + 10000)
                    + "              , 317, 'd');\n",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_set) values(" + (random.nextInt(20) + 10000)
                    + "              , 318, NULL);\n "))
            .put(C_JSON, ImmutableList.of(
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_json) values(" + (random.nextInt(20) + 10000)
                    + "             , 320, '{\"k1\": \"v1\", \"k2\": 10}');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_json) values(" + (random.nextInt(20) + 10000)
                    + "             , 321, '{\"k1\": \"v1\", \"k2\": [10, 20]}');\n ",
                "insert into " + PRIMARY_TABLE_NAME + "(id, " + sk + ", c_json) values(" + (random.nextInt(20) + 10000)
                    + "             , 322, NULL);\n "))
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

        if (sk.toLowerCase().indexOf("tiny") != -1 || sk.endsWith("_1")) {
            result = base.entrySet()
                .stream()
                .filter(e -> e.getKey().toLowerCase().indexOf("int") != -1
                    && e.getKey().toLowerCase().indexOf("point") == -1
                    && !e.getKey().equalsIgnoreCase(sk)
                    && !e.getKey().equalsIgnoreCase(pk)
                )
                .flatMap(e -> e.getValue().stream())
                .collect(Collectors.toList());
        } else {
            result = base.entrySet()
                .stream()
                .filter(e -> !e.getKey().equalsIgnoreCase(sk) && !e.getKey().equalsIgnoreCase(pk))
                .flatMap(e -> e.getValue().stream())
                .collect(Collectors.toList());
        }
        List<String> res = new ArrayList<>();
        res.addAll(result);
        result.stream().forEach(c -> {
            c = c.replace("insert into", "insert ignore into")
                .replace("(id", "(" + pk);
            res.add(c);
        });
        return res;
    }

    public static List<String> getDeleteWithShardKey(String PRIMARY_TABLE_NAME, String sk) {
        ImmutableMap<String, List<String>> base = ImmutableMap.<String, List<String>>builder()
            .put(C_ID, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 0;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 1;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 2;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=0;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=2;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >0;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <2;\n "))
            .put(C_TINYINT_1, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 28;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 29;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 30;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 31;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=28;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=29;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >30;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <38;\n "))
            .put(C_TINYINT_1_UN, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 33;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 34;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 35;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 36;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 37;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=35;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=34;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >34;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <37;\n "))
            .put(C_TINYINT_4, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 39;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 40;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 41;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 42;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=39;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=40;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >42;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <40;\n "))
            .put(C_TINYINT_4_UN, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 44;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 45;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 46;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 47;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 48;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=45;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=47;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >49;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <45;\n "))
            .put(C_TINYINT_8, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 50;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 51;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 52;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 53;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 54;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=55;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=53;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >52;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <52;\n "))
            .put(C_TINYINT_8_UN, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 56;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 57;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 58;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 59;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 60;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=60;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=52;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >57;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <90;\n "))
            .put(C_SMALLINT_1, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 161;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 162;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 63;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 164;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=65;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=70;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >82;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <20;\n "))
            .put(C_SMALLINT_16, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 66;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 67;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 68;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 69;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=68;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=69;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >42;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <40;\n "))
            .put(C_SMALLINT_16_UN, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 71;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 72;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 74;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 79;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=73;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=78;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >72;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <72;\n "))
            .put(C_MEDIUMINT_1, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 76;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 77;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 78;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 79;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=78;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=79;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >82;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <79;\n "))
            .put(C_MEDIUMINT_24, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 66;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 67;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 68;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 69;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=68;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=69;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >42;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <40;\n "))
            .put(C_MEDIUMINT_24_UN, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 86;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 87;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 88;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 89;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=88;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=89;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >82;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <80;\n "))
            .put(C_INT_1, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 91;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 92;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 93;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 94;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=92;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=90;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >92;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <94;\n "))
            .put(C_INT_32, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 96;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 97;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 98;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 99;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=98;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=99;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >92;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <100;\n "))
            .put(C_INT_32_UN, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 101;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 102;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 103;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 104;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=102;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=103;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >100;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <100;\n "))
            .put(C_BIGINT_1, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 106;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 107;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 108;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 109;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=105;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=107;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >105;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <109;\n "))
            .put(C_BIGINT_64, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 111;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 112;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 113;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 114;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=112;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=113;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >110;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <110;\n "))
            .put(C_BIGINT_64_UN, ImmutableList.of(
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 117;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 118;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 119;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " = 116;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=117;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >=119;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id > " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id = " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where id < " + (random.nextInt(20) + 10000)
                    + ";\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " >116;\n ",
                "delete from " + PRIMARY_TABLE_NAME + " where " + sk + " <118;\n "))
            .build();
        return base.entrySet()
            .stream()
            .filter(e -> !e.getKey().equalsIgnoreCase(sk))
            .flatMap(e -> e.getValue().stream())
            .collect(Collectors.toList());

    }

    public static List<String> getUpdateWithShardKey(String PRIMARY_TABLE_NAME, String sk) {
        ImmutableMap<String, List<String>> base = ImmutableMap.<String, List<String>>builder()
            .put(C_ID, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 0;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 1;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 1;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 2;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=0;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=0;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=2;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >0;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <2;\n "))
            .put(C_TINYINT_1, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 28;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 29;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 30;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 31;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=28;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=29;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >30;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <38;\n "))
            .put(C_TINYINT_1_UN, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 33;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 34;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 35;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 36;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 37;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=35;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=34;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >34;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <37;\n "))
            .put(C_TINYINT_4, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 39;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 40;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 41;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 42;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=39;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=40;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >42;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <40;\n "))
            .put(C_TINYINT_4_UN, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 44;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 45;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 46;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 47;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 48;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=45;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=47;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >49;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <45;\n "))
            .put(C_TINYINT_8, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 50;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 51;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 52;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 53;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 54;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=55;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=53;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >52;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <52;\n "))
            .put(C_TINYINT_8_UN, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 56;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 57;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 58;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 59;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 60;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=60;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=52;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >57;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <90;\n "))
            .put(C_SMALLINT_1, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 161;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 162;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 63;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 164;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=65;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=70;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >82;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <20;\n "))
            .put(C_SMALLINT_16, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 66;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 67;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 68;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 69;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=68;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=69;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >42;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <40;\n "))
            .put(C_SMALLINT_16_UN, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 71;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 72;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 74;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 79;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=73;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=78;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >72;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <72;\n "))
            .put(C_MEDIUMINT_1, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 76;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 77;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 78;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 79;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=78;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=79;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >82;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <79;\n "))
            .put(C_MEDIUMINT_24, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 66;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 67;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 68;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 69;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=68;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=69;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >42;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <40;\n "))
            .put(C_MEDIUMINT_24_UN, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 86;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 87;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 88;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 89;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=88;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=89;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >82;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <80;\n "))
            .put(C_INT_1, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 91;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 92;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 93;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 94;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=92;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=90;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >92;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <94;\n "))
            .put(C_INT_32, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 96;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 97;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 98;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 99;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=98;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=99;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >92;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <100;\n "))
            .put(C_INT_32_UN, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 101;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 102;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 103;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 104;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=102;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=103;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >100;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <100;\n "))
            .put(C_BIGINT_1, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 106;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 107;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 108;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 109;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=105;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=107;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >105;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <109;\n "))
            .put(C_BIGINT_64, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 111;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 112;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 113;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 114;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=112;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=113;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >110;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <110;\n "))
            .put(C_BIGINT_64_UN, ImmutableList.of(
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 117;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 118;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 119;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " = 116;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=117;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >=119;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " >116;\n ",
                "update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + " <118;\n "))
            .build();
        String updateIds =
            "update " + PRIMARY_TABLE_NAME + " set id=" + (random.nextInt(20) + 10000) + ";\n";
        if (sk.toLowerCase().indexOf("tiny") != -1 || sk.endsWith("_1")) {
            List<String> updates = new ArrayList<>();
            updates.add("update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "+1 where " + sk + "<127");
            updates.add("update " + PRIMARY_TABLE_NAME + " set " + sk + "=" + sk + "-1 where " + sk + ">0");
            updates.add(updateIds);
            return updates;
        } else {
            List<String> result = base.entrySet()
                .stream()
                .filter(e -> !e.getKey().equalsIgnoreCase(sk))
                .flatMap(e -> e.getValue().stream())
                .collect(Collectors.toList());
            result.add(updateIds);
            result.add(updateIds);
            result.add(updateIds);
            result.add(updateIds);
            return result;
        }
    }

    public static List<String> getUpdateForTwoTable(String tableName1, String tableName2, String sk) {
        String sql;
        List<String> result = new ArrayList<>();
        int count = 20;
        for (int i = 0; i < count; i++) {
            sql = String.format("update %s t1,%s t2 set t2.id=t2.id+1 where t1.%s > t2.%s;",
                tableName1, tableName2, sk, sk);
            result.add(sql);
            sql = String.format("update %s t1,%s t2 set t1.%s=t2.%s+20,t2.id=t1.id where t1.id > t2.id;",
                tableName1, tableName2, sk, sk);
            sql = String.format("update %s t1,%s t2 set t2.id=t1.id-10 where t1.%s > t2.%s + 1;",
                tableName1, tableName2, sk, sk);
            result.add(sql);
            sql = String.format("update %s t1,%s t2 set t1.id=t1.%s+20,t2.id=t2.%s+20 where t1.%s = t2.%s;",
                tableName1, tableName2, sk, sk, sk, sk);
            result.add(sql);
            sql = String
                .format("update %s t1,%s t2 set t2.id=t1.%s,t1.id=t2.%s,t2.c_timestamp=now() where t1.id = t2.id;",
                    tableName1, tableName2, sk, sk);
            result.add(sql);
            sql =
                String.format("update %s t1,%s t2 set t1.c_datetime_6=now(),t2.c_timestamp=now() where t1.%s <> t2.%s;",
                    tableName1, tableName2, sk, sk);
            result.add(sql);
        }
        return result;
    }

    public static String partitioning(String sk) {
        return GsiConstant.partitioning(sk);
    }

    public static String hashPartitioning(String sk) {
        return GsiConstant.hashPartitioning(sk);
    }

    public static String getIndexDef(String tableName, String indexCol1, String indexCol2) {
        return MessageFormat.format(createIndex, tableName, tableName, indexCol1, indexCol2, indexCol1);
    }
}

