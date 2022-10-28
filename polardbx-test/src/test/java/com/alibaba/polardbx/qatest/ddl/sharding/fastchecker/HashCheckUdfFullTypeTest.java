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

package com.alibaba.polardbx.qatest.ddl.sharding.fastchecker;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.text.MessageFormat;
/**
 * Created by zhuqiwei.
 *
 * @author: zhuqiwei
 */

@Ignore
public class HashCheckUdfFullTypeTest extends AsyncDDLBaseNewDBTestCase {
    private static final String TABLE_NAME = "hashcheck_full_type_table";
    private static final String FULL_TYPE_TABLE_MYSQL = "CREATE TABLE `{0}` (\n"
            + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c_bit_1` bit(1) DEFAULT NULL,\n"
            + "  `c_bit_8` bit(8) DEFAULT NULL,\n"
            + "  `c_bit_16` bit(16) DEFAULT NULL,\n"
            + "  `c_bit_32` bit(32) DEFAULT NULL,\n"
            + "  `c_bit_64` bit(64) DEFAULT NULL,\n"
            + "  `c_tinyint_1` tinyint(1) DEFAULT NULL,\n"
            + "  `c_tinyint_1_un` tinyint(1) unsigned DEFAULT NULL,\n"
            + "  `c_tinyint_4` tinyint(4) DEFAULT NULL,\n"
            + "  `c_tinyint_4_un` tinyint(4) unsigned DEFAULT NULL,\n"
            + "  `c_tinyint_8` tinyint(8) DEFAULT NULL,\n"
            + "  `c_tinyint_8_un` tinyint(8) unsigned DEFAULT NULL,\n"
            + "  `c_smallint_1` smallint(1) DEFAULT NULL,\n"
            + "  `c_smallint_16` smallint(16) DEFAULT NULL,\n"
            + "  `c_smallint_16_un` smallint(16) unsigned DEFAULT NULL,\n"
            + "  `c_mediumint_1` mediumint(1) DEFAULT NULL,\n"
            + "  `c_mediumint_24` mediumint(24) DEFAULT NULL,\n"
            + "  `c_mediumint_24_un` mediumint(24) unsigned DEFAULT NULL,\n"
            + "  `c_int_1` int(1) DEFAULT NULL,\n"
            + "  `c_int_32` int(32) NOT NULL DEFAULT 0 COMMENT \"For multi pk.\",\n"
            + "  `c_int_32_un` int(32) unsigned DEFAULT NULL,\n"
            + "  `c_bigint_1` bigint(1) DEFAULT NULL,\n"
            + "  `c_bigint_64` bigint(64) DEFAULT NULL,\n"
            + "  `c_bigint_64_un` bigint(64) unsigned DEFAULT NULL,\n"
            + "  `c_decimal` decimal DEFAULT NULL,\n"
            + "  `c_decimal_pr` decimal(10,3) DEFAULT NULL,\n"
            + "  `c_float` float DEFAULT NULL,\n"
            + "  `c_float_pr` float(10,3) DEFAULT NULL,\n"
            + "  `c_float_un` float(10,3) unsigned DEFAULT NULL,\n"
            + "  `c_double` double DEFAULT NULL,\n"
            + "  `c_double_pr` double(10,3) DEFAULT NULL,\n"
            + "  `c_double_un` double(10,3) unsigned DEFAULT NULL,\n"
            + "  `c_date` date DEFAULT NULL COMMENT \"date\",\n"
            + "  `c_datetime` datetime DEFAULT NULL,\n"
            + "  `c_datetime_1` datetime(1) DEFAULT NULL,\n"
            + "  `c_datetime_3` datetime(3) DEFAULT NULL,\n"
            + "  `c_datetime_6` datetime(6) DEFAULT NULL,\n"
            + "  `c_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `c_timestamp_1` timestamp(1) DEFAULT \"2000-01-01 00:00:00\",\n"
            + "  `c_timestamp_3` timestamp(3) DEFAULT \"2000-01-01 00:00:00\",\n"
            + "  `c_timestamp_6` timestamp(6) DEFAULT \"2000-01-01 00:00:00\",\n"
            + "  `c_time` time DEFAULT NULL,\n"
            + "  `c_time_1` time(1) DEFAULT NULL,\n"
            + "  `c_time_3` time(3) DEFAULT NULL,\n"
            + "  `c_time_6` time(6) DEFAULT NULL,\n"
            + "  `c_year` year DEFAULT NULL,\n"
            + "  `c_year_4` year(4) DEFAULT NULL,\n"
            + "  `c_char` char(10) DEFAULT NULL,\n"
            + "  `c_varchar` varchar(10) DEFAULT NULL,\n"
            + "  `c_binary` binary(10) DEFAULT NULL,\n"
            + "  `c_varbinary` varbinary(10) DEFAULT NULL,\n"
            + "  `c_blob_tiny` tinyblob DEFAULT NULL,\n"
            + "  `c_blob` blob DEFAULT NULL,\n"
            + "  `c_blob_medium` mediumblob DEFAULT NULL,\n"
            + "  `c_blob_long` longblob DEFAULT NULL,\n"
            + "  `c_text_tiny` tinytext DEFAULT NULL,\n"
            + "  `c_text` text DEFAULT NULL,\n"
            + "  `c_text_medium` mediumtext DEFAULT NULL,\n"
            + "  `c_text_long` longtext DEFAULT NULL,\n"
            + "  `c_enum` enum(\"a\",\"b\",\"c\") DEFAULT NULL,\n"
            + "  `c_set` set(\"a\",\"b\",\"c\") DEFAULT NULL,\n"
            + "  `c_json` json DEFAULT NULL,\n"
            + "  `c_geometory` geometry DEFAULT NULL,\n"
            + "  `c_point` point DEFAULT NULL,\n"
            + "  `c_linestring` linestring DEFAULT NULL,\n"
            + "  `c_polygon` polygon DEFAULT NULL,\n"
            + "  `c_multipoint` multipoint DEFAULT NULL,\n"
            + "  `c_multilinestring` multilinestring DEFAULT NULL,\n"
            + "  `c_multipolygon` multipolygon DEFAULT NULL,\n"
            + "  `c_geometrycollection` geometrycollection DEFAULT NULL,\n"
            + "  PRIMARY KEY ({1})\n"
            + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT=\"10000000\"\n";

    private static final String FULL_INSERT = "insert into `" + TABLE_NAME + "` values (\n" +
            "10240,\n" +
            "1,2,2,2,2,\n" +
            "-1,1,-1,1,-1,1,\n" +
            "-1,-1,1,\n" +
            "-1,-1,1,\n" +
            "-1,-1,1,\n" +
            "-1,-1,1,\n" +
            "-100.003, -100.000,\n" +
            "100.000,100.003,100.003,100.000,100.003,100.003,\n" +
            "'2017-12-12',\n" +
            "'2017-12-12 23:59:59','2017-12-12 23:59:59.1','2017-12-12 23:59:59.001','2017-12-12 23:59:59.000001',\n" +
            "'2017-12-12 23:59:59','2017-12-12 23:59:59.1','2017-12-12 23:59:59.001','2017-12-12 23:59:59.000001',\n" +
            "'01:01:01','01:01:01.1','01:01:01.001','01:01:01.000001',\n" +
            "'2000','2000',\n" +
            "'11','11','11','11',\n" +
            "'11','11','11','11',\n" +
            "'11','11','11','11',\n" +
            "'a','b,a',\n" +
            "'{\"k1\": \"v1\", \"k2\": 10}',\n" +
            "ST_GEOMFROMTEXT('LINESTRING(0 0, 10 10, 20 25, 50 60)'),\n" +
            "ST_POINTFROMTEXT('POINT(15 20)'),\n" +
            "ST_GEOMFROMTEXT('LINESTRING(0 0, 10 10, 20 25, 50 60)'),\n" +
            "ST_GEOMFROMTEXT('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))'),\n" +
            "ST_GEOMFROMTEXT('MULTIPOINT(0 0, 15 25, 45 65)'),\n" +
            "ST_GEOMFROMTEXT('MULTILINESTRING((10 10, 20 20), (15 15, 30 15))'),\n" +
            "ST_GEOMFROMTEXT('MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))'),\n" +
            "ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))'));";

    private static final String FULL_SELECT = "select hashcheck( "
            + "  `id`,"
            + "  `c_bit_1`,"
            + "  `c_bit_8` ,"
            + "  `c_bit_16`,"
            + "  `c_bit_32`,"
            + "  `c_bit_64` ,"
            + "  `c_tinyint_1`,"
            + "  `c_tinyint_1_un` ,"
            + "  `c_tinyint_4` ,"
            + "  `c_tinyint_4_un` ,"
            + "  `c_tinyint_8` ,"
            + "  `c_tinyint_8_un` ,"
            + "  `c_smallint_1`,"
            + "  `c_smallint_16`,"
            + "  `c_smallint_16_un` ,"
            + "  `c_mediumint_1` ,"
            + "  `c_mediumint_24`,"
            + "  `c_mediumint_24_un` ,"
            + "  `c_int_1`,"
            + "  `c_int_32` ,"
            + "  `c_int_32_un` ,"
            + "  `c_bigint_1`,"
            + "  `c_bigint_64` ,"
            + "  `c_bigint_64_un` ,"
            + "  `c_decimal` ,"
            + "  `c_decimal_pr`,"
            + "  `c_float`,"
            + "  `c_float_pr`,"
            + "  `c_float_un` ,"
            + "  `c_double` ,"
            + "  `c_double_pr`,"
            + "  `c_double_un` ,"
            + "  `c_date` ,"
            + "  `c_datetime` ,"
            + "  `c_datetime_1`,"
            + "  `c_datetime_3` ,"
            + "  `c_datetime_6`,"
            + "  `c_timestamp` ,"
            + "  `c_timestamp_1` ,"
            + "  `c_timestamp_3` ,"
            + "  `c_timestamp_6` ,"
            + "  `c_time`,"
            + "  `c_time_1`,"
            + "  `c_time_3` ,"
            + "  `c_time_6` ,"
            + "  `c_year` ,"
            + "  `c_year_4` ,"
            + "  `c_char` ,"
            + "  `c_varchar`,"
            + "  `c_binary` ,"
            + "  `c_varbinary` ,"
            + "  `c_blob_tiny` ,"
            + "  `c_blob`,"
            + "  `c_blob_medium`,"
            + "  `c_blob_long` ,"
            + "  `c_text_tiny`,"
            + "  `c_text`,"
            + "  `c_text_medium`,"
            + "  `c_text_long` ,"
            + "  `c_enum` ,"
            + "  `c_set` ,"
            + "  `c_json` ,"
            + "  `c_geometory` ,"
            + "  `c_point`,"
            + "  `c_linestring` ,"
            + "  `c_polygon`,"
            + "  `c_multipoint` ,"
            + "  `c_multilinestring` ,"
            + "  `c_multipolygon` ,"
            + "  `c_geometrycollection`"
            + ") from {0}";

    private static final Long hashcheckAnswer = -156254481117072327L;

    @Before
    public void initData() throws Exception {
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS `" + TABLE_NAME + "`");
        JdbcUtil.executeUpdateSuccess(mysqlConnection, MessageFormat.format(FULL_TYPE_TABLE_MYSQL, TABLE_NAME, "id"));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, FULL_INSERT);
    }

    @After
    public void cleanup() throws Exception {
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS `" + TABLE_NAME + "`");
    }

    @Test
    public void test() throws Exception {
        ResultSet rs = JdbcUtil.executeQuerySuccess(mysqlConnection,
                MessageFormat.format(FULL_SELECT, TABLE_NAME));
        Long ans = JdbcUtil.resultLong(rs);
        Assert.assertTrue(hashcheckAnswer.equals(ans));
        JdbcUtil.close(rs);
    }
}
