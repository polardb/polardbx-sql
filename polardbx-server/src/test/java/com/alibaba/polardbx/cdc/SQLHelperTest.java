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

package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.alibaba.polardbx.cdc.SQLHelper.filterColumns;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;

/**
 * created by ziyang.lb
 **/
public class SQLHelperTest {

    @Test
    public void testCheckToString() {
        String sql = "/*!50003 create function bug14723()\n"
            + "returns bigint(20)\n"
            + "main_loop: begin\n"
            + "return 42;\n"
            + "end */";
        SQLHelper.checkToString(sql);
    }

    @Test
    public void testFilterColumns() {
        String sqlInput = "CREATE TABLE `omc_index_col_unique_test_gsi1` \n"
            + "( `a` int(11) NOT NULL,\n"
            + " `B` bigint(20) DEFAULT NULL, \n"
            + " `c` bigint(20) DEFAULT NULL, \n"
            + " PRIMARY KEY (`a`), \n"
            + " UNIQUE KEY `b` (`b`), \n"
            + " UNIQUE KEY `c` USING BTREE (`c`), \n"
            + " INDEX `idx_i` using btree(`B`),"
            + " UNIQUE KEY `b_zplr` \n"
            + " USING BTREE (`c`) ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
        String sqlOutput = "CREATE TABLE `omc_index_col_unique_test_gsi1` (\n"
            + "\t`a` int(11) NOT NULL,\n"
            + "\t`c` bigint(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`a`),\n"
            + "\tUNIQUE KEY `c` USING BTREE (`c`),\n"
            + "\tUNIQUE KEY `b_zplr` USING BTREE (`c`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";

        List<SQLStatement> statementList =
            SQLParserUtils.createSQLStatementParser(sqlInput, DbType.mysql, SQL_PARSE_FEATURES).parseStatementList();
        MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement) statementList.get(0);
        filterColumns(createStmt, Lists.newArrayList("b"));
        String result = createStmt.toString();
        Assert.assertEquals(sqlOutput, result);
    }

    @Test
    public void testAlterIndexToString() {
        String sql = "ALTER INDEX idx_name ON TABLE tb3\n"
            + "  SPLIT PARTITION p3";
        List<SQLStatement> statementList =
            SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES).parseStatementList();
        SQLAlterTableStatement statement = (SQLAlterTableStatement) statementList.get(0);

        String expectSql = "ALTER INDEX idx_name ON TABLE tb3\n"
            + "\tSPLIT PARTITION p3 ";
        String toStrSql = SQLUtils.toSQLString(statement, DbType.mysql);
        Assert.assertEquals(expectSql, toStrSql);
    }

    @Test
    public void testAlterSequence() {
        String sql = "CONVERT ALL SEQUENCES FROM NEW TO GROUP FOR cdc_sequence_test";
        SQLStatementParser parser =
            SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statement = statementList.get(0);
        String parserResult = statement.toString();
        // 逆向parse一遍
        parser = SQLParserUtils.createSQLStatementParser(parserResult, DbType.mysql, SQL_PARSE_FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        parserResult = statement.toString();
    }

    @Test
    public void testHintsInCreateIndex() {
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_CREATE_EXPRESSION_INDEX=TRUE,ENABLE_UNIQUE_KEY_ON_GEN_COL=TRUE)*/"
            + "create local unique index expr_multi_column_tbl_idx on expr_multi_column_tbl((a+1) desc,b,c-1,substr(d,-2) asc,a+b+c*2)";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement sqlStatement = statementList.get(0);
        String outputSql = sqlStatement.toString();
        Assert.assertTrue(StringUtils.contains(outputSql,
            "/*+TDDL:CMD_EXTRA(ENABLE_CREATE_EXPRESSION_INDEX=TRUE,ENABLE_UNIQUE_KEY_ON_GEN_COL=TRUE)*/"));

        sql = "/*+TDDL:CMD_EXTRA(ENABLE_CREATE_EXPRESSION_INDEX=TRUE,ENABLE_UNIQUE_KEY_ON_GEN_COL=TRUE)*/"
            + "alter table expr_multi_column_tbl add local unique index expr_multi_column_tbl_idx((a+1) desc,b,c-1,substr(d,-2) asc,a+b+c*2)";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        statementList = parser.parseStatementList();
        sqlStatement = statementList.get(0);
        outputSql = sqlStatement.toString();
        Assert.assertTrue(StringUtils.contains(outputSql,
            "/*+TDDL:CMD_EXTRA(ENABLE_CREATE_EXPRESSION_INDEX=TRUE,ENABLE_UNIQUE_KEY_ON_GEN_COL=TRUE)*/"));
    }

    @Test
    public void testParseWithPolarxOriginalSql() {
        String sql = "# POLARX_ORIGIN_SQL_ENCODE=BASE64\n"
            + "# POLARX_ORIGIN_SQL=LypERExfSUQ9NzEzNjE4MTY0OTkxMzQxMzY5NiovCkNSRUFURSBUQUJMRSBgYmFzZVRhYmxlYCAoCglgaWRgIGJpZ2ludCgyMCkgTk9UIE5VTEwgQVVUT19JTkNSRU1FTlQsCglgY19iaXRfMWAgYml0KDEpIERFRkFVTFQgTlVMTCwKCWBjX2JpdF84YCBiaXQoOCkgREVGQVVMVCBOVUxMLAoJYGNfYml0XzE2YCBiaXQoMTYpIERFRkFVTFQgTlVMTCwKCWBjX2JpdF8zMmAgYml0KDMyKSBERUZBVUxUIE5VTEwsCglgY19iaXRfNjRgIGJpdCg2NCkgREVGQVVMVCBOVUxMLAoJYGNfdGlueWludF8xYCB0aW55aW50KDEpIERFRkFVTFQgTlVMTCwKCWBjX3RpbnlpbnRfMV91bmAgdGlueWludCgxKSBVTlNJR05FRCBERUZBVUxUIE5VTEwsCglgY190aW55aW50XzRgIHRpbnlpbnQoNCkgREVGQVVMVCBOVUxMLAoJYGNfdGlueWludF80X3VuYCB0aW55aW50KDQpIFVOU0lHTkVEIERFRkFVTFQgTlVMTCwKCWBjX3RpbnlpbnRfOGAgdGlueWludCg4KSBERUZBVUxUIE5VTEwsCglgY190aW55aW50XzhfdW5gIHRpbnlpbnQoOCkgVU5TSUdORUQgREVGQVVMVCBOVUxMLAoJYGNfc21hbGxpbnRfMWAgc21hbGxpbnQoMSkgREVGQVVMVCBOVUxMLAoJYGNfc21hbGxpbnRfMTZgIHNtYWxsaW50KDE2KSBERUZBVUxUIE5VTEwsCglgY19zbWFsbGludF8xNl91bmAgc21hbGxpbnQoMTYpIFVOU0lHTkVEIERFRkFVTFQgTlVMTCwKCWBjX21lZGl1bWludF8xYCBtZWRpdW1pbnQoMSkgREVGQVVMVCBOVUxMLAoJYGNfbWVkaXVtaW50XzI0YCBtZWRpdW1pbnQoMjQpIERFRkFVTFQgTlVMTCwKCWBjX21lZGl1bWludF8yNF91bmAgbWVkaXVtaW50KDI0KSBVTlNJR05FRCBERUZBVUxUIE5VTEwsCglgY19pbnRfMWAgaW50KDEpIERFRkFVTFQgTlVMTCwKCWBjX2ludF8zMmAgaW50KDMyKSBOT1QgTlVMTCBERUZBVUxUICcwJyBDT01NRU5UICdGb3IgbXVsdGkgcGsuJywKCWBjX2ludF8zMl91bmAgaW50KDMyKSBVTlNJR05FRCBERUZBVUxUIE5VTEwsCglgY19iaWdpbnRfMWAgYmlnaW50KDEpIERFRkFVTFQgTlVMTCwKCWBjX2JpZ2ludF82NGAgYmlnaW50KDY0KSBERUZBVUxUIE5VTEwsCglgY19iaWdpbnRfNjRfdW5gIGJpZ2ludCg2NCkgVU5TSUdORUQgREVGQVVMVCBOVUxMLAoJYGNfZGVjaW1hbGAgZGVjaW1hbCgxMCwgMCkgREVGQVVMVCBOVUxMLAoJYGNfZGVjaW1hbF9wcmAgZGVjaW1hbCg2NSwgMzApIERFRkFVTFQgTlVMTCwKCWBjX2Zsb2F0YCBmbG9hdCBERUZBVUxUIE5VTEwsCglgY19mbG9hdF9wcmAgZmxvYXQoMTAsIDMpIERFRkFVTFQgTlVMTCwKCWBjX2Zsb2F0X3VuYCBmbG9hdCgxMCwgMykgVU5TSUdORUQgREVGQVVMVCBOVUxMLAoJYGNfZG91YmxlYCBkb3VibGUgREVGQVVMVCBOVUxMLAoJYGNfZG91YmxlX3ByYCBkb3VibGUoMTAsIDMpIERFRkFVTFQgTlVMTCwKCWBjX2RvdWJsZV91bmAgZG91YmxlKDEwLCAzKSBVTlNJR05FRCBERUZBVUxUIE5VTEwsCglgY19kYXRlYCBkYXRlIERFRkFVTFQgTlVMTCBDT01NRU5UICdkYXRlJywKCWBjX2RhdGV0aW1lYCBkYXRldGltZSBERUZBVUxUIE5VTEwsCglgY19kYXRldGltZV8xYCBkYXRldGltZSgxKSBERUZBVUxUIE5VTEwsCglgY19kYXRldGltZV8zYCBkYXRldGltZSgzKSBERUZBVUxUIE5VTEwsCglgY19kYXRldGltZV82YCBkYXRldGltZSg2KSBERUZBVUxUIE5VTEwsCglgY190aW1lc3RhbXBgIHRpbWVzdGFtcCBOT1QgTlVMTCBERUZBVUxUIENVUlJFTlRfVElNRVNUQU1QLAoJYGNfdGltZXN0YW1wXzFgIHRpbWVzdGFtcCgxKSBOT1QgTlVMTCBERUZBVUxUICcxOTk5LTEyLTMxIDEyOjAwOjAwLjAnLAoJYGNfdGltZXN0YW1wXzNgIHRpbWVzdGFtcCgzKSBOT1QgTlVMTCBERUZBVUxUICcxOTk5LTEyLTMxIDEyOjAwOjAwLjAwMCcsCglgY190aW1lc3RhbXBfNmAgdGltZXN0YW1wKDYpIE5PVCBOVUxMIERFRkFVTFQgJzE5OTktMTItMzEgMTI6MDA6MDAuMDAwMDAwJywKCWBjX3RpbWVgIHRpbWUgREVGQVVMVCBOVUxMLAoJYGNfdGltZV8xYCB0aW1lKDEpIERFRkFVTFQgTlVMTCwKCWBjX3RpbWVfM2AgdGltZSgzKSBERUZBVUxUIE5VTEwsCglgY190aW1lXzZgIHRpbWUoNikgREVGQVVMVCBOVUxMLAoJYGNfeWVhcmAgeWVhcig0KSBERUZBVUxUIE5VTEwsCglgY195ZWFyXzRgIHllYXIoNCkgREVGQVVMVCBOVUxMLAoJYGNfY2hhcmAgY2hhcigxMCkgREVGQVVMVCBOVUxMLAoJYGNfdmFyY2hhcmAgdmFyY2hhcigxMCkgREVGQVVMVCBOVUxMLAoJYGNfYmluYXJ5YCBiaW5hcnkoMTApIERFRkFVTFQgTlVMTCwKCWBjX3ZhcmJpbmFyeWAgdmFyYmluYXJ5KDEwKSBERUZBVUxUIE5VTEwsCglgY19ibG9iX3RpbnlgIHRpbnlibG9iLAoJYGNfYmxvYmAgYmxvYiwKCWBjX2Jsb2JfbWVkaXVtYCBtZWRpdW1ibG9iLAoJYGNfYmxvYl9sb25nYCBsb25nYmxvYiwKCWBjX3RleHRfdGlueWAgdGlueXRleHQsCglgY190ZXh0YCB0ZXh0LAoJYGNfdGV4dF9tZWRpdW1gIG1lZGl1bXRleHQsCglgY190ZXh0X2xvbmdgIGxvbmd0ZXh0LAoJYGNfZW51bWAgZW51bSgnYScsICdiJywgJ2MnKSBERUZBVUxUIE5VTEwsCglgY19qc29uYCBqc29uIERFRkFVTFQgTlVMTCwKCWBjX2dlb21ldG9yeWAgZ2VvbWV0cnkgREVGQVVMVCBOVUxMLAoJYGNfcG9pbnRgIHBvaW50IERFRkFVTFQgTlVMTCwKCWBjX2xpbmVzdHJpbmdgIGxpbmVzdHJpbmcgREVGQVVMVCBOVUxMLAoJYGNfcG9seWdvbmAgcG9seWdvbiBERUZBVUxUIE5VTEwsCglgY19tdWx0aXBvaW50YCBtdWx0aXBvaW50IERFRkFVTFQgTlVMTCwKCWBjX211bHRpbGluZXN0cmluZ2AgbXVsdGlsaW5lc3RyaW5nIERFRkFVTFQgTlVMTCwKCWBjX211bHRpcG9seWdvbmAgbXVsdGlwb2x5Z29uIERFRkFVTFQgTlVMTCwKCWBjX2dlb21ldHJ5Y29sbGVjdGlvbmAgZ2VvbWV0cnljb2xsZWN0aW9uIERFRkFVTFQgTlVMTCwKCVBSSU1BUlkgS0VZIChgaWRgKSwKCUtFWSBgaWR4X2NfZG91YmxlYCAoYGNfZG91YmxlYCksCglLRVkgYGlkeF9jX2Zsb2F0YCAoYGNfZmxvYXRgKQopIEVOR0lORSA9ICdJTk5PREInIEFVVE9fSU5DUkVNRU5UID0gMTAwMjgxIERFRkFVTFQgQ0hBUlNFVCA9IHV0ZjhtYjQgREVGQVVMVCBDT0xMQVRFID0gdXRmOG1iNF9nZW5lcmFsX2NpIENPTU1FTlQgJzEwMDAwMDAwJwpQQVJUSVRJT04gQlkgS0VZIChgaWRgKSBQQVJUSVRJT05TIDM7\n"
            + "# POLARX_TSO=713618165110040172816672468666448650240000000000000000\n"
            + "CREATE TABLE `basetable` ( `id` bigint(20) NOT NULL AUTO_INCREMENT, `c_bit_1` bit(1) DEFAULT NULL, `c_bit_8` bit(8) DEFAULT NULL, `c_bit_16` bit(16) DEFAULT NULL, `c_bit_32` bit(32) DEFAULT NULL, `c_bit_64` bit(64) DEFAULT NULL, `c_tinyint_1` tinyint(1) DEFAULT NULL, `c_tinyint_1_un` tinyint(1) UNSIGNED DEFAULT NULL, `c_tinyint_4` tinyint(4) DEFAULT NULL, `c_tinyint_4_un` tinyint(4) UNSIGNED DEFAULT NULL, `c_tinyint_8` tinyint(8) DEFAULT NULL, `c_tinyint_8_un` tinyint(8) UNSIGNED DEFAULT NULL, `c_smallint_1` smallint(1) DEFAULT NULL, `c_smallint_16` smallint(16) DEFAULT NULL, `c_smallint_16_un` smallint(16) UNSIGNED DEFAULT NULL, `c_mediumint_1`mediumint(1) DEFAULT NULL, `c_mediumint_24` mediumint(24) DEFAULT NULL, `c_mediumint_24_un` mediumint(24) UNSIGNED DEFAULT NULL, `c_int_1` int(1) DEFAULT NULL, `c_int_32` int(32) NOT NULL DEFAULT '0' COMMENT 'For multi pk.', `c_int_32_un` int(32) UNSIGNED DEFAULT NULL, `c_bigint_1` bigint(1) DEFAULT NULL, `c_bigint_64` bigint(64) DEFAULT NULL, `c_bigint_64_un` bigint(64) UNSIGNED DEFAULT NULL, `c_decimal` decimal(10, 0) DEFAULT NULL, `c_decimal_pr` decimal(65, 30) DEFAULT NULL, `c_float` float DEFAULT NULL, `c_float_pr` float(10, 3) DEFAULT NULL, `c_float_un` float(10, 3) UNSIGNED DEFAULT NULL, `c_double` double DEFAULT NULL, `c_double_pr` double(10, 3) DEFAULT NULL, `c_double_un` double(10, 3) UNSIGNED DEFAULT NULL, `c_date` date DEFAULT NULL COMMENT 'date', `c_datetime` datetime DEFAULT NULL, `c_datetime_1` datetime(1) DEFAULT NULL, `c_datetime_3` datetime(3) DEFAULT NULL, `c_datetime_6` datetime(6) DEFAULT NULL, `c_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `c_timestamp_1` timestamp(1) NOT NULL DEFAULT '1999-12-31 12:00:00.0', `c_timestamp_3` timestamp(3) NOT NULL DEFAULT '1999-12-31 12:00:00.000', `c_timestamp_6` timestamp(6) NOT NULL DEFAULT '1999-12-31 12:00:00.000000', `c_time` time DEFAULT NULL, `c_time_1` time(1) DEFAULT NULL, `c_time_3` time(3) DEFAULT NULL, `c_time_6` time(6) DEFAULT NULL, `c_year` year(4) DEFAULT NULL, `c_year_4` year(4) DEFAULT NULL, `c_char` char(10) DEFAULT NULL, `c_varchar` varchar(10) DEFAULT NULL, `c_binary` binary(10) DEFAULT NULL, `c_varbinary` varbinary(10) DEFAULT NULL, `c_blob_tiny` tinyblob, `c_blob` blob, `c_blob_medium` mediumblob, `c_blob_long` longblob, `c_text_tiny` tinytext, `c_text` text, `c_text_medium` mediumtext, `c_text_long` longtext, `c_enum` enum('a', 'b', 'c') DEFAULT NULL, `c_json` json DEFAULT NULL, `c_geometory` geometry DEFAULT NULL, `c_point` point DEFAULT NULL, `c_linestring` linestring DEFAULT NULL, `c_polygon` polygon DEFAULT NULL, `c_multipoint` multipoint DEFAULT NULL, `c_multilinestring` multilinestring DEFAULT NULL, `c_multipolygon` multipolygon DEFAULT NULL, `c_geometrycollection` geometrycollection DEFAULT NULL, PRIMARY KEY (`id`), KEY `idx_c_double` (`c_double`), KEY `idx_c_float` (`c_float`) ) ENGINE = 'INNODB' AUTO_INCREMENT = 100281 DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci COMMENT '10000000'";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
    }

    @Test
    public void testParseCreateTableGroupSql() {
        String sql = "CREATE TABLEGROUP str_key_tg PARTITION BY KEY(VARCHAR(255)) "
            + "PARTITIONS 16 LOCALITY='dn=polardbx-storage-0-master'";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement sqlStatement = statementList.get(0);

        String toStringSql = sqlStatement.toString();
        Assert.assertEquals(
            "CREATE TABLEGROUP str_key_tg PARTITION BY KEY ( VARCHAR(255)) PARTITIONS 16 LOCALITY = 'dn=polardbx-storage-0-master'",
            toStringSql);

        parser = SQLParserUtils.createSQLStatementParser(toStringSql, DbType.mysql, SQL_PARSE_FEATURES);
        parser.parseStatementList();
    }

    @Test
    public void testRewriteWithTableGroupImplicit() {
        String sql = "alter table `t7` with tablegroup=single_tg468 implicit";
        SQLStatement statement =
            SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES).parseStatementList().get(0);
        Assert.assertEquals("ALTER TABLE `t7`\n"
            + "\tSET tablegroup = single_tg468 IMPLICIT FORCE", statement.toString());
    }

    @Test
    public void testLocalPartitionBy() {
        // toString时，SQLASTOutputVisitor将local partition的toString注释掉了，不知为何
        // SQLPartitionBy localPartitionBy = x.getLocalPartitioning();
        String sql = "CREATE TABLE alter_partition_ddl_primary_table_7viu ( "
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
            + "`c_int_1` int(1) DEFAULT NULL, "
            + "`c_int_32` int(32) NOT NULL DEFAULT 0, "
            + "`c_varchar` varchar(10) DEFAULT NULL, "
            + "`c_datetime` datetime DEFAULT NULL, "
            + "`c_datetime_1` datetime(1) DEFAULT NULL, "
            + "`c_datetime_3` datetime(3) DEFAULT NULL, "
            + "`c_datetime_6` datetime(6) DEFAULT NULL, "
            + "`c_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP, "
            + "`c_timestamp_1` timestamp(1) DEFAULT \"2000-01-01 00:00:00\", "
            + "`c_timestamp_3` timestamp(3) DEFAULT \"2000-01-01 00:00:00\", "
            + "`c_timestamp_6` timestamp(6) DEFAULT \"2000-01-01 00:00:00\", "
            + "`gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            + "PRIMARY KEY(`id`, `gmt_modified`) ) SINGLE "
            + "LOCAL PARTITION BY RANGE (gmt_modified) INTERVAL 1 MONTH EXPIRE AFTER 6 PRE ALLOCATE 6 PIVOTDATE NOW()";
        SQLStatement statement =
            SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES).parseStatementList().get(0);
        String expectSql = "CREATE TABLE alter_partition_ddl_primary_table_7viu (\n"
            + "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c_int_1` int(1) DEFAULT NULL,\n"
            + "\t`c_int_32` int(32) NOT NULL DEFAULT 0,\n"
            + "\t`c_varchar` varchar(10) DEFAULT NULL,\n"
            + "\t`c_datetime` datetime DEFAULT NULL,\n"
            + "\t`c_datetime_1` datetime(1) DEFAULT NULL,\n"
            + "\t`c_datetime_3` datetime(3) DEFAULT NULL,\n"
            + "\t`c_datetime_6` datetime(6) DEFAULT NULL,\n"
            + "\t`c_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`c_timestamp_1` timestamp(1) DEFAULT '2000-01-01 00:00:00',\n"
            + "\t`c_timestamp_3` timestamp(3) DEFAULT '2000-01-01 00:00:00',\n"
            + "\t`c_timestamp_6` timestamp(6) DEFAULT '2000-01-01 00:00:00',\n"
            + "\t`gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\tPRIMARY KEY (`id`, `gmt_modified`)\n"
            + ") SINGLE";
        Assert.assertEquals(expectSql, statement.toString());
    }

    @Test
    public void testCreateGlobalIndexToString() {
        String sql = "CREATE GLOBAL INDEX g_i_pk_type ON gsi_primary_table(id) COVERING(c_bit_1) "
            + "DBPARTITION BY HASH(id) TBPARTITION BY HASH(id) TBPARTITIONS 3";
        SQLStatement statement =
            SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES).parseStatementList().get(0);
        Assert.assertEquals(
            "CREATE GLOBAL INDEX g_i_pk_type ON gsi_primary_table (id) COVERING (c_bit_1) DBPARTITION BY HASH(id) TBPARTITION BY HASH(id) TBPARTITIONS 3",
            statement.toString());
    }

    @Test
    public void testParseSetPassword() {
        String sql = "set password for user@'%' = '123456'";
        SQLStatement statement =
            SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES).parseStatementList().get(0);
        Assert.assertEquals("SET PASSWORD FOR 'user'@'%' = '123456'", statement.toString());
    }
}
