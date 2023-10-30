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

package com.alibaba.polardbx.druid.sql.parser;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateJavaFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropJavaFunctionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLStatementParserTest {

    @Test
    public void testParseCreateTablePartitionBy() {
        String sql =
            "create table if not exists `t_hash_hash_not_template_1690868091444` ("
                + "a bigint unsigned not null,"
                + "b bigint unsigned not null,"
                + "c datetime NOT NULL,"
                + "d varchar(16) NOT NULL,"
                + "e varchar(16) NOT NULL) "
                + "partition by hash (c,d) partitions 2 subpartition by hash (a,b) "
                + "(  partition p1 subpartitions 2, partition p2 subpartitions 4)";
        SQLParserFeature[] FEATURES = {
            SQLParserFeature.EnableSQLBinaryOpExprGroup,
            SQLParserFeature.UseInsertColumnsCache, SQLParserFeature.OptimizedForParameterized,
            SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL,
            SQLParserFeature.DRDSBaseline, SQLParserFeature.DrdsMisc, SQLParserFeature.DrdsGSI, SQLParserFeature.DrdsCCL
        };
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statement = statementList.get(0);
        String parserResult = statement.toString();
        // 反向parse一遍
        parser = SQLParserUtils.createSQLStatementParser(parserResult, DbType.mysql, FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        sql = statement.toString();
    }

    @Test
    public void testUnparserOriginSqlWithSubPart_h_hc_ntp() {
        String content =
            "create table if not exists `h_hc_ntp` (a bigint unsigned not null,b bigint unsigned not null,c datetime NOT NULL,d varchar(16) NOT NULL,e varchar(16) NOT NULL)\n"
                + "partition by hash (c,d) partitions 2 \n"
                + "subpartition by hash (a,b)\n"
                + "(  \t\n"
                + "\tpartition p1 subpartitions 2,  \n"
                + "\tpartition p2 subpartitions 4\n"
                + ")\n";
        String expectResult = "CREATE TABLE IF NOT EXISTS `h_hc_ntp` (\n"
            + "\ta bigint UNSIGNED NOT NULL,\n"
            + "\tb bigint UNSIGNED NOT NULL,\n"
            + "\tc datetime NOT NULL,\n"
            + "\td varchar(16) NOT NULL,\n"
            + "\te varchar(16) NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH (c, d) PARTITIONS 2\n"
            + "SUBPARTITION BY HASH (a, b) (\n"
            + "\tPARTITION p1\n"
            + "\t\tSUBPARTITIONS 2, \n"
            + "\tPARTITION p2\n"
            + "\t\tSUBPARTITIONS 4\n"
            + ")";

        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));

        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(1, parseResult.size());

        SQLStatement stmt = parseResult.get(0);
        String unparseSql = SQLUtils.toSQLString(stmt, DbType.mysql, null, VisitorFeature.OutputHashPartitionsByRange);
        Assert.assertEquals(expectResult, unparseSql);
        String unparseSql2 = stmt.toString();
        Assert.assertEquals(expectResult, unparseSql2);
    }

    @Test
    public void testUnparserOriginSqlWithSubPart2_k_k_tp() {
        String content =
            "    create table if not exists k_k_tp (\n"
                + "        a bigint unsigned not null,\n"
                + "        b bigint unsigned not null,\n"
                + "        c datetime NOT NULL,\n"
                + "        d varchar(16) NOT NULL,\n"
                + "    e varchar(16) NOT NULL\n"
                + ")\n"
                + "    partition by key (c,d)\n"
                + "    partitions 4\n"
                + "    subpartition by key (a,b)\n"
                + "    subpartitions 4;";

        String expectResult = "CREATE TABLE IF NOT EXISTS k_k_tp (\n"
            + "\ta bigint UNSIGNED NOT NULL,\n"
            + "\tb bigint UNSIGNED NOT NULL,\n"
            + "\tc datetime NOT NULL,\n"
            + "\td varchar(16) NOT NULL,\n"
            + "\te varchar(16) NOT NULL\n"
            + ")\n"
            + "PARTITION BY KEY (c, d) PARTITIONS 4\n"
            + "SUBPARTITION BY KEY (a, b) SUBPARTITIONS 4;";

        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));

        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(1, parseResult.size());

        SQLStatement stmt = parseResult.get(0);
        String unparseSql = SQLUtils.toSQLString(stmt, DbType.mysql, null, VisitorFeature.OutputHashPartitionsByRange);
        Assert.assertEquals(expectResult, unparseSql);

        String unparseSql2 = stmt.toString();
        Assert.assertEquals(expectResult, unparseSql2);
    }

    @Test
    public void testUnparserOriginSqlWithSubPart2_k_lc_ntp1() {
        String content =
            "create table if not exists k_lc_ntp1 (\n"
                + "a bigint unsigned not null,\n"
                + "b bigint unsigned not null,\n"
                + "c datetime NOT NULL,\n"
                + "d varchar(16) NOT NULL,\n"
                + "e varchar(16) NOT NULL\n"
                + ")\n"
                + "partition by key (c,d)\n"
                + "subpartition by list columns (a,b)\n"
                + "(\n"
                + "partition p1 (\n"
                + "subpartition sp0 values in ((5,5),(6,6)),\n"
                + "subpartition sp1 values in ((7,7),(8,8))\n"
                + "),\n"
                + "partition p2 (\n"
                + "subpartition sp2 values in ((5,5),(6,6)),\n"
                + "subpartition sp3 values in ((17,17),(18,18))\n"
                + ")\n"
                + ")\n"
                + ";";
        String expectResult = "CREATE TABLE IF NOT EXISTS k_lc_ntp1 (\n"
            + "\ta bigint UNSIGNED NOT NULL,\n"
            + "\tb bigint UNSIGNED NOT NULL,\n"
            + "\tc datetime NOT NULL,\n"
            + "\td varchar(16) NOT NULL,\n"
            + "\te varchar(16) NOT NULL\n"
            + ")\n"
            + "PARTITION BY KEY (c, d)\n"
            + "SUBPARTITION BY LIST COLUMNS (a, b) (\n"
            + "\tPARTITION p1 (\n"
            + "\t\tSUBPARTITION sp0 VALUES IN ((5, 5), (6, 6)),\n"
            + "\t\tSUBPARTITION sp1 VALUES IN ((7, 7), (8, 8))\n"
            + "\t), \n"
            + "\tPARTITION p2 (\n"
            + "\t\tSUBPARTITION sp2 VALUES IN ((5, 5), (6, 6)),\n"
            + "\t\tSUBPARTITION sp3 VALUES IN ((17, 17), (18, 18))\n"
            + "\t)\n"
            + ");";

        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));

        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(1, parseResult.size());

        SQLStatement stmt = parseResult.get(0);
        String unparseSql = SQLUtils.toSQLString(stmt, DbType.mysql, null, VisitorFeature.OutputHashPartitionsByRange);
        Assert.assertEquals(expectResult, unparseSql);
        String unparseSql2 = stmt.toString();
        Assert.assertEquals(expectResult, unparseSql2);
    }

    @Test
    public void testUnparserOriginSqlWithSubPart2_k_rc_ntp1() {
        String content =
            "create table if not exists k_rc_ntp1 (\n"
                + "a bigint unsigned not null,\n"
                + "b bigint unsigned not null,\n"
                + "c datetime NOT NULL,\n"
                + "d varchar(16) NOT NULL,\n"
                + "e varchar(16) NOT NULL\n"
                + ")\n"
                + "partition by key (c,d)\n"
                + "subpartition by range columns (a,b)\n"
                + "(\n"
                + "partition p1 values less than (3,9223372036854775807) (\n"
                + "subpartition sp0 values less than (5,5),\n"
                + "subpartition sp1 values less than (maxvalue,maxvalue)\n"
                + "),\n"
                + "partition p2 values less than (4611686018427387905,9223372036854775807) (\n"
                + "subpartition sp2 values less than (5,6),\n"
                + "subpartition sp3 values less than (maxvalue,maxvalue)\n"
                + "),\n"
                + "partition p3 values less than (9223372036854775807,9223372036854775807) (\n"
                + "subpartition sp4 values less than (5,7),\n"
                + "subpartition sp5 values less than (maxvalue,maxvalue)\n"
                + ")\n"
                + ");";
        String expectResult = "CREATE TABLE IF NOT EXISTS k_rc_ntp1 (\n"
            + "\ta bigint UNSIGNED NOT NULL,\n"
            + "\tb bigint UNSIGNED NOT NULL,\n"
            + "\tc datetime NOT NULL,\n"
            + "\td varchar(16) NOT NULL,\n"
            + "\te varchar(16) NOT NULL\n"
            + ")\n"
            + "PARTITION BY KEY (c, d)\n"
            + "SUBPARTITION BY RANGE COLUMNS (a, b) (\n"
            + "\tPARTITION p1 VALUES LESS THAN (3, 9223372036854775807) (\n"
            + "\t\tSUBPARTITION sp0 VALUES LESS THAN (5, 5),\n"
            + "\t\tSUBPARTITION sp1 VALUES LESS THAN (maxvalue, maxvalue)\n"
            + "\t), \n"
            + "\tPARTITION p2 VALUES LESS THAN (4611686018427387905, 9223372036854775807) (\n"
            + "\t\tSUBPARTITION sp2 VALUES LESS THAN (5, 6),\n"
            + "\t\tSUBPARTITION sp3 VALUES LESS THAN (maxvalue, maxvalue)\n"
            + "\t), \n"
            + "\tPARTITION p3 VALUES LESS THAN (9223372036854775807, 9223372036854775807) (\n"
            + "\t\tSUBPARTITION sp4 VALUES LESS THAN (5, 7),\n"
            + "\t\tSUBPARTITION sp5 VALUES LESS THAN (maxvalue, maxvalue)\n"
            + "\t)\n"
            + ");";

        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));

        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(1, parseResult.size());

        SQLStatement stmt = parseResult.get(0);
        String unparseSql = SQLUtils.toSQLString(stmt, DbType.mysql, null, VisitorFeature.OutputHashPartitionsByRange);
        Assert.assertEquals(expectResult, unparseSql);
        String unparseSql2 = stmt.toString();
        Assert.assertEquals(expectResult, unparseSql2);
    }

    @Test
    public void testUnparserOriginSqlWithSubPart2_r_h_todays_tp1() {
        String content =
            "CREATE TABLE `ts` (\n"
                + "  `id` int(11) DEFAULT NULL,\n"
                + "  `purchased` date DEFAULT NULL,\n"
                + "  `_drds_implicit_id_` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "  PRIMARY KEY (`_drds_implicit_id_`),\n"
                + "  KEY `auto_shard_key_purchased` USING BTREE (`purchased`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY RANGE(YEAR(`purchased`))\n"
                + "SUBPARTITION BY HASH(TO_DAYS(`purchased`))\n"
                + "SUBPARTITIONS 2\n"
                + "(PARTITION `p0` VALUES LESS THAN (1990),\n"
                + " PARTITION `p1` VALUES LESS THAN (2000),\n"
                + " PARTITION `p2` VALUES LESS THAN (MAXVALUE))";
        String expectResult = "CREATE TABLE `ts` (\n"
            + "\t`id` int(11) DEFAULT NULL,\n"
            + "\t`purchased` date DEFAULT NULL,\n"
            + "\t`_drds_implicit_id_` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "\tPRIMARY KEY (`_drds_implicit_id_`),\n"
            + "\tKEY `auto_shard_key_purchased` USING BTREE (`purchased`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY RANGE (YEAR(`purchased`))\n"
            + "SUBPARTITION BY HASH (TO_DAYS(`purchased`)) SUBPARTITIONS 2 (\n"
            + "\tPARTITION `p0` VALUES LESS THAN (1990),\n"
            + "\tPARTITION `p1` VALUES LESS THAN (2000),\n"
            + "\tPARTITION `p2` VALUES LESS THAN MAXVALUE\n"
            + ")";

        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));

        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(1, parseResult.size());

        SQLStatement stmt = parseResult.get(0).clone();
        String unparseSql = SQLUtils.toSQLString(stmt, DbType.mysql, null, VisitorFeature.OutputHashPartitionsByRange);
        Assert.assertEquals(expectResult, unparseSql);
        String unparseSql2 = stmt.toString();
        Assert.assertEquals(expectResult, unparseSql2);
    }

    @Test
    public void testParserOriginSqlAfterHashTag() {
        String content =
            "# POLARX_ORIGIN_SQL=CREATE TABLE test ( id int, _drds_implicit_id_ bigint AUTO_INCREMENT, PRIMARY KEY (_drds_implicit_id_) )\n"
                + "# POLARX_TSO=706157053851834784015926357519950192640000000000000000\n"
                + "CREATE TABLE test ( id int ) DEFAULT CHARACTER SET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci;";
        String expectResult = "CREATE TABLE test (\n"
            + "\tid int,\n"
            + "\t_drds_implicit_id_ bigint AUTO_INCREMENT,\n"
            + "\tPRIMARY KEY (_drds_implicit_id_)\n"
            + ")";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(1, parseResult.size());
        Assert.assertEquals(expectResult, parseResult.get(0).toString());
    }

    @Test
    public void testParserOriginSqlAfterHashTagWithHint() {
        String content =
            "/*+TDDL:cmd_extra(TSO=706967379996416416016007390155295989780000000000000000)*/"
                + "# POLARX_ORIGIN_SQL=/*+TDDL:CMD_EXTRA(OMC_FORCE_TYPE_CONVERSION=TRUE)*/ "
                + "ALTER TABLE column_backfill_ts_tbl MODIFY COLUMN c1_1 timestamp(6) DEFAULT current_timestamp(6) "
                + "ON UPDATE current_timestamp(6), ALGORITHM = omc\n"
                + "# POLARX_TSO=706967379996416416016007390155295989780000000000000000\n"
                + "/*+tddl:cmd_extra(omc_force_type_conversion=true)*/\n"
                + "ALTER TABLE column_backfill_ts_tbl\n"
                + "  MODIFY COLUMN c1_1 timestamp(6) DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6)";
        String expectResult =
            "/*+TDDL:cmd_extra(TSO=706967379996416416016007390155295989780000000000000000)*/\n"
                + "/*+TDDL:CMD_EXTRA(OMC_FORCE_TYPE_CONVERSION=TRUE)*/\n"
                + "ALTER TABLE column_backfill_ts_tbl\n"
                + "\tMODIFY COLUMN c1_1 timestamp(6) DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6),\n"
                + "\tALGORITHM = omc";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(1, parseResult.size());
        Assert.assertEquals(expectResult, parseResult.get(0).toString());
    }

    @Test
    public void test() {
        String content =
            "/*+TDDL:cmd_extra(TSO=706967379996416416016007390155295989780000000000000000)*/ "
                + "/*+TDDL:CMD_EXTRA(OMC_FORCE_TYPE_CONVERSION=TRUE)*/ "
                + "ALTER TABLE column_backfill_ts_tbl MODIFY COLUMN c1_1 timestamp(6) "
                + "DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), ALGORITHM = omc";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));
        List<SQLStatement> parseResult = parser.parseStatementList();
        System.out.println(parseResult.get(0));
    }

    @Test
    public void testInsertFunction() throws SQLException {
        MySqlStatementParser sqlStatementParser = new MySqlStatementParser(
            "insert into batch_features_pb (id, url, feature) values(0, 'https://cbu01.alicdn.com/img/ibank/2013/441/486/1036684144_687583347.jpg', CLOTHES_FEATURE_EXTRACT_V1(url))");
        sqlStatementParser.config(SQLParserFeature.InsertReader, true);

        MyInsertValueHandler myInsertValueHandler = new MyInsertValueHandler();
        sqlStatementParser.parseInsert();
        sqlStatementParser.parseValueClause(myInsertValueHandler);
        Assert.assertTrue(myInsertValueHandler.getFunctions().containsKey("CLOTHES_FEATURE_EXTRACT_V1"));
        Assert.assertEquals("url",
            myInsertValueHandler.getFunctions().get("CLOTHES_FEATURE_EXTRACT_V1"));
    }

    @Test
    public void testInsertFunction2()
        throws SQLException {
        MySqlStatementParser sqlStatementParser = new MySqlStatementParser(
            "insert into batch_features_pb (id, url, feature) values(0, \"https://cbu01.alicdn.com/img/ibank/2013/441/486/1036684144_687583347.jpg\", CLOTHES_FEATURE_EXTRACT_V1(url))");
        sqlStatementParser.config(SQLParserFeature.InsertReader, true);

        MyInsertValueHandler myInsertValueHandler = new MyInsertValueHandler();
        sqlStatementParser.parseInsert();
        sqlStatementParser.parseValueClause(myInsertValueHandler);
        Assert.assertTrue(myInsertValueHandler.getFunctions().containsKey("CLOTHES_FEATURE_EXTRACT_V1"));
        Assert.assertEquals("url",
            myInsertValueHandler.getFunctions().get("CLOTHES_FEATURE_EXTRACT_V1"));
    }

    @Test
    public void testInsertFunctions()
        throws SQLException {
        MySqlStatementParser sqlStatementParser = new MySqlStatementParser(
            "insert into batch_features_pb (id, url, feature) values(0, 'https://cbu01.alicdn.com/img/ibank/2013/441/486/1036684144_687583347.jpg', CLOTHES_FEATURE_EXTRACT_V1(url), CLOTHES_ATTRIBUTE_EXTRACT_V1(url))");
        sqlStatementParser.config(SQLParserFeature.InsertReader, true);

        MyInsertValueHandler myInsertValueHandler = new MyInsertValueHandler();
        sqlStatementParser.parseInsert();
        sqlStatementParser.parseValueClause(myInsertValueHandler);
        Assert.assertTrue(myInsertValueHandler.getFunctions().containsKey("CLOTHES_FEATURE_EXTRACT_V1"));
        Assert.assertTrue(myInsertValueHandler.getFunctions().containsKey("CLOTHES_ATTRIBUTE_EXTRACT_V1"));
        Assert.assertEquals("url",
            myInsertValueHandler.getFunctions().get("CLOTHES_FEATURE_EXTRACT_V1"));
        Assert.assertEquals("url",
            myInsertValueHandler.getFunctions().get("CLOTHES_ATTRIBUTE_EXTRACT_V1"));
    }

    private class MyInsertValueHandler
        implements SQLInsertValueHandler {
        private Map<String, Object> functions = new HashMap<String, Object>();

        public Map<String, Object> getFunctions() {
            return functions;
        }

        @Override
        public Object newRow()
            throws SQLException {
            return null;
        }

        @Override
        public void processInteger(Object row, int index, Number value)
            throws SQLException {

        }

        @Override
        public void processString(Object row, int index, String value)
            throws SQLException {

        }

        @Override
        public void processDate(Object row, int index, String value)
            throws SQLException {

        }

        @Override
        public void processDate(Object row, int index, Date value)
            throws SQLException {

        }

        @Override
        public void processTimestamp(Object row, int index, String value)
            throws SQLException {

        }

        @Override
        public void processTimestamp(Object row, int index, Date value)
            throws SQLException {

        }

        @Override
        public void processTime(Object row, int index, String value)
            throws SQLException {

        }

        @Override
        public void processDecimal(Object row, int index, BigDecimal value)
            throws SQLException {

        }

        @Override
        public void processBoolean(Object row, int index, boolean value)
            throws SQLException {

        }

        @Override
        public void processNull(Object row, int index)
            throws SQLException {

        }

        @Override
        public void processFunction(Object row, int index, String funcName, long funcNameHashCode64, Object... values)
            throws SQLException {
            this.functions.put(funcName, values[0]);
        }

        @Override
        public void processRow(Object row)
            throws SQLException {

        }

        @Override
        public void processComplete()
            throws SQLException {

        }
    }
}