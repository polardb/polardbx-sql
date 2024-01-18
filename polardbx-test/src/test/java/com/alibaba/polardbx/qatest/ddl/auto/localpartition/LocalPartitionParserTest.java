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

package com.alibaba.polardbx.qatest.ddl.auto.localpartition;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Optional;

public class LocalPartitionParserTest {

    /**
     * com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlCreateTableParser.parseCreateTable(boolean)
     * <p>
     * com/alibaba/polardbx/druid/sql/dialect/mysql/parser/MySqlCreateTableParser.java:426
     */
    @Test
    public void testParse() {
        String sql1 = "CREATE TABLE order (\n"
            + "\tid bigint,\n"
            + "\tseller_id bigint,\n"
            + "\tgmt_modified DATETIME,\n"
            + "\tPRIMARY KEY (id, gmt_modified),\n"
            + "\tINDEX `_local_sdx`(seller_id)\n"
            + ") /*!50500 PARTITION BY RANGE COLUMNS (gmt_modified) (\n"
            + "\tPARTITION p20211123 VALUES LESS THAN ('2021-11-23') COMMENT '',\n"
            + "\tPARTITION p20211223 VALUES LESS THAN ('2021-12-23') COMMENT '',\n"
            + "\tPARTITION p20220123 VALUES LESS THAN ('2022-01-23') COMMENT '',\n"
            + "\tPARTITION pmax VALUES LESS THAN MAXVALUE COMMENT ''\n"
            + ")*/";

        final MySqlCreateTableStatement astCreateIndexTable = (MySqlCreateTableStatement) SQLUtils
            .parseStatements(sql1, JdbcConstants.MYSQL, SQLParserFeature.KeepComments).get(0).clone();

        System.out.println(astCreateIndexTable.toString());
        Assert.assertTrue(astCreateIndexTable.toString().contains("50500 PARTITION BY RANGE COLUMNS"));
    }

    @Test
    public void testParse2() {
        String sql1 = "CREATE TABLE t_create_hodk (\n"
            + "\tc1 bigint,\n"
            + "\tc2 bigint,\n"
            + "\tc3 bigint,\n"
            + "\tgmt_modified DATETIME PRIMARY KEY NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH (c1) PARTITIONS 4 /*!50500 PARTITION BY RANGE COLUMNS (gmt_modified) (\n"
            + "\tPARTITION p20211123 VALUES LESS THAN ('2021-11-23') COMMENT '',\n"
            + "\tPARTITION p20211223 VALUES LESS THAN ('2021-12-23') COMMENT '',\n"
            + "\tPARTITION p20220123 VALUES LESS THAN ('2022-01-23') COMMENT '',\n"
            + "\tPARTITION p20220223 VALUES LESS THAN ('2022-02-23') COMMENT '',\n"
            + "\tPARTITION p20220323 VALUES LESS THAN ('2022-03-23') COMMENT '',\n"
            + "\tPARTITION p20220423 VALUES LESS THAN ('2022-04-23') COMMENT '',\n"
            + "\tPARTITION pmax VALUES LESS THAN MAXVALUE COMMENT ''\n"
            + ")*/";

        final MySqlCreateTableStatement astCreateIndexTable = (MySqlCreateTableStatement) SQLUtils
            .parseStatements(sql1, JdbcConstants.MYSQL, SQLParserFeature.KeepComments).get(0).clone();

        System.out.println(astCreateIndexTable.toString());
        Assert.assertTrue(astCreateIndexTable.toString().contains("50500 PARTITION BY RANGE COLUMNS"));
    }

    @Test
    public void testRotate7() {
        LocalPartitionDefinitionInfo definitionInfo = new LocalPartitionDefinitionInfo();
//        definitionInfo.setStartWithDate(StringTimeParser.parseDatetime("2021-01-01".getBytes()));
        definitionInfo.setTableName("table_name");
        definitionInfo.setTableSchema("table_schema");
        definitionInfo.setColumnName("gmt_created");
        definitionInfo.setPivotDateExpr("NOW()");
        definitionInfo.setIntervalCount(1);
        definitionInfo.setIntervalUnit("MONTH");
        definitionInfo.setExpireAfterCount(12);
        definitionInfo.setPreAllocateCount(6);

        {
            LocalDate localDate = LocalDate.of(2021, 12, 27);
            MysqlDateTime newest = localdatetime2mysqldatetime(localDate);
            MysqlDateTime pivot = localdatetime2mysqldatetime(localDate.plusMonths(26));
            Optional<SQLPartitionByRange> sqlPartitionByRange =
                LocalPartitionDefinitionInfo.generatePreAllocateLocalPartitionStmt(
                    definitionInfo,
                    newest,
                    pivot
                );
            System.out.println(
                "sqlPartitionByRange.get().getPartitions().size() :" + sqlPartitionByRange.get().getPartitions()
                    .size());
            org.junit.Assert.assertEquals(33, sqlPartitionByRange.get().getPartitions().size());
        }

        {
            LocalDate localDate = LocalDate.of(2021, 12, 28);
            MysqlDateTime newest = localdatetime2mysqldatetime(localDate);
            MysqlDateTime pivot = localdatetime2mysqldatetime(localDate.plusMonths(26));
            Optional<SQLPartitionByRange> sqlPartitionByRange =
                LocalPartitionDefinitionInfo.generatePreAllocateLocalPartitionStmt(
                    definitionInfo,
                    newest,
                    pivot
                );
            System.out.println(
                "sqlPartitionByRange.get().getPartitions().size() :" + sqlPartitionByRange.get().getPartitions()
                    .size());
            org.junit.Assert.assertEquals(33, sqlPartitionByRange.get().getPartitions().size());
        }

        {
            LocalDate localDate = LocalDate.of(2021, 12, 29);
            MysqlDateTime newest = localdatetime2mysqldatetime(localDate);
            MysqlDateTime pivot = localdatetime2mysqldatetime(localDate.plusMonths(26));
            Optional<SQLPartitionByRange> sqlPartitionByRange =
                LocalPartitionDefinitionInfo.generatePreAllocateLocalPartitionStmt(
                    definitionInfo,
                    newest,
                    pivot
                );
            System.out.println(
                "sqlPartitionByRange.get().getPartitions().size() :" + sqlPartitionByRange.get().getPartitions()
                    .size());
            org.junit.Assert.assertEquals(33, sqlPartitionByRange.get().getPartitions().size());
        }

        {
            LocalDate localDate = LocalDate.of(2021, 12, 30);
            MysqlDateTime newest = localdatetime2mysqldatetime(localDate);
            MysqlDateTime pivot = localdatetime2mysqldatetime(localDate.plusMonths(26));
            Optional<SQLPartitionByRange> sqlPartitionByRange =
                LocalPartitionDefinitionInfo.generatePreAllocateLocalPartitionStmt(
                    definitionInfo,
                    newest,
                    pivot
                );
            System.out.println(
                "sqlPartitionByRange.get().getPartitions().size() :" + sqlPartitionByRange.get().getPartitions()
                    .size());
            org.junit.Assert.assertEquals(32, sqlPartitionByRange.get().getPartitions().size());
        }

        {
            LocalDate localDate = LocalDate.of(2021, 12, 31);
            MysqlDateTime newest = localdatetime2mysqldatetime(localDate);
            MysqlDateTime pivot = localdatetime2mysqldatetime(localDate.plusMonths(26));
            Optional<SQLPartitionByRange> sqlPartitionByRange =
                LocalPartitionDefinitionInfo.generatePreAllocateLocalPartitionStmt(
                    definitionInfo,
                    newest,
                    pivot
                );
            System.out.println(
                "sqlPartitionByRange.get().getPartitions().size() :" + sqlPartitionByRange.get().getPartitions()
                    .size());
            org.junit.Assert.assertEquals(32, sqlPartitionByRange.get().getPartitions().size());
        }

    }

    private MysqlDateTime localdatetime2mysqldatetime(LocalDate localDate) {
        MysqlDateTime mysqlDateTime = new MysqlDateTime(
            localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(),
            15L, 0L, 0L, 0L
        );
        return mysqlDateTime;
    }
}