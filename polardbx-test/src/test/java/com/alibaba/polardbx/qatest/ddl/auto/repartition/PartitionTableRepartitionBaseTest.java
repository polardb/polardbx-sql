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

package com.alibaba.polardbx.qatest.ddl.auto.repartition;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.ddl.sharding.repartition.RepartitionBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public abstract class PartitionTableRepartitionBaseTest extends RepartitionBaseTest {
    /**
     * 分区模板
     */
    static final String NEW_PARTITIONING_TEMPLATE =
        "PARTITION BY {0}({1}) PARTITIONS {2}";

    /**
     * 分区模板
     */
    static final String NEW_PARTITIONING_TEMPLATE2 =
        "PARTITION BY {0}({1})";

    /**
     * 分区模板
     */
    static final String NEW_PARTITIONING_TEMPLATE3 =
        "PARTITION BY {0}({1}({2}))";

    static final String PARTITIONING_RANGE_TEMPLATE =
        "\n(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB,\n"
            + " PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
            + " PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB,\n"
            + " PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB)";

    static final String PARTITIONING_RANGE_COLUMN_TEMPLATE =
        "\n(PARTITION p0 VALUES LESS THAN ('ab', '1998-01-01 00:00:00') ENGINE = InnoDB,\n"
            + " PARTITION p1 VALUES LESS THAN ('bc', '1999-01-01 00:00:00') ENGINE = InnoDB,\n"
            + " PARTITION p2 VALUES LESS THAN ('cd', '2000-01-01 00:00:00') ENGINE = InnoDB,\n"
            + " PARTITION p3 VALUES LESS THAN (MAXVALUE, MAXVALUE) ENGINE = InnoDB)";

    static final String PARTITIONING_LIST_TEMPLATE =
        "(PARTITION p0 VALUES IN (1800,1801,1802) ENGINE = InnoDB,\n"
            + " PARTITION p1 VALUES IN (1900,2020) ENGINE = InnoDB,\n"
            + " PARTITION p2 VALUES IN (1990,1991,1992) ENGINE = InnoDB,\n"
            + " PARTITION p3 VALUES IN (2010,2012) ENGINE = InnoDB)";

    static final String PARTITIONING_LIST_COLUMN_TEMPLATE =
        "\n(PARTITION p0 VALUES IN ((1, 'ab'),(3, 'bc'),(5, 'cd'),(7, 'ef') ) ENGINE = InnoDB,\n"
            + "PARTITION p1 VALUES IN ((2, 'ABC'),(4, 'CDE'),(6, 'EFG')) ENGINE = InnoDB,\n"
            + "PARTITION p2 VALUES IN ((10, 'XY'),(13, 'ZZ')) ENGINE = InnoDB)";

    static final String LOCAL_PARTITION_TABLE = "CREATE TABLE %s (\n"
        + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "  `c_int_1` int(1) DEFAULT NULL,\n"
        + "  `c_int_32` int(32) NOT NULL DEFAULT 0,\n"
        + "  `c_varchar` varchar(10) DEFAULT NULL,\n"
        + "  `c_datetime` datetime DEFAULT NULL,\n"
        + "  `c_datetime_1` datetime(1) DEFAULT NULL,\n"
        + "  `c_datetime_3` datetime(3) DEFAULT NULL,\n"
        + "  `c_datetime_6` datetime(6) DEFAULT NULL,\n"
        + "  `c_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
        + "  `c_timestamp_1` timestamp(1) DEFAULT \"2000-01-01 00:00:00\",\n"
        + "  `c_timestamp_3` timestamp(3) DEFAULT \"2000-01-01 00:00:00\",\n"
        + "  `c_timestamp_6` timestamp(6) DEFAULT \"2000-01-01 00:00:00\",\n"
        + "  `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
        + "  PRIMARY KEY(`id`, `gmt_modified`)\n"
        + ")\n"
        + "%s\n"
        + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
        + "INTERVAL 1 MONTH\n"
        + "EXPIRE AFTER 6\n"
        + "PRE ALLOCATE 6\n"
        + "PIVOTDATE NOW()\n"
        + ";";

    protected String buildPartitionRule(PartitionParam p) {
        String partitionRule = "";
        if (StringUtils.equalsIgnoreCase(p.broadcastOrSingle, "SINGLE")) {
            partitionRule = "SINGLE";
        } else if (StringUtils.equalsIgnoreCase(p.broadcastOrSingle, "BROADCAST")) {
            partitionRule = "BROADCAST";
        } else if (StringUtils.equalsIgnoreCase(p.operator, "HASH") || StringUtils.equalsIgnoreCase(p.operator,
            "KEY")) {
            partitionRule = MessageFormat.format(NEW_PARTITIONING_TEMPLATE, p.operator, p.operand, p.partCount);
        } else if (StringUtils.equalsIgnoreCase(p.operator, "RANGE")) {
            partitionRule = (p.func == null ? MessageFormat.format(NEW_PARTITIONING_TEMPLATE2, p.operator, p.operand)
                : MessageFormat.format(NEW_PARTITIONING_TEMPLATE3, p.operator, p.func, p.operand))
                + PARTITIONING_RANGE_TEMPLATE;
        } else if (StringUtils.equalsIgnoreCase(p.operator, "LIST")) {
            partitionRule = (p.func == null ? MessageFormat.format(NEW_PARTITIONING_TEMPLATE2, p.operator, p.operand)
                : MessageFormat.format(NEW_PARTITIONING_TEMPLATE3, p.operator, p.func, p.operand))
                + PARTITIONING_LIST_TEMPLATE;
        } else if (StringUtils.equalsIgnoreCase(p.operator, "RANGE COLUMNS")) {
            partitionRule = MessageFormat.format(NEW_PARTITIONING_TEMPLATE2, p.operator, p.operand)
                + PARTITIONING_RANGE_COLUMN_TEMPLATE;
        } else if (StringUtils.equalsIgnoreCase(p.operator, "LIST COLUMNS")) {
            partitionRule = MessageFormat.format(NEW_PARTITIONING_TEMPLATE2, p.operator, p.operand)
                + PARTITIONING_LIST_COLUMN_TEMPLATE;
        }

        return partitionRule;
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    /**
     * 创建主表(不包含局部索引)
     */
    @Override
    protected void createPrimaryTable(String primaryTableName, PartitionParam p, boolean createMysqlTable)
        throws SQLException {
        String partitionRule = buildPartitionRule(p);

        String createTableSql;

        if (StringUtils.equals(MULTI_PK_PRIMARY_TABLE_NAME, primaryTableName)) {
            createTableSql = ExecuteTableSelect.getFullTypeMultiPkTableDef(primaryTableName, partitionRule);
        } else if (StringUtils.equals(DEFAULT_PRIMARY_TABLE_NAME, primaryTableName)) {
            createTableSql = ExecuteTableSelect.getFullTypeTableDef(primaryTableName, partitionRule);
        } else {
            createTableSql = String.format(LOCAL_PARTITION_TABLE, primaryTableName, partitionRule);
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        final ResultSet resultSet = JdbcUtil.executeQuery(
            "SELECT COUNT(1) FROM " + primaryTableName,
            tddlConnection);
        assertThat(resultSet.next(), is(true));

        if (createMysqlTable) {
            String createMysqlTableSql;
            if (StringUtils.equals(MULTI_PK_PRIMARY_TABLE_NAME, primaryTableName)) {
                createMysqlTableSql = ExecuteTableSelect.getFullTypeMultiPkTableDef(primaryTableName, "");
            } else {
                createMysqlTableSql = ExecuteTableSelect.getFullTypeTableDef(primaryTableName, "");
            }
            JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysqlTableSql);
        }

    }

    /**
     * 生成拆分键变更DDL语句
     */
    @Override
    protected String createAlterPartitionKeySQL(String primaryTableName, PartitionParam p) {
        String partitionRule = buildPartitionRule(p);

        return MessageFormat
            .format(repartitionHint + "ALTER TABLE {0}.{1} {2}", getRepartitonBaseDB(), primaryTableName,
                partitionRule);
    }

    /**
     * 1. 校验表结构
     * 2. 校验分区规则
     */
    @Override
    protected void generalAssert(SqlCreateTable originAst, String apkDDL, String logicalTableName) throws SQLException {
//        try {
//            //因为原物理表是异步删除的，所以等待一下
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        SqlCreateTable newAst = showCreateTable(logicalTableName);

        SqlAlterTable ddl = (SqlAlterTable) PARSER.parse(apkDDL).get(0);

        List<Pair<SqlIdentifier, SqlIndexDefinition>> originGlobalKeys = originAst.getGlobalKeys();
        Assert.assertTrue(CollectionUtils.isEmpty(originGlobalKeys));

        List<Pair<SqlIdentifier, SqlIndexDefinition>> newGlobalKeys = newAst.getGlobalKeys();
        Assert.assertTrue(CollectionUtils.isEmpty(newGlobalKeys));

        //检查2个逻辑表的列是否全部相同
        Assert.assertTrue(identicalColumnDefList(originAst.getColDefs(), newAst.getColDefs()));

        //检查ddl的operands和gsi的index是否全部相同
        if (ddl instanceof SqlAlterTablePartitionKey) {
            assertPartitionKeyEquals((SqlAlterTablePartitionKey) ddl, newAst);
        } else {
            assert (ddl instanceof SqlAlterTableRepartition);
            assertSqlPartitionEquals((SqlAlterTableRepartition) ddl, newAst);
        }
    }

    /**
     * 获取table的所有拆分列
     */
    protected static void assertSqlPartitionEquals(SqlAlterTableRepartition ddl, SqlCreateTable newAst) {
        SqlNode partitionBy = ddl.getSqlPartition();

        if (partitionBy != null) {
            partitionBy.equalsDeep(newAst.getSqlPartition(), Litmus.THROW, EqualsContext.DEFAULT_EQUALS_CONTEXT);
        }
    }

    protected static final PartitionParam partitionOf(String operator, String func, String operand) {
        return new PartitionParam(operator, func, operand, -1);
    }

    protected static final PartitionParam partitionOf(String operator, String operand, int partCount) {
        return new PartitionParam(operator, null, operand, partCount);
    }

    protected static final PartitionParam partitionOf(String operator, String operand) {
        return new PartitionParam(operator, null, operand, -1);
    }

    protected static final PartitionParam partitionOf(String broadcastOrSingle) {
        return new PartitionParam(broadcastOrSingle);
    }
}
