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

package com.alibaba.polardbx.qatest.dql.sharding.explain;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;

/**
 * test explain execute xxx
 *
 * @author roy
 * @since 5.3.8
 */

public class ExplainExecuteTest extends CrudBasedLockTestCase {

    private static final Log log = LogFactory.getLog(ExplainExecuteTest.class);
    private ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectOneTableMultiRuleMode());
    }

    public ExplainExecuteTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainSelectTest() {
        String sql = "explain execute select * from " + baseOneTableName;
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainSelectWithPartitionFilterTest() {
        String sql = "explain execute select * from " + baseOneTableName + " where pk=1";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainUpdateTest() {
        String sql = "explain execute update " + baseOneTableName + " set varchar_test='a'";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainUpdateWithPartitionFilterTest() {
        String sql = "explain execute update " + baseOneTableName + " set varchar_test='a' where pk=1";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDeleteTest() {
        String sql = "explain execute delete " + baseOneTableName;
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDeleteWithPartitionFilterTest() {
        String sql = "explain execute delete " + baseOneTableName + " where varchar_test='a'";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainInsertTest() {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "explain execute insert into " + baseOneTableName + " (";
        String values = " values ( ";

        for (int j = 0; j < columns.size(); j++) {
            String columnName = columns.get(j).getName();
            sql = sql + columnName + ",";
            values = values + " ?,";
        }

        sql = sql.substring(0, sql.length() - 1) + ") ";
        values = values.substring(0, values.length() - 1) + ")";

        sql = sql + values;
        List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, 1);

        executeErrorAssert(tddlConnection, sql, param, "not support");
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDDLTest() {
        String sql = "explain execute CREATE TABLE `REGION` (\n" + "  `R_REGIONKEY` decimal(11,0) NOT NULL,\n"
            + "  `R_NAME` char(25) DEFAULT NULL,\n" + "  `R_COMMENT` varchar(152) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`R_REGIONKEY`)\n" + ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

        executeErrorAssert(tddlConnection, sql, null, "not support");
    }
}
