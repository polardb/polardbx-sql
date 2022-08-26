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

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_ONE_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

@Ignore

public class InventorHintUpdateTest extends CrudBasedLockTestCase {
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    @Parameters(name = "{index}:{0},{1},{2}")
    public static List<Object[]> getParameters() {
        return cartesianProduct(
            transPolicy(),
            inventorHint(),
            table());
    }

    public static List<Object[]> cartesianProduct(Object[]... arrays) {
        List[] lists = Arrays.stream(arrays)
            .map(Arrays::asList)
            .toArray(List[]::new);
        List<List<Object>> result = Lists.cartesianProduct(lists);
        return result.stream()
            .map(List::toArray)
            .collect(Collectors.toList());
    }

    public static Object[] table() {
        return new String[] {
            ExecuteTableName.UPDATE_DELETE_BASE + ONE_DB_ONE_TB_SUFFIX,
            ExecuteTableName.UPDATE_DELETE_BASE + ONE_DB_MUTIL_TB_SUFFIX,
            ExecuteTableName.UPDATE_DELETE_BASE + MUlTI_DB_MUTIL_TB_SUFFIX
        };
    }

    public static Object[] transPolicy() {
        return new Object[] {
            ITransactionPolicy.XA,
            // ITransactionPolicy.BEST_EFFORT,
            ITransactionPolicy.ALLOW_READ_CROSS_DB,
            ITransactionPolicy.TSO
        };
    }

    public static Object[] inventorHint() {
        return new String[] {
            "/*+ COMMIT_ON_SUCCESS */",
            "/*+ ROLLBACK_ON_FAIL */",
            "/*+ TARGET_AFFECT_ROW(10) */",
            "/*+ COMMIT_ON_SUCCESS  ROLLBACK_ON_FAIL */",
            "/*+ COMMIT_ON_SUCCESS TARGET_AFFECT_ROW(10) */",
            "/*+ COMMIT_ON_SUCCESS ROLLBACK_ON_FAIL TARGET_AFFECT_ROW(10) */",
            "/*+ ROLLBACK_ON_FAIL TARGET_AFFECT_ROW(10) */"
        };
    }

    private final ITransactionPolicy transPolicy;
    private final String inventorHint;

    public InventorHintUpdateTest(Object transPolicy, Object inventorHint, Object tableName) {
        this.transPolicy = (ITransactionPolicy) transPolicy;
        this.inventorHint = inventorHint.toString();
        this.baseOneTableName = tableName.toString();
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void updateTestForInventorHint() throws Exception {
        String sql = String.format(
            "update %s %s set integer_test = -1 where pk = 1", inventorHint, baseOneTableName);

        String selectSql = String.format(
            "select integer_test from %s where pk = 1", baseOneTableName);

        String showTrace = String.format("show trace");

        tddlConnection.setAutoCommit(false);
        JdbcUtil.setTxPolicy(transPolicy, tddlConnection);
        if (transPolicy == ITransactionPolicy.XA) {
            // 共享readview不支持inventory hint
            // 当前默认关闭共享readview特性
//            JdbcUtil.setShareReadView(false, tddlConnection);
        }
        if (inventorHint.contains(HintType.INVENTORY_TARGET_AFFECT_ROW.getValue())) {

            String failed = null;
            //execute
            String sql1 = sql;
            Statement statement1 = tddlConnection.createStatement();
            try {
                statement1.execute(sql1);
                throw new RuntimeException("Don't expect here!");
            } catch (Throwable t) {
                if (transPolicy == ITransactionPolicy.TSO) {
                    Assert.assertTrue(t.getMessage()
                        .contains("Don't support the Inventory Hint on current Transaction Policy"));
                } else {
                    Assert.assertTrue(t.getMessage()
                        .contains("The affected row number does not match that of user specified"));
                }
                failed = t.getMessage();
            }
            statement1.close();

            if (inventorHint.contains(HintType.INVENTORY_ROLLBACK_ON_FAIL.getValue())) {
                Statement statement4 = tddlConnection.createStatement();
                ResultSet rs4 = statement4.executeQuery("show trans");
                while (rs4.next()) {
                    String traceId = rs4.getString(1);
                    if (traceId != null && failed != null) {
                        Assert.assertFalse(failed.contains(traceId));
                    }
                }
                rs4.close();
                statement4.close();
            } else {
                //rollback
                Statement statement3 = tddlConnection.createStatement();
                statement3.executeQuery("rollback");
                statement3.close();
            }
        } else {
            //execute
            String sql1 = "trace " + sql;
            Statement statement1 = tddlConnection.createStatement();
            try {

                statement1.execute(sql1);
                statement1.close();
            } catch (Throwable t) {
                if (transPolicy == ITransactionPolicy.TSO) {
                    Assert.assertTrue(t.getMessage()
                        .contains("Don't support the Inventory Hint on current Transaction Policy"));
                    statement1.close();
                    if (inventorHint.contains(HintType.INVENTORY_ROLLBACK_ON_FAIL.getValue())) {
                        Statement statement4 = tddlConnection.createStatement();
                        ResultSet rs4 = statement4.executeQuery("show trans");
                        while (rs4.next()) {
                            String traceId = rs4.getString(1);
                            if (traceId != null) {
                                Assert.assertFalse(t.getMessage().contains(traceId));
                            }
                        }
                        rs4.close();
                        statement4.close();
                    } else {
                        //rollback
                        Statement statement3 = tddlConnection.createStatement();
                        statement3.executeQuery("rollback");
                        statement3.close();
                    }
                    return;
                } else {
                    throw t;
                }
            }

            Statement statement2 = tddlConnection.createStatement();
            ResultSet rs2 = statement2.executeQuery(showTrace);
            int traceCount = 0;
            while (rs2.next()) {
                traceCount++;
                Assert.assertTrue(rs2.getString("STATEMENT").contains(inventorHint));
            }
            statement2.close();
            Preconditions.checkArgument(
                traceCount == 1, "Inventory hint is not allowed when the transaction involves more than one group!");

            //rollback
            Statement statement3 = tddlConnection.createStatement();
            statement3.executeQuery("rollback");
            statement3.close();

            //verify
            if (inventorHint.contains(HintType.INVENTORY_COMMIT_ON_SUCCESS.getValue())) {
                Statement statement4 = tddlConnection.createStatement();
                ResultSet rs4 = statement4.executeQuery(selectSql);
                while (rs4.next()) {
                    Preconditions.checkArgument(
                        rs4.getLong(1) == -1, "update failed!");
                }
                statement4.close();
            } else {
                Statement statement4 = tddlConnection.createStatement();
                ResultSet rs4 = statement4.executeQuery(selectSql);
                while (rs4.next()) {
                    Preconditions.checkArgument(
                        rs4.getLong(1) != -1, "update failed!");
                }
                statement4.close();
            }
        }
    }

    @Test
    public void updateWithCommitHintOnShardingTable2() throws Exception {
        if (baseOneTableName.contains(ONE_DB_ONE_TB_SUFFIX)) {
            return;
        }

        String sql = String.format(
            "update %s %s set integer_test = -1", inventorHint, baseOneTableName);

        tddlConnection.setAutoCommit(false);
        JdbcUtil.setTxPolicy(transPolicy, tddlConnection);
        JdbcUtil.setTxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB, tddlConnection);

        String failed = null;
        //execute
        String sql1 = sql;
        Statement statement1 = tddlConnection.createStatement();
        try {
            statement1.execute(sql1);
            throw new RuntimeException("Don't expect here!");
        } catch (Throwable t) {
            Assert.assertTrue(t.getMessage()
                .contains("ERR_IVENTORY_HINT_NOT_SUPPORT_CROSS_SHARD"));
            failed = t.getMessage();
        }
        statement1.close();

        if (inventorHint.contains(HintType.INVENTORY_ROLLBACK_ON_FAIL.getValue())) {
            Statement statement4 = tddlConnection.createStatement();
            ResultSet rs4 = statement4.executeQuery("show trans");
            while (rs4.next()) {
                String traceId = rs4.getString(1);
                if (traceId != null && failed != null) {
                    Assert.assertFalse(failed.contains(traceId));
                }
            }
            rs4.close();
            statement4.close();
        } else {
            //rollback
            Statement statement3 = tddlConnection.createStatement();
            statement3.executeQuery("rollback");
            statement3.close();
        }
    }

}

