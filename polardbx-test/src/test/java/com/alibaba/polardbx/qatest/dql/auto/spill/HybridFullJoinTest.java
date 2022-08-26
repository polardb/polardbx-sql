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

package com.alibaba.polardbx.qatest.dql.auto.spill;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.google.common.collect.Lists;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static com.alibaba.polardbx.qatest.dql.auto.spill.HybridFullJoinTest.JOIN_ALGORITHM.HASH;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@RunWith(CommonCaseRunner.class)
@FileStoreIgnore
@NotThreadSafe
public class HybridFullJoinTest extends AutoCrudBasedLockTestCase {

    private BinaryJoinUnit binaryJoinUnit;
    private ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameterized.Parameters(name = "{index}:join={0}")
    public static List<BinaryJoinUnit> prepareDate() throws Exception {
        String[][] tables = ExecuteTableName.joinTable();
        List<BinaryJoinUnit> binaryJoinUnits = buildBinaryJoinUnit(tables);
//        for (BinaryJoinUnit binaryJoinUnit : binaryJoinUnits) {
//            binaryJoinUnit.tableNameLeft =
//                createLikeTable(binaryJoinUnit.tableNameLeft, PropertiesUtil.polardbXAutoDBName1(),
//                    PropertiesUtil.mysqlDBName1(), randomTableName("HFJ", 4),
//                    true);
//            binaryJoinUnit.tableNameRight =
//                createLikeTable(binaryJoinUnit.tableNameRight, PropertiesUtil.polardbXAutoDBName1(),
//                    PropertiesUtil.mysqlDBName1(), randomTableName("HFJ", 4),
//                    true);
//        }
        return binaryJoinUnits;
    }

    @After
    public void afterData() throws Exception {
//        dropTable(binaryJoinUnit.tableNameLeft, PropertiesUtil.polardbXAutoDBName1(), PropertiesUtil.mysqlDBName1(), "",
//            true);
//        dropTable(binaryJoinUnit.tableNameRight, PropertiesUtil.polardbXAutoDBName1(), PropertiesUtil.mysqlDBName1(), "",
//            true);
    }

    public HybridFullJoinTest(BinaryJoinUnit binaryJoinUnit) {
        this.binaryJoinUnit = binaryJoinUnit;
        this.baseOneTableName = binaryJoinUnit.tableNameLeft;
        this.baseTwoTableName = binaryJoinUnit.tableNameRight;
    }

    enum DATA_TYPE {
        EMPTY, NULL, NORMAL, NORMAL_WITH_NULL;

        public void build(String tableName, HybridFullJoinTest joinTest) {
            joinTest.buildEmptyData(tableName);
            switch (this) {
            case EMPTY:
                joinTest.buildEmptyData(tableName);
                break;
            case NULL:
                joinTest.buildNullData(tableName);
                break;
            case NORMAL:
                joinTest.buildNormalData(tableName);
                break;
            case NORMAL_WITH_NULL:
                joinTest.buildNormalWithNullData(tableName);
                break;
            default:
                throw new NotSupportException(this.name());
            }
        }
    }

    enum OP_TYPE {

        LESS_THAN("<"), LESS_THAN_OR_EQUAL("<="), EQUAL("="), GREATER_THAN(">"), GREATER_THAN_OR_EQUAL(">="),
        LESS_THAN_ANY("<ANY"), LESS_THAN_OR_EQUAL_ANY("<=ANY"), EQUAL_ANY("=ANY"), GREATER_THAN_ANY(">ANY"),
        GREATER_THAN_OR_EQUAL_ANY(">=ANY"),
        LESS_THAN_ALL("<ALL"), LESS_THAN_OR_EQUAL_ALL("<=ALL"), EQUAL_ALL("=ALL"), GREATER_THAN_ALL(">ALL"),
        GREATER_THAN_OR_EQUAL_ALL(">=ALL");

        String name;

        OP_TYPE(String name) {
            this.name = name;
        }

        public boolean isAllOrAny() {
            return name.contains("ALL") || name.contains("ANY");
        }

        public boolean isAll() {
            return name.contains("ALL");
        }

        public boolean isAny() {
            return name.contains("ANY");
        }

        public String build() {
            return name;
        }
    }

    enum JOIN_TYPE {
        INNER(HASH) {
            @Override
            public List<String> buildSQLforBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable, String rightTable,
                                                      OP_TYPE op) {
                List<String> rs = Lists.newLinkedList();
                StringBuilder stringBuilder = new StringBuilder();
                List<String> hints = joinAlgorithm.buildHint(leftTable, rightTable);
                for (String hint : hints) {
                    stringBuilder.setLength(0);
                    switch (joinAlgorithm) {
                    case HASH:
                        stringBuilder.append(hint)
                            .append("SELECT * FROM ")
                            .append(leftTable)
                            .append(" a")
                            .append(" JOIN ")
                            .append(rightTable)
                            .append(" b")
                            .append(" ON a.integer_test")
                            .append("=")
                            .append("b.integer_test");
                        rs.add(stringBuilder.toString());
                        break;
                    default:
                        throw new NotSupportException(this.name() + ":" + joinAlgorithm);
                    }
                }
                return rs;
            }
        }, LEFT(HASH) {
            @Override
            public List<String> buildSQLforBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable, String rightTable,
                                                      OP_TYPE op) {
                List<String> rs = Lists.newLinkedList();
                StringBuilder stringBuilder = new StringBuilder();
                List<String> hints = joinAlgorithm.buildHint(leftTable, rightTable);
                for (String hint : hints) {
                    stringBuilder.setLength(0);
                    switch (joinAlgorithm) {
                    case HASH:
                        stringBuilder.append(hint)
                            .append("SELECT * FROM ")
                            .append(leftTable)
                            .append(" a")
                            .append(" LEFT JOIN ")
                            .append(rightTable)
                            .append(" b")
                            .append(" ON a.integer_test")
                            .append("=")
                            .append("b.integer_test");
                        rs.add(stringBuilder.toString());
                        break;
                    default:
                        throw new NotSupportException(this.name() + ":" + joinAlgorithm);
                    }
                }
                return rs;
            }
        }, RIGHT(HASH) {
            @Override
            public List<String> buildSQLforBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable, String rightTable,
                                                      OP_TYPE op) {
                List<String> rs = Lists.newLinkedList();
                StringBuilder stringBuilder = new StringBuilder();
                List<String> hints = joinAlgorithm.buildHint(leftTable, rightTable);
                for (String hint : hints) {
                    stringBuilder.setLength(0);
                    switch (joinAlgorithm) {
                    case HASH:
                        stringBuilder.append(hint)
                            .append("SELECT * FROM ")
                            .append(leftTable)
                            .append(" a")
                            .append(" RIGHT JOIN ")
                            .append(rightTable)
                            .append(" b")
                            .append(" ON a.integer_test")
                            .append("=")
                            .append("b.integer_test");
                        rs.add(stringBuilder.toString());
                        break;
                    default:
                        throw new NotSupportException(this.name() + ":" + joinAlgorithm);
                    }
                }
                return rs;
            }
        };

        List<JOIN_ALGORITHM> supportAlgorithm = Lists.newLinkedList();

        JOIN_TYPE(JOIN_ALGORITHM... joinAlgorithms) {
            supportAlgorithm.addAll(Arrays.asList(joinAlgorithms));
        }

        public abstract List<String> buildSQLforBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable,
                                                           String rightTable, OP_TYPE op);

        public Collection<? extends BinaryJoinUnit> buildBinaryJoinUnit(BinaryJoinUnit binaryJoinUnit) {
            binaryJoinUnit.joinType = this;
            List<BinaryJoinUnit> rs = Lists.newLinkedList();
            for (JOIN_ALGORITHM algorithm : this.supportAlgorithm) {
                BinaryJoinUnit temp = binaryJoinUnit.copy();
                temp.joinAlgorithm = algorithm;
                if (!algorithm.isNL()) {
                    temp.opType = OP_TYPE.EQUAL;
                    rs.add(temp);
                } else {
                    for (OP_TYPE op : OP_TYPE.values()) {
                        if (!algorithm.isSubquery() && op.isAllOrAny()) {
                            continue;
                        }
                        if (algorithm.isSemi() && op.isAll()) {
                            continue;
                        }
                        if (algorithm.isAnti() && op.isAny()) {
                            continue;
                        }
                        rs.add(temp.copy(op));
                    }
                }
            }
            return rs;
        }
    }

    enum JOIN_ALGORITHM {
        NL("NL_JOIN", "NlJoin"), HASH("HASH_JOIN", "HashJoin"), SORT_MERGE("SORT_MERGE_JOIN", "SortMergeJoin"),
        BKA("BKA_JOIN", "BKAJoin"), MATERIALIZED_SEMI(
            "MATERIALIZED_SEMI_JOIN", "MaterializedSemiJoin"), SEMI_HASH("SEMI_HASH_JOIN", "SemiHashJoin"),
        ANTI_HASH("SEMI_HASH_JOIN", "SemiHashJoin"), SEMI_NL(
            "SEMI_NL_JOIN", "SemiNLJoin"), ANTI_NL("SEMI_NL_JOIN", "SemiNLJoin");

        String hintName;
        String planName;

        JOIN_ALGORITHM(String hintName, String planName) {
            this.hintName = hintName;
            this.planName = planName;
        }

        public List<String> buildHint(String leftTableName, String rightTableName) {
            List<String> hints = Lists.newLinkedList();
            hints.add("/*+TDDL: cmd_extra(enable_push_join=false enable_cbo_push_join=false ENABLE_CBO=true "
                + "ENABLE_SEMI_HASH_JOIN=true PARALLELISM=0 ENABLE_SPILL=true) "
                + this.hintName + "(" + leftTableName + "," + rightTableName + ") */ ");
            hints.add(
                "/*+TDDL: cmd_extra(enable_push_join=false enable_cbo_push_join=false ENABLE_CBO=true "
                    + "ENABLE_SEMI_HASH_JOIN=true ENABLE_SPILL=true) " + this.hintName
                    + "(" + leftTableName + "," + rightTableName + ") */ ");
            return hints;
        }

        public boolean isHash() {
            return this == HASH || this == SEMI_HASH || this == ANTI_HASH;
        }

        public boolean isNL() {
            return this == NL || this == SEMI_NL || this == ANTI_NL;
        }

        public boolean isSubquery() {
            return hintName.contains("SEMI") || hintName.contains("ANTI");
        }

        public boolean isSemi() {
            return hintName.contains("SEMI");
        }

        public boolean isAnti() {
            return hintName.contains("ANTI");
        }
    }

    static class BinaryJoinUnit {
        DATA_TYPE dataTypeLeft;
        DATA_TYPE dataTypeRight;
        JOIN_TYPE joinType;
        JOIN_ALGORITHM joinAlgorithm;
        OP_TYPE opType;
        String tableNameLeft;
        String tableNameRight;

        void buildTest(HybridFullJoinTest joinTest) {
            /**
             * get data ready
             */
            dataTypeLeft.build(tableNameLeft, joinTest);
            dataTypeRight.build(tableNameRight, joinTest);

            List<String> sqls = joinType.buildSQLforBinaryJoin(joinAlgorithm, tableNameLeft, tableNameRight, opType);
            for (String sql : sqls) {
                /**
                 * check explain
                 */
                explainAllResultMatchAssert("EXPLAIN " + sql,
                    null,
                    joinTest.tddlConnection,
                    "[\\s\\S]*" + joinAlgorithm.planName + "[\\s\\S]*");

                /**
                 * check result
                 */
                selectContentSameAssert(sql, null, joinTest.mysqlConnection, joinTest.tddlConnection, true);
            }
        }

        public BinaryJoinUnit copy() {
            BinaryJoinUnit binaryJoinUnit = new BinaryJoinUnit();
            binaryJoinUnit.tableNameLeft = tableNameLeft;
            binaryJoinUnit.tableNameRight = tableNameRight;
            binaryJoinUnit.dataTypeLeft = dataTypeLeft;
            binaryJoinUnit.dataTypeRight = dataTypeRight;
            binaryJoinUnit.joinType = joinType;
            binaryJoinUnit.joinAlgorithm = joinAlgorithm;
            binaryJoinUnit.opType = opType;
            return binaryJoinUnit;
        }

        public BinaryJoinUnit copy(DATA_TYPE left, DATA_TYPE right) {
            BinaryJoinUnit binaryJoinUnit = copy();
            binaryJoinUnit.dataTypeLeft = left;
            binaryJoinUnit.dataTypeRight = right;
            return binaryJoinUnit;
        }

        public BinaryJoinUnit copy(OP_TYPE op) {
            BinaryJoinUnit binaryJoinUnit = copy();
            binaryJoinUnit.opType = op;
            return binaryJoinUnit;
        }

        @Override
        public String toString() {
            return dataTypeLeft.name() + "," + dataTypeRight.name() + ","
                + joinType + "," + joinAlgorithm + "," + opType;
        }
    }

    @Test
    public void fullJoinBinaryOperatorsTest() {
        binaryJoinUnit.buildTest(this);
    }

    public static List<BinaryJoinUnit> buildBinaryJoinUnit(String[][] tables) {
        List<BinaryJoinUnit> toTest = Lists.newLinkedList();
        for (String[] table : tables) {
            BinaryJoinUnit binaryJoinUnit = new BinaryJoinUnit();
            binaryJoinUnit.tableNameLeft = table[0];
            binaryJoinUnit.tableNameRight = table[1];

            for (DATA_TYPE left : DATA_TYPE.values()) {
                for (DATA_TYPE right : DATA_TYPE.values()) {
                    for (JOIN_TYPE joinType : JOIN_TYPE.values()) {
                        toTest.addAll(joinType.buildBinaryJoinUnit(binaryJoinUnit.copy(left,
                            right)));//op type random handle inside.
                    }
                }
            }
        }
        return toTest;
    }

    private void buildEmptyData(String tableName) {
        executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            "delete from " + tableName,
            Lists.newArrayList());
    }

    private void buildNullData(String tableName) {
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, tableName);
        List<Object> params = buildNullParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

    }

    private void buildNormalData(String tableName) {
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, tableName);
        List<Object> params = buildNormalParams(columns);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql, params);

    }

    private void buildNormalWithNullData(String tableName) {
        buildNullData(tableName);
        buildNormalData(tableName);
    }

    private List<Object> buildNullParams(List<ColumnEntity> columns) {
        Random r = new Random();
        List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, Math.abs(r.nextInt()));
        param.set(1, null);
        return param;
    }

    private List<Object> buildNormalParams(List<ColumnEntity> columns) {
        Random r = new Random();
        List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, Math.abs(r.nextInt()));
        return param;
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
