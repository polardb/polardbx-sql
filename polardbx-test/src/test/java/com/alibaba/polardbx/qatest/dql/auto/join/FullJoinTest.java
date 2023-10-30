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

package com.alibaba.polardbx.qatest.dql.auto.join;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.ANTI_HASH;
import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.ANTI_NL;
import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.BKA;
import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.HASH;
import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.HASH_OUTER;
import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.MATERIALIZED_SEMI;
import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.NL;
import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.SEMI_BKA;
import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.SEMI_HASH;
import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.SEMI_NL;
import static com.alibaba.polardbx.qatest.dql.auto.join.FullJoinTest.JOIN_ALGORITHM.SORT_MERGE;
import static com.alibaba.polardbx.qatest.util.JdbcUtil.createLikeTable;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXAutoDBName1;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author fangwu
 */

public class FullJoinTest extends AutoCrudBasedLockTestCase {

    BinaryJoinUnit binaryJoinUnit;

    @Parameterized.Parameters(name = "{index}:join={0}")
    public static List<BinaryJoinUnit> prepareDate() throws Exception {
        String[][] tables = ExecuteTableName.newJoinTable();
        String[][] newTables = new String[tables.length][];
        int i = 0;
        for (String[] tableNames : tables) {
            String[] newTableNames = new String[tableNames.length];
            int j = 0;
            for (String table : tableNames) {
                String newTable =
                    createLikeTable(table, polardbXAutoDBName1(), PropertiesUtil.mysqlDBName1(), "FJT",
                        true);
                newTableNames[j] = newTable;
                try (
                    Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
                    Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
                    JdbcUtil.useDb(tddlConnection, polardbXAutoDBName1());
                    JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
                    readyTheData(mysqlConnection, tddlConnection, newTable);
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
                j++;
            }
            newTables[i] = newTableNames;
            i++;

        }
        return buildBinaryJoinUnit(newTables);
    }

    public FullJoinTest(BinaryJoinUnit binaryJoinUnit) {
        this.binaryJoinUnit = binaryJoinUnit;
        this.baseOneTableName = binaryJoinUnit.tableNameLeft;
        this.baseTwoTableName = binaryJoinUnit.tableNameRight;
    }

    enum JOIN_COL_TYPE {

        COL_INTEGER("integer_test"),
        COL_VARCHAR("varchar_test"),
        COL_TIME("timestamp_test");

        private String colName;

        JOIN_COL_TYPE(String colName) {
            this.colName = colName;
        }

        public String getColName() {
            return colName;
        }
    }

    enum OP_TYPE {

        LESS_THAN("<"),
        //        LESS_THAN_OR_EQUAL("<="),
        EQUAL("="),
        NOT_EQUAL("!="),
        //        GREATER_THAN(">"), GREATER_THAN_OR_EQUAL(">="),
//        LESS_THAN_ANY("<ANY"), LESS_THAN_OR_EQUAL_ANY("<=ANY"),
        EQUAL_ANY("=ANY"),
        GREATER_THAN_ANY(">ANY"),
        NOT_EQUAL_ANY("!=ANY"),
        //        GREATER_THAN_OR_EQUAL_ANY(">=ANY"),
        LESS_THAN_ALL("<ALL"),
        //        LESS_THAN_OR_EQUAL_ALL("<=ALL"),
        EQUAL_ALL("=ALL"),
        //        GREATER_THAN_ALL(">ALL"),
        GREATER_THAN_OR_EQUAL_ALL(">=ALL"),
        NOT_EQUAL_ALL("!=ALL");

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

        public boolean isComparsion() {
            return this == LESS_THAN || this == EQUAL || this == NOT_EQUAL;
        }

        public boolean isIN() {
            return this == EQUAL_ANY || this == NOT_EQUAL_ALL;
        }

        public String build() {
            return name;
        }
    }

    enum JOIN_TYPE {
        INNER(NL, HASH, SORT_MERGE, BKA) {
            @Override
            public List<String> buildSQLForBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable, String rightTable,
                                                      OP_TYPE op,
                                                      List<JOIN_COL_TYPE> join_col_types) {
                List<String> rs = Lists.newLinkedList();
                StringBuilder stringBuilder = new StringBuilder();
                List<String> hints = joinAlgorithm.buildHint(leftTable, rightTable);
                for (String hint : hints) {
                    stringBuilder.setLength(0);
                    switch (joinAlgorithm) {
                    case HASH:
                    case NL:
                    case SORT_MERGE:
                    case BKA:
                        stringBuilder.append(hint)
                            .append("SELECT * FROM ")
                            .append(leftTable)
                            .append(" a")
                            .append(" JOIN ")
                            .append(rightTable)
                            .append(" b")
                            .append(" ON ")
                            .append(buildJoinCondition(join_col_types, op));
                        rs.add(stringBuilder.toString());
                        break;

                    default:
                        throw new NotSupportException(this.name() + ":" + joinAlgorithm);
                    }
                }
                return rs;
            }
        }, LEFT(NL, HASH, HASH_OUTER, SORT_MERGE, BKA) {
            @Override
            public List<String> buildSQLForBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable, String rightTable,
                                                      OP_TYPE op,
                                                      List<JOIN_COL_TYPE> join_col_types) {
                List<String> rs = Lists.newLinkedList();
                StringBuilder stringBuilder = new StringBuilder();
                List<String> hints = joinAlgorithm.buildHint(leftTable, rightTable);
                for (String hint : hints) {
                    stringBuilder.setLength(0);
                    switch (joinAlgorithm) {
                    case HASH:
                    case HASH_OUTER:
                    case NL:
                    case SORT_MERGE:
                    case BKA:
                        stringBuilder.append(hint)
                            .append("SELECT * FROM ")
                            .append(leftTable)
                            .append(" a")
                            .append(" LEFT JOIN ")
                            .append(rightTable)
                            .append(" b")
                            .append(" ON ")
                            .append(buildJoinCondition(join_col_types, op));
                        rs.add(stringBuilder.toString());
                        break;
                    default:
                        throw new NotSupportException(this.name() + ":" + joinAlgorithm);
                    }
                }
                return rs;
            }
        }, RIGHT(NL, HASH, HASH_OUTER, SORT_MERGE, BKA) {
            @Override
            public List<String> buildSQLForBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable, String rightTable,
                                                      OP_TYPE op,
                                                      List<JOIN_COL_TYPE> join_col_types) {
                List<String> rs = Lists.newLinkedList();
                StringBuilder stringBuilder = new StringBuilder();
                List<String> hints = joinAlgorithm.buildHint(leftTable, rightTable);
                for (String hint : hints) {
                    stringBuilder.setLength(0);
                    switch (joinAlgorithm) {
                    case HASH:
                    case HASH_OUTER:
                    case NL:
                    case SORT_MERGE:
                    case BKA:
                        stringBuilder.append(hint)
                            .append("SELECT * FROM ")
                            .append(leftTable)
                            .append(" a")
                            .append(" RIGHT JOIN ")
                            .append(rightTable)
                            .append(" b")
                            .append(" ON ")
                            .append(buildJoinCondition(join_col_types, op));
                        rs.add(stringBuilder.toString());
                        break;
                    default:
                        throw new NotSupportException(this.name() + ":" + joinAlgorithm);
                    }
                }
                return rs;
            }
        }, SEMI(SEMI_HASH, SEMI_BKA, MATERIALIZED_SEMI, SEMI_NL) {
            @Override
            public List<String> buildSQLForBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable, String rightTable,
                                                      OP_TYPE op,
                                                      List<JOIN_COL_TYPE> join_col_types) {
                List<String> rs = Lists.newLinkedList();
                StringBuilder stringBuilder = new StringBuilder();
                List<String> hints = joinAlgorithm.buildHint(leftTable, rightTable);
                for (String hint : hints) {
                    stringBuilder.setLength(0);
                    switch (joinAlgorithm) {
                    case SEMI_HASH:
                    case MATERIALIZED_SEMI:
                    case SEMI_BKA:
                        /**
                         * exists
                         */
//                        stringBuilder.append(hint)
//                            .append("SELECT * FROM ")
//                            .append(leftTable)
//                            .append(" a")
//                            .append(" WHERE EXISTS (SELECT 1 FROM ")
//                            .append(rightTable)
//                            .append(" b")
//                            .append(" WHERE ")
//                            .append(buildJoinCondition(join_col_types, op))
//                            .append(")");
//                        rs.add(stringBuilder.toString());

                        /**
                         * in
                         */
                        stringBuilder.setLength(0);
                        stringBuilder.append(hint)
                            .append("SELECT a.pk, a.varchar_test FROM ")
                            .append(leftTable)
                            .append(" a")
                            .append(" WHERE ")
                            .append(buildSubquery(join_col_types, op))
                            .append(rightTable)
                            .append(" b")
                            .append(")");
                        rs.add(stringBuilder.toString());
                        break;
                    case SEMI_NL:
                        if (op.isAny()) {
                            if (join_col_types.size() > 2 && !op.isIN()) {
                                break;
                            }
                            stringBuilder.append(hint)
                                .append("SELECT  a.pk, a.varchar_test  FROM ")
                                .append(leftTable)
                                .append(" a")
                                .append(" WHERE ")
                                .append(buildSubquery(join_col_types, op))
                                .append(rightTable)
                                .append(" b")
                                .append(")");
                            rs.add(stringBuilder.toString());
                        } else {
                            /**
                             * exists
                             */
                            stringBuilder.append(hint)
                                .append("SELECT * FROM ")
                                .append(leftTable)
                                .append(" a")
                                .append(" WHERE EXISTS (SELECT 1 FROM ")
                                .append(rightTable)
                                .append(" b")
                                .append(" WHERE ")
                                .append(buildJoinCondition(join_col_types, op))
                                .append(")");
                            rs.add(stringBuilder.toString());

                            /**
                             * in
                             */
                            stringBuilder.setLength(0);
                            stringBuilder.append(hint)
                                .append("SELECT * FROM ")
                                .append(leftTable)
                                .append(" a")
                                .append(" WHERE ")
                                .append(buildSubquery(join_col_types, op))
                                .append(rightTable)
                                .append(" b")
                                .append(")");
                            rs.add(stringBuilder.toString());
                        }

                        break;
                    default:
                        throw new NotSupportException(this.name() + ":" + joinAlgorithm);
                    }
                }
                return rs;
            }
        }, ANTI(ANTI_HASH, ANTI_NL, MATERIALIZED_SEMI) {
            @Override
            public List<String> buildSQLForBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable, String rightTable,
                                                      OP_TYPE op,
                                                      List<JOIN_COL_TYPE> join_col_types) {
                List<String> rs = Lists.newLinkedList();
                StringBuilder stringBuilder = new StringBuilder();
                List<String> hints = joinAlgorithm.buildHint(leftTable, rightTable);
                for (String hint : hints) {
                    stringBuilder.setLength(0);
                    switch (joinAlgorithm) {
                    case ANTI_HASH:
                        if (op != OP_TYPE.EQUAL) {
                            break;
                        }
                        /**
                         * not exists
                         */
                        stringBuilder.append(hint)
                            .append("SELECT * FROM ")
                            .append(leftTable)
                            .append(" a")
                            .append(" WHERE NOT EXISTS (SELECT 1 FROM ")
                            .append(rightTable)
                            .append(" b")
                            .append(" WHERE ")
                            .append(buildJoinCondition(join_col_types, op))
                            .append(")");
                        rs.add(stringBuilder.toString());
                        /**
                         * not in
                         */
                        //                        stringBuilder.setLength(0);
                        //                        stringBuilder.append(hint)
                        //                                .append("SELECT * FROM ")
                        //                                .append(leftTable)
                        //                                .append(" a")
                        //                                .append(" WHERE integer_test not in (SELECT integer_test FROM ")
                        //                                .append(rightTable)
                        //                                .append(" )");
                        //                        rs.add(stringBuilder.toString());
                        break;
                    case ANTI_NL:
                        if (op.isAllOrAny()) {
                            if (join_col_types.size() > 2 && !op.isIN()) {
                                break;
                            }
                            stringBuilder.append(hint)
                                .append("SELECT * FROM ")
                                .append(leftTable)
                                .append(" a")
                                .append(" WHERE ")
                                .append(buildSubquery(join_col_types, op))
                                .append(rightTable)
                                .append(" b)");
                            rs.add(stringBuilder.toString());
                        } else {
                            /**
                             * not exists
                             */
                            stringBuilder.append(hint)
                                .append("SELECT * FROM ")
                                .append(leftTable)
                                .append(" a")
                                .append(" WHERE NOT EXISTS (SELECT 1 FROM ")
                                .append(rightTable)
                                .append(" b")
                                .append(" WHERE ")
                                .append(buildJoinCondition(join_col_types, op))
                                .append(")");
                            rs.add(stringBuilder.toString());
                            /**
                             * not in
                             */
                            stringBuilder.setLength(0);
                            stringBuilder.append(hint)
                                .append("SELECT * FROM ")
                                .append(leftTable)
                                .append(" a")
                                .append(" WHERE ")
                                .append(buildSubquery(join_col_types, op))
                                .append(rightTable)
                                .append(" )");
                            rs.add(stringBuilder.toString());
                        }

                        break;
                    case MATERIALIZED_SEMI:
                        /**
                         * not in
                         */
                        stringBuilder.setLength(0);
                        stringBuilder.append(hint)
                            .append("SELECT * FROM ")
                            .append(leftTable)
                            .append(" a")
                            .append(" WHERE ")
                            .append(buildSubquery(join_col_types, op))
                            .append(rightTable)
                            .append(" )");
                        rs.add(stringBuilder.toString());
                        break;
                    default:
                        throw new NotSupportException(this.name() + ":" + joinAlgorithm);
                    }
                }
                return rs;
            }
        }, LEFT_SEMI(SEMI_NL, SEMI_HASH, SEMI_BKA) {
            @Override
            public List<String> buildSQLForBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable, String rightTable,
                                                      OP_TYPE op,
                                                      List<JOIN_COL_TYPE> join_col_types) {
                List<String> rs = Lists.newLinkedList();
                StringBuilder stringBuilder = new StringBuilder();
                List<String> hints = joinAlgorithm.buildHint(leftTable, rightTable);
                for (String hint : hints) {
                    stringBuilder.setLength(0);
                    switch (joinAlgorithm) {
                    case SEMI_HASH:
                        if (op.isAny()) {
                            break;
                        }
                        stringBuilder.append(hint)
                            .append("SELECT *,")
                            .append("(SELECT 1 FROM ")
                            .append(rightTable)
                            .append(" WHERE ")
                            .append(buildJoinCondition(join_col_types, op))
                            .append(" ) ")
                            .append(" FROM ")
                            .append(leftTable)
                            .append(" a");
                        rs.add(stringBuilder.toString());
                        break;
                    case SEMI_BKA:
                        if (op.isAny()) {
                            break;
                        }
                        stringBuilder.append(hint)
                            .append("SELECT *,")
                            .append("(SELECT 1 FROM ")
                            .append(rightTable)
                            .append(" WHERE ")
                            .append(buildJoinCondition(join_col_types, op))
                            .append(" ) ")
                            .append(" FROM ")
                            .append(leftTable)
                            .append(" a");
                        rs.add(stringBuilder.toString());
                        break;
                    case SEMI_NL:
                        if (op.isAny()) {
                            stringBuilder.append(hint)
                                .append("SELECT *,integer_test")
                                .append(op.build())
                                .append("(SELECT integer_test FROM ")
                                .append(rightTable)
                                .append(")")
                                .append(" FROM ")
                                .append(leftTable)
                                .append(" a");
                        } else {
                            stringBuilder.append(hint)
                                .append("SELECT *,")
                                .append("(SELECT 1 FROM ")
                                .append(rightTable)
                                .append(" WHERE ")
                                .append(buildJoinCondition(join_col_types, op))
                                .append(" ) ")
                                .append(" FROM ")
                                .append(leftTable)
                                .append(" a");
                        }

                        rs.add(stringBuilder.toString());
                        break;
                    default:
                        throw new NotSupportException(this.name() + ":" + joinAlgorithm);
                    }
                }
                return rs;
            }
        }, INNER_SEMI(SEMI_NL, SEMI_HASH, SEMI_BKA) {
            @Override
            public List<String> buildSQLForBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable, String rightTable,
                                                      OP_TYPE op,
                                                      List<JOIN_COL_TYPE> join_col_types) {
                List<String> rs = Lists.newLinkedList();
                StringBuilder stringBuilder = new StringBuilder();
                List<String> hints = joinAlgorithm.buildHint(leftTable, rightTable);
                for (String hint : hints) {
                    stringBuilder.setLength(0);
                    switch (joinAlgorithm) {
                    case SEMI_HASH:
                        stringBuilder.append(hint)
                            .append("SELECT * FROM ")
                            .append(leftTable)
                            .append(" a WHERE ")
                            .append(buildSubquery(join_col_types, op))
                            .append(rightTable)
                            .append(" )");
                        rs.add(stringBuilder.toString());
                        break;
                    case SEMI_BKA:
                        stringBuilder.append(hint)
                            .append("SELECT * FROM ")
                            .append(leftTable)
                            .append(" a WHERE ")
                            .append(buildSubquery(join_col_types, op))
                            .append(rightTable)
                            .append(" )");
                        rs.add(stringBuilder.toString());
                        break;
                    case SEMI_NL:
                        if (op.isAny()) {
                            if (join_col_types.size() > 2 && !op.isIN()) {
                                break;
                            }
                            stringBuilder.append(hint)
                                .append("SELECT * FROM ")
                                .append(leftTable)
                                .append(" a WHERE ")
                                .append(buildSubquery(join_col_types, op))
                                .append(rightTable)
                                .append(" )");
                        } else {
                            stringBuilder.append(hint)
                                .append("SELECT * FROM ")
                                .append(leftTable)
                                .append(" a WHERE ")
                                .append(buildSubquery(join_col_types, op))
                                .append(rightTable)
                                .append(" )");
                        }
                        rs.add(stringBuilder.toString());
                        break;
                    default:
                        throw new NotSupportException(this.name() + ":" + joinAlgorithm);
                    }
                }
                return rs;
            }
        };

        private static String buildSubquery(List<JOIN_COL_TYPE> colTypes, OP_TYPE op) {
            StringBuilder stringBuilder = new StringBuilder();
            colTypes.subList(0, colTypes.size() / 2).stream()
                .forEach(colType -> stringBuilder.append(colType.getColName()).append(","));
            stringBuilder.setLength(stringBuilder.length() - 1);
            String columnsString = stringBuilder.toString();
            stringBuilder.setLength(0);
            colTypes.subList(colTypes.size() / 2, colTypes.size()).stream()
                .forEach(colType -> stringBuilder.append(colType.getColName()).append(","));
            stringBuilder.setLength(stringBuilder.length() - 1);
            String rightColumnsString = stringBuilder.toString();
            stringBuilder.setLength(0);

            stringBuilder.append("(").append(columnsString).append(") ");
            stringBuilder.append(op.build());
            stringBuilder.append(" (SELECT ");
            stringBuilder.append(rightColumnsString)
                .append(" FROM ");
            return stringBuilder.toString();
        }

        private static String buildJoinCondition(List<JOIN_COL_TYPE> colTypes, OP_TYPE op) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < colTypes.size() / 2; i++) {
                stringBuilder.append("a." + colTypes.get(i).getColName()).append(op.build())
                    .append("b." + colTypes.get(colTypes.size() / 2 + i).getColName()).append(" AND ");
            }
            stringBuilder.setLength(stringBuilder.length() - 4);
            return stringBuilder.toString();
        }

        List<JOIN_ALGORITHM> supportAlgorithm = Lists.newLinkedList();

        JOIN_TYPE(JOIN_ALGORITHM... joinAlgorithms) {
            supportAlgorithm.addAll(Arrays.asList(joinAlgorithms));
        }

        public abstract List<String> buildSQLForBinaryJoin(JOIN_ALGORITHM joinAlgorithm, String leftTable,
                                                           String rightTable, OP_TYPE op,
                                                           List<JOIN_COL_TYPE> join_col_types);

        public Collection<? extends BinaryJoinUnit> buildBinaryJoinUnit(BinaryJoinUnit binaryJoinUnit) {
            binaryJoinUnit.joinType = this;
            List<BinaryJoinUnit> rs = Lists.newLinkedList();

            if (binaryJoinUnit.joinColTypes.size() > 2) {
                // meaning multi columns

            }

            for (JOIN_ALGORITHM algorithm : this.supportAlgorithm) {
                BinaryJoinUnit temp = binaryJoinUnit.copy();
                temp.joinAlgorithm = algorithm;
                if (!algorithm.isNL()) {
                    if (algorithm.isSubquery()) {
                        rs.add(temp.copy(OP_TYPE.EQUAL_ANY));
                    }
                    rs.add(temp.copy(OP_TYPE.EQUAL));
                } else {
                    for (OP_TYPE op : OP_TYPE.values()) {
                        if (!algorithm.isSubquery() && op.isAllOrAny()) {
                            continue;
                        }
                        if (!algorithm.isSubquery() && binaryJoinUnit.joinColTypes.size() > 2 && !op.isIN()) {
                            continue;
                        }
                        if (algorithm.isSemi() && op.isAll()) {
                            continue;
                        }
                        if (algorithm.isAnti() && op.isAny()) {
                            continue;
                        }
                        if (temp.joinType == LEFT_SEMI && op.isAllOrAny()) {
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
        NL("NL_JOIN", "NlJoin"), HASH("HASH_JOIN", "HashJoin"), HASH_OUTER("HASH_OUTER_JOIN", "HashJoin.*build="),
        SORT_MERGE("SORT_MERGE_JOIN", "SortMergeJoin"),
        BKA("BKA_JOIN", "BKAJoin"), MATERIALIZED_SEMI(
            "MATERIALIZED_SEMI_JOIN", "MaterializedSemiJoin"), SEMI_HASH("SEMI_HASH_JOIN", "SemiHashJoin"),
        ANTI_HASH("SEMI_HASH_JOIN", "SemiHashJoin"), SEMI_BKA("SEMI_BKA_JOIN", "SemiBKAJoin"), SEMI_NL(
            "SEMI_NL_JOIN", "SemiNLJoin"), ANTI_NL("SEMI_NL_JOIN", "SemiNLJoin");

        String hintName;
        String planName;

        JOIN_ALGORITHM(String hintName, String planName) {
            this.hintName = hintName;
            this.planName = planName;
        }

        public List<String> buildHint(String leftTableName, String rightTableName) {
            List<String> hints = Lists.newLinkedList();
            hints.add(
                "/*+TDDL: cmd_extra(ENABLE_CBO=true ENABLE_SEMI_HASH_JOIN=true PARALLELISM=0) " + this.hintName + "("
                    + leftTableName + "," + rightTableName + ") */ ");
            hints.add(
                "/*+TDDL: cmd_extra(ENABLE_CBO=true ENABLE_SEMI_HASH_JOIN=true) " + this.hintName + "(" + leftTableName
                    + "," + rightTableName + ") */ ");
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
            return this == SEMI_HASH || this == SEMI_NL || this == SEMI_BKA;
        }

        public boolean isAnti() {
            return this == ANTI_HASH || this == ANTI_NL;
        }
    }

    static class BinaryJoinUnit {
        List<JOIN_COL_TYPE> joinColTypes;
        JOIN_TYPE joinType;
        JOIN_ALGORITHM joinAlgorithm;
        OP_TYPE opType;
        String tableNameLeft;
        String tableNameRight;
        int leftRowNum;
        int rightRowNum;
        boolean sameNullRow;

        public BinaryJoinUnit(int leftRowNum, int rightRowNum) {
            this.leftRowNum = leftRowNum;
            this.rightRowNum = rightRowNum;
        }

        void buildTest(FullJoinTest joinTest) {
            /**
             * get data ready
             */
//            System.out.println("left data");

            List<String> sqls =
                joinType.buildSQLForBinaryJoin(joinAlgorithm, tableNameLeft, tableNameRight, opType, joinColTypes);
            for (String sql : sqls) {
//                System.out.println(sql);
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
            BinaryJoinUnit binaryJoinUnit = new BinaryJoinUnit(leftRowNum, rightRowNum);
            binaryJoinUnit.tableNameLeft = tableNameLeft;
            binaryJoinUnit.tableNameRight = tableNameRight;
            binaryJoinUnit.joinColTypes = joinColTypes;
            binaryJoinUnit.joinType = joinType;
            binaryJoinUnit.joinAlgorithm = joinAlgorithm;
            binaryJoinUnit.opType = opType;
            return binaryJoinUnit;
        }

        public BinaryJoinUnit copy(boolean sameNullRow,
                                   List<JOIN_COL_TYPE> joinColTypeList) {
            BinaryJoinUnit binaryJoinUnit = copy();
            binaryJoinUnit.sameNullRow = sameNullRow;
            binaryJoinUnit.joinColTypes = joinColTypeList;
            return binaryJoinUnit;
        }

        public BinaryJoinUnit copy(OP_TYPE op) {
            BinaryJoinUnit binaryJoinUnit = copy();
            binaryJoinUnit.opType = op;
            return binaryJoinUnit;
        }

        @Override
        public String toString() {
            return joinColTypes.toString() + ","
                + joinType + "," + joinAlgorithm + "," + opType;
        }
    }

    @Test
    public void fullJoinBinaryOperatorsTest() {
        binaryJoinUnit.buildTest(this);
    }

    public static List<BinaryJoinUnit> buildBinaryJoinUnit(String[][] tables) {
        List<BinaryJoinUnit> toTest = Lists.newLinkedList();
        int rowNum = 3;
        for (String[] table : tables) {
            BinaryJoinUnit binaryJoinUnit = new BinaryJoinUnit(rowNum, rowNum);
            binaryJoinUnit.tableNameLeft = table[0];
            binaryJoinUnit.tableNameRight = table[1];

            // 1 column
            int columnNum = 1;
            for (JOIN_TYPE joinType : JOIN_TYPE.values()) {
                for (int j = 0; j < 2; j++) {
                    List<List<JOIN_COL_TYPE>> joinColTypes = buildJoinColTypes(columnNum * 2);
                    for (List<JOIN_COL_TYPE> joinColTypeList : joinColTypes) {
                        if (j == 0) {
                            toTest.addAll(
                                joinType.buildBinaryJoinUnit(
                                    binaryJoinUnit
                                        .copy(true, joinColTypeList)
                                )
                            );
                        } else {
                            toTest.addAll(
                                joinType.buildBinaryJoinUnit(
                                    binaryJoinUnit
                                        .copy(false, joinColTypeList)
                                )

                            );
                        }
                    }

                    //op type random handle inside.
                }

            }

            // 2 column
            columnNum = 2;
            for (JOIN_TYPE joinType : JOIN_TYPE.values()) {
                for (int j = 0; j < 2; j++) {
                    List<List<JOIN_COL_TYPE>> joinColTypes = buildJoinColTypes(columnNum * 2);
                    for (List<JOIN_COL_TYPE> joinColTypeList : joinColTypes) {
                        if (j == 0) {
                            toTest.addAll(
                                joinType.buildBinaryJoinUnit(
                                    binaryJoinUnit
                                        .copy(true,
                                            joinColTypeList)
                                )
                            );
                        } else {
                            toTest.addAll(
                                joinType.buildBinaryJoinUnit(
                                    binaryJoinUnit
                                        .copy(false,
                                            joinColTypeList)
                                )

                            );
                        }
                    }

                    //op type random handle inside.
                }

            }

        }

        List<BinaryJoinUnit> toRemove = Lists.newArrayList();
        for (BinaryJoinUnit binaryJoinUnit : toTest) {
            if (!isSymmetry(binaryJoinUnit.joinColTypes)) {
                toRemove.add(binaryJoinUnit);
            }

            if (binaryJoinUnit.joinAlgorithm.isSubquery() && binaryJoinUnit.opType.isComparsion()) {
                toRemove.add(binaryJoinUnit);
            }
        }
        toTest.removeAll(toRemove);

        for (BinaryJoinUnit binaryJoinUnit : toTest) {
            if (binaryJoinUnit.opType.isComparsion()) {
                binaryJoinUnit.rightRowNum = 1;
            }
        }
        return toTest;
    }

    private static boolean isSymmetry(List<JOIN_COL_TYPE> joinColTypes) {
        for (int i = 0; i < joinColTypes.size() / 2; i++) {
            if (joinColTypes.get(i) != joinColTypes.get(i + joinColTypes.size() / 2)) {
                return false;
            }
        }
        return true;
    }

    private static List<List<JOIN_COL_TYPE>> buildJoinColTypes(int columnNum) {
        if (columnNum == 1) {
            return Arrays.stream(JOIN_COL_TYPE.values()).map(joinColType -> Lists.newArrayList(joinColType))
                .collect(Collectors.toList());
        }
        List<List<JOIN_COL_TYPE>> listReturn = Lists.newArrayList();
        for (JOIN_COL_TYPE joinColType : JOIN_COL_TYPE.values()) {
            List<List<JOIN_COL_TYPE>> listsTemp = buildJoinColTypes(columnNum - 1);
            listsTemp.stream().forEach(list -> list.add(joinColType));
            listReturn.addAll(listsTemp);
        }
        return listReturn;
    }

    protected void buildEmptyData(String tableName) {
        executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            "delete from " + tableName,
            Lists.newArrayList());
    }

    private static void readyTheData(Connection mysqlConnection, Connection tddlConnection, String tableName) {
        executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            "delete from " + tableName,
            Lists.newArrayList());
        /**
         * prepare data
         */
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String dataSql = buildInsertColumnsSQLWithTableName(columns, tableName);
        List<List<Object>> paramsList = buildParams(new ColumnDataGenerator(), 2, columns);
        paramsList.stream().forEach(
            params -> executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dataSql,
                params));
    }

    private static List<List<Object>> buildParams(
        ColumnDataGenerator columnDataGenerator, int iterNum, List<ColumnEntity> columns) {
        List<List<Object>> paramsList = Lists.newArrayList();
        int pk = 0;
        for (int j = 0; j < iterNum; j++) {
            //生产正常一行
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, pk++);
            paramsList.add(param);
            //迭代生成各个null值
            for (int i = 1; i < columns.size(); i++) {
                List<Object> ret =
                    columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, pk++);
                ret.set(i, null);
                paramsList.add(ret);
            }
        }
        return paramsList;
    }
}
