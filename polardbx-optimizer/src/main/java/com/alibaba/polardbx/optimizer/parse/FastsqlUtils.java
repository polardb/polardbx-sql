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

package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator.BooleanOr;

public class FastsqlUtils {

    private static final List<SQLParserFeature> DEFAULT_FEATURES = Arrays.asList(
        SQLParserFeature.TDDLHint,
        SQLParserFeature.EnableCurrentUserExpr,
        SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DRDSBaseline,
        SQLParserFeature.DrdsGSI,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DrdsCCL
    );

    public static List<SQLStatement> parseSql(String stmt, SQLParserFeature... features) {
        List<SQLParserFeature> featuresList = new ArrayList<>(features.length + DEFAULT_FEATURES.size());
        featuresList.addAll(Arrays.asList(features));
        featuresList.addAll(DEFAULT_FEATURES);
        MySqlStatementParser parser = new MySqlStatementParser(stmt, featuresList.toArray(new SQLParserFeature[0]));
        return parser.parseStatementList();
    }

    public static List<SQLStatement> parseSql(ByteString stmt, SQLParserFeature... features) {
        List<SQLParserFeature> featuresList = new ArrayList<>(features.length + DEFAULT_FEATURES.size());
        featuresList.addAll(Arrays.asList(features));
        featuresList.addAll(DEFAULT_FEATURES);
        MySqlStatementParser parser = new MySqlStatementParser(stmt, featuresList.toArray(new SQLParserFeature[0]));
        return parser.parseStatementList();
    }

    public static List<SQLDataType> parseDataType(String stmt, SQLParserFeature... features) {
        List<SQLParserFeature> featuresList = new ArrayList<>(features.length + DEFAULT_FEATURES.size());
        featuresList.addAll(Arrays.asList(features));
        featuresList.addAll(DEFAULT_FEATURES);
        MySqlStatementParser parser = new MySqlStatementParser(stmt, featuresList.toArray(new SQLParserFeature[0]));
        return parser.parseDataTypeList();
    }

    public static SQLPartitionBy parsePartitionBy(String stmt, boolean forTableGroup, SQLParserFeature... features) {
        List<SQLParserFeature> featuresList = new ArrayList<>(features.length + DEFAULT_FEATURES.size());
        featuresList.addAll(Arrays.asList(features));
        featuresList.addAll(DEFAULT_FEATURES);
        MySqlStatementParser parser = new MySqlStatementParser(stmt, featuresList.toArray(new SQLParserFeature[0]));
        return parser.getSQLCreateTableParser().parsePartitionBy(forTableGroup);
    }

    /**
     * a strut records information used in non-recursive dfs
     */
    public static class BinaryOpTreeNode {
        SQLBinaryOpExpr x;
        MergeLinkedList<SqlNode> operands;
        int visit;
        BinaryOpTreeNode parent;

        public BinaryOpTreeNode(SQLBinaryOpExpr x, BinaryOpTreeNode parent) {
            this.x = x;
            this.parent = parent;
            this.visit = 0;
        }

        public boolean leftVisited() {
            return visit >= 1;
        }

        public boolean rightVisited() {
            return visit >= 2;
        }

        public void visit() {
            visit++;
        }

        public SQLBinaryOpExpr getX() {
            return x;
        }

        /**
         * add an operand to current node
         *
         * @param child the operand to be added
         */
        public void addOperand(SqlNode child) {
            if (operands == null) {
                operands = new MergeLinkedList<>(child);
            } else {
                operands.add(child);
            }
        }

        /**
         * add an operand list to current node in O(1)
         *
         * @param children the operand list to be added
         */
        private void addOperand(MergeLinkedList<SqlNode> children) {
            if (operands == null) {
                operands = children;
                return;
            }
            operands.merge(children);
        }

        public void addOperandToParent() {
            if (parent != null) {
                parent.addOperand(this.getOperands());
            }
        }

        public MergeLinkedList<SqlNode> getOperands() {
            return operands;
        }
    }

    /**
     * an append-only single direction linked-list supporting O(1) merge
     */
    public static class MergeLinkedList<E> {

        private static class Node<E> {
            E item;
            Node<E> next;

            Node(E element) {
                this.item = element;
                this.next = null;
            }
        }

        // head of the list
        private final Node<E> head;

        // tail of the list
        private Node<E> tail;

        // length of the list
        int size;

        public MergeLinkedList(E element) {
            Node<E> node = new Node<>(element);
            head = tail = node;
            size = 1;
        }

        /**
         * add new element to the tail of the list
         */
        public void add(E element) {
            Node<E> node = new Node<>(element);
            this.tail.next = node;
            this.tail = node;
            this.size++;
        }

        /**
         * merge two list in O(1)
         */
        public void merge(MergeLinkedList<E> list) {
            if (list == null) {
                return;
            }
            this.tail.next = list.head;
            this.tail = list.tail;
            this.size += list.size;
        }

        public int getSize() {
            return size;
        }

        public void toArray(E[] array) {
            int i = 0;
            for (Node<E> x = head; x != null; x = x.next) {
                array[i++] = x.item;
            }
        }

    }

    public static boolean isOrExpr(SQLExpr expr) {
        return expr instanceof SQLBinaryOpExpr && ((SQLBinaryOpExpr) expr).getOperator() == BooleanOr;
    }

}
