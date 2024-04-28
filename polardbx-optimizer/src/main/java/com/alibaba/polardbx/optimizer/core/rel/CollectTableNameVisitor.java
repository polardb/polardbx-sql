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

package com.alibaba.polardbx.optimizer.core.rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * @author dylan
 */
public abstract class CollectTableNameVisitor extends SqlShuttle {

    protected SqlKind sqlKind = SqlKind.SELECT;

    protected CollectTableNameVisitor() {

    }

    @Override
    public SqlNode visit(SqlCall call) {
        SqlKind kind = call.getKind();
        if (kind == SqlKind.SELECT) {
            this.sqlKind = kind;

            SqlCall copy = SqlNode.clone(call);
            SqlSelect select = (SqlSelect) copy;

            /**
             * Replace tableName at Select.
             */
            if (select.getSelectList() != null) {
                visit(select.getSelectList());
            }

            /**
             * Replace tableName at From.
             */
            final SqlNode from = select.getFrom();
            convertFrom(from);

            /**
             * Replace tableName at where subSelect
             */
            SqlNode where = select.getWhere();
            if (where instanceof SqlCall) {
                visit((SqlCall) where);
            }

            /**
             * Having
             */
            SqlNode having = select.getHaving();
            if (having instanceof SqlCall) {
                visit((SqlCall) having);
            }

            return select;
        }

        if (kind == SqlKind.DELETE) {
            this.sqlKind = kind;

            SqlDelete delete = (SqlDelete) call;

            if (delete.singleTable()) {
                if (delete.getTargetTable().getKind() == SqlKind.IDENTIFIER) {
                    /**
                     * DELETE does't support alias, so we need clear ALL
                     * table-refers.
                     */
                    final SqlIdentifier identifier = (SqlIdentifier) delete.getTargetTable();
                    buildSth(identifier);
                }
            } else {
                /**
                 * Multi table delete
                 */
                final SqlNode from = delete.getSourceTableNode();
                if (from != null) {
                    SqlKind fromKind = from.getKind();
                    // 单表查询
                    if (fromKind == SqlKind.IDENTIFIER) {
                        SqlIdentifier identifier = (SqlIdentifier) from;
                        buildForIdentifier(identifier);
                    } else if (fromKind == SqlKind.AS) {
                        buildAsNode(from);
                    } else if (fromKind == SqlKind.JOIN) {
                        // 多表JOIN
                        visit((SqlJoin) from);
                    }
                }
            }

            // Replace table-refers on WHERE condition
            if (delete.getCondition() instanceof SqlCall) {
                visit((SqlCall) delete.getCondition());
            }

            // Replace table-refers on ORDER BY column
            if (delete.getOrderList() != null && delete.getOrderList().size() > 0) {
                visit(delete.getOrderList());
            }

            return delete;
        }

        if (kind == SqlKind.UPDATE) {
            this.sqlKind = kind;

            final SqlUpdate update = (SqlUpdate) call;
            final SqlNode targetTable = update.getTargetTable();
            if (update.singleTable()) {
                if (update.getAlias() != null || targetTable.getKind() == SqlKind.AS) {
                    buildSth(targetTable);
                } else if (targetTable.getKind() == SqlKind.IDENTIFIER) {
                    final SqlIdentifier identifier = (SqlIdentifier) targetTable;
                    buildForIdentifier(identifier);
                }
            } else {
                /**
                 * Multi table update
                 */
                final SqlKind targetKind = targetTable.getKind();
                // 单表查询
                if (targetKind == SqlKind.IDENTIFIER) {
                    final SqlIdentifier identifier = (SqlIdentifier) targetTable;
                    buildForIdentifier(identifier);
                } else if (targetKind == SqlKind.AS) {
                    buildAsNode(targetTable);
                } else if (targetKind == SqlKind.JOIN) {
                    // 多表JOIN
                    visit((SqlJoin) targetTable);
                }
            }

            // Replace table-refers on WHERE condition
            if (update.getCondition() instanceof SqlCall) {
                visit((SqlCall) update.getCondition());
            }

            // Replace table-refers on ORDER BY column
            if (update.getOrderList() != null && update.getOrderList().size() > 0) {
                visit(update.getOrderList());
            }

            return update;
        }
        if (SqlKind.SEQUENCE_DDL.contains(kind)) {
            this.sqlKind = kind;

            return call;
        } else if (SqlKind.SUPPORT_DDL.contains(kind)) {
            this.sqlKind = kind;

            SqlDdl ddl = (SqlDdl) call;
            SqlNode targetTable = ddl.getTargetTable();
            if (targetTable != null && targetTable.getKind() == SqlKind.IDENTIFIER) {
                buildSth(targetTable);
            }
            return ddl;
        } else if (kind == SqlKind.RENAME_TABLE) {
            this.sqlKind = kind;

            SqlDdl ddl = (SqlDdl) call;
            SqlNode targetTable = ddl.getTargetTable();
            if (targetTable.getKind() == SqlKind.IDENTIFIER) {
                buildSth(targetTable);
            }
            return ddl;
        }

        if (kind == SqlKind.INSERT || kind == SqlKind.REPLACE) {
            this.sqlKind = kind;

            return buildInsertNode(call);
        }

        if (kind == SqlKind.AS) {
            return buildAsNodeForSelect(call);
        }

        if (kind == SqlKind.JOIN) {
            return buildJoinNode(call);
        }

        // replace table ignore show stmt
        if (SqlKind.LOGICAL_SHOW_QUERY.contains(kind) || SqlKind.SHOW_QUERY.contains(kind)
            || SqlKind.TABLE_MAINTENANCE_QUERY.contains(kind)) {
            this.sqlKind = kind;

            return call;
        }

        return super.visit(call);
    }

    public void convertFrom(SqlNode from) {
        if (null == from) {
            return;
        }

        SqlKind fromKind = from.getKind();
        // 单表查询
        if (fromKind == SqlKind.IDENTIFIER) {
            SqlIdentifier identifier = (SqlIdentifier) from;
            buildForIdentifier(identifier);
        } else if (fromKind == SqlKind.AS) {
            final SqlCall as = (SqlCall) from;
            buildAsNode(as);
        } else if (fromKind == SqlKind.JOIN) {
            // 多表JOIN
            visit((SqlJoin) from);
        } else if (fromKind == SqlKind.SELECT) {
            visit((SqlSelect) from);
        }
    }

    @Override
    public SqlNode visit(SqlLiteral literal) {
        return SqlNode.clone(literal);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        return SqlNode.clone(id);
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
        return SqlNode.clone(type);
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
        return SqlNode.clone(param);
    }

    @Override
    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
        return SqlNode.clone(intervalQualifier);
    }

    @Override
    public SqlNode visit(SqlNodeList nodeList) {
        SqlNodeList copy = new SqlNodeList(nodeList.getParserPosition());
        for (SqlNode node : nodeList) {
            copy.add(node.accept(this));
        }
        return copy;
    }

    public SqlNode buildJoinNode(SqlNode join) {
        SqlJoin sqlJoin = (SqlJoin) join.clone(SqlParserPos.ZERO);

        /**
         * Left
         */
        SqlNode left = sqlJoin.getLeft();
        if (left instanceof SqlCall) {
            if (((SqlCall) left).getOperator() instanceof SqlAsOperator) {
                buildAsNode(left);
            } else {
                visit((SqlCall) left);
            }
        } else if (left instanceof SqlIdentifier) {
            buildForIdentifier((SqlIdentifier) left);
        }

        /**
         * Right
         */
        SqlNode right = sqlJoin.getRight();
        if (right instanceof SqlCall) {
            if (((SqlCall) right).getOperator() instanceof SqlAsOperator) {
                buildAsNode(right);
            } else {
                visit((SqlCall) right);
            }
        } else if (right instanceof SqlIdentifier) {
            buildForIdentifier((SqlIdentifier) right);
        }

        return sqlJoin;
    }

    protected SqlNode buildAsNodeForSelect(SqlNode oldNode) {
        SqlBasicCall call = (SqlBasicCall) oldNode;
        SqlNode aliasNode = call.getOperandList().get(1);
        String aliasName;
        SqlIdentifier sqlIdentifier = null;
        if (aliasNode instanceof SqlIdentifier) {
            aliasName = ((SqlIdentifier) aliasNode).getSimple();
            sqlIdentifier = new SqlIdentifier(ImmutableList
                .of(aliasName), null, aliasNode.getParserPosition(), null, ((SqlIdentifier) aliasNode).indexNode);
        }

        SqlNode leftNode = call.getOperandList().get(0);
        // 子查询
        if (leftNode instanceof SqlSelect) {
            leftNode = visit((SqlSelect) leftNode);
        } else if (leftNode instanceof SqlIdentifier) {
            SqlIdentifier idOfLeft = (SqlIdentifier) leftNode;
            int lastNamesIdx = idOfLeft.names.size() - 1;
            if (idOfLeft.names.get(lastNamesIdx).equalsIgnoreCase(SqlFunction.NEXTVAL_FUNC_NAME)) {
                oldNode = buildNextVal(idOfLeft);
            }
            return oldNode;
        } else if (leftNode instanceof SqlCall) {
            leftNode = visit((SqlCall) leftNode);
        }

        return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, leftNode, sqlIdentifier);
    }

    protected SqlNode buildAsNode(SqlNode oldNode) {
        final SqlBasicCall call = (SqlBasicCall) oldNode;
        final SqlNode aliasNode = call.getOperandList().get(1);
        String aliasName;
        SqlIdentifier sqlIdentifier = null;
        if (aliasNode instanceof SqlIdentifier) {
            aliasName = ((SqlIdentifier) aliasNode).getSimple();
            sqlIdentifier = new SqlIdentifier(ImmutableList
                .of(aliasName), null, aliasNode.getParserPosition(), null, ((SqlIdentifier) aliasNode).indexNode);
        }

        SqlNode leftNode = call.getOperandList().get(0);
        // 子查询
        if (leftNode instanceof SqlSelect) {
            leftNode = visit((SqlSelect) leftNode);
        } else if (leftNode instanceof SqlIdentifier) {
            leftNode = buildSth(leftNode);
        } else if (leftNode instanceof SqlCall) {
            leftNode = visit((SqlCall) leftNode);
        }

        return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, leftNode, sqlIdentifier);
    }

    protected SqlCall buildForIdentifier(SqlIdentifier identifier) {
        SqlNode indexNode = identifier.indexNode;

        final SqlNode clone = buildSth(identifier).clone(SqlParserPos.ZERO);
        if (clone instanceof SqlIdentifier) {
            ((SqlIdentifier) clone).indexNode = null;
        }
        SqlIdentifier asName = new SqlIdentifier(ImmutableList
            .of(Util.last(identifier.names)), null, identifier.getParserPosition(), null, indexNode);
        return SqlStdOperatorTable.AS.createCall(identifier.getParserPosition(), clone, asName);
    }

    protected abstract <T extends SqlNode> T buildSth(SqlNode sqlNode);

    protected SqlNode buildInsertNode(SqlNode oldNode) {
        SqlInsert oldInsert = (SqlInsert) oldNode;
        if (oldInsert.getTargetTable() instanceof SqlIdentifier) {
        }

        List<SqlNode> operandList = oldInsert.getOperandList();
        buildSth(oldInsert.getTargetTable());
        SqlNode source = operandList.get(2);
        if (source instanceof SqlCall) {
            visit((SqlCall) source);
        }
        return oldNode;
    }

    protected SqlNode buildNextVal(SqlIdentifier nextVal) {
        return nextVal;
    }
}
