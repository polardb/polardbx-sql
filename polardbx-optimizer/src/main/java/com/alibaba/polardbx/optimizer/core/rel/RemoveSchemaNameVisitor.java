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

import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlReplace;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * @author lingce.ldm 2018-01-12 15:52
 */
public class RemoveSchemaNameVisitor extends SqlShuttle {

    private final static Logger logger = LoggerFactory.getLogger(PlannerUtils.class);

    private String logicalSchemaName;

    public RemoveSchemaNameVisitor(String logicalSchemaName) {
        this.logicalSchemaName = logicalSchemaName;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        SqlCall copy = SqlNode.clone(call);
        SqlKind kind = call.getKind();
        if (kind == SqlKind.SELECT) {
            SqlSelect select = (SqlSelect) copy;

            select.setSelectList((SqlNodeList) visit(select.getSelectList()));
            /**
             * Replace tableName at From.
             */
            SqlNode from = select.getFrom();
            if (from != null) {
                SqlKind fromKind = from.getKind();
                // 单表查询
                if (fromKind == SqlKind.IDENTIFIER) {
                    SqlIdentifier identifier = (SqlIdentifier) from;
                    select.setFrom(buildNewIdentifier(identifier));
                } else if (fromKind == SqlKind.AS) {
                    select.setFrom(buildAsNode(from));
                } else if (fromKind == SqlKind.JOIN) {
                    // 多表JOIN
                    select.setFrom(visit((SqlJoin) from));
                }
            }

            /**
             * Replace tableName at where subSelect
             */
            SqlNode where = select.getWhere();
            if (where instanceof SqlCall) {
                select.setWhere(visit((SqlCall) where));
            }

            /**
             * Having
             */
            SqlNode having = select.getHaving();
            if (having instanceof SqlCall) {
                select.setHaving(visit((SqlCall) having));
            }
            return select;
        }

        if (kind == SqlKind.DELETE) {
            final SqlDelete delete = (SqlDelete) call;

            if (delete.singleTable()) {
                Preconditions.checkArgument(delete.getTargetTable().getKind() == SqlKind.IDENTIFIER);
                /**
                 * set TargetTable, DELETE DO NOT use alias for MYSQL.
                 */
                SqlIdentifier targetTable = (SqlIdentifier) delete.getTargetTable();
                delete.setOperand(0, buildNewIdentifier(targetTable));
            } else {
                /**
                 * Multi table delete
                 */
                SqlNodeList targetTableList = delete.getTargetTables();
                for (int i = 0; i < targetTableList.size(); ++i) {
                    SqlNode targetTable = targetTableList.get(i);
                    if (targetTable.getKind() == SqlKind.IDENTIFIER) {
                        SqlNode newTargetTable = buildNewIdentifier((SqlIdentifier) targetTable);
                        targetTableList.set(i, newTargetTable);
                    }
                }

                boolean fromUsing = delete.fromUsing();
                if (fromUsing) {
                    SqlNode from = delete.getUsing();
                    if (from != null) {
                        SqlKind fromKind = from.getKind();
                        // 单表
                        if (fromKind == SqlKind.IDENTIFIER) {
                            SqlIdentifier identifier = (SqlIdentifier) from;
                            delete.setUsing(buildNewIdentifier(identifier));
                        } else if (fromKind == SqlKind.AS) {
                            delete.setUsing(buildAsNode(from));
                        } else if (fromKind == SqlKind.JOIN) {
                            // 多表
                            delete.setUsing(visit((SqlJoin) from));
                        }
                    }
                } else {
                    SqlNode from = delete.getFrom();
                    if (from != null) {
                        SqlKind fromKind = from.getKind();
                        // 单表
                        if (fromKind == SqlKind.IDENTIFIER) {
                            SqlIdentifier identifier = (SqlIdentifier) from;
                            delete.setFrom(buildNewIdentifier(identifier));
                        } else if (fromKind == SqlKind.AS) {
                            delete.setFrom(buildAsNode(from));
                        } else if (fromKind == SqlKind.JOIN) {
                            // 多表
                            delete.setFrom(visit((SqlJoin) from));
                        }
                    }
                }
            }

            /**
             * Replace tableName at where subSelect
             * Remove schema in where condition
             */
            SqlNode where = delete.getCondition();
            if (where instanceof SqlCall) {
                delete.setCondition(visit((SqlCall) where));
            }

            /**
             * Remove schema in order list
             */
            final SqlNodeList orderList = delete.getOrderList();
            if (null != orderList) {
                delete.setOrderList((SqlNodeList) visit(orderList));
            }

            return delete;
        }

        if (kind == SqlKind.UPDATE) {
            final SqlUpdate update = (SqlUpdate) call;
            final SqlNode targetTable = update.getTargetTable();

            if (update.singleTable()) {
                if (targetTable.getKind() == SqlKind.IDENTIFIER) {
                    update.setTargetTable(buildNewIdentifier((SqlIdentifier) targetTable));
                } else if (targetTable.getKind() == SqlKind.AS) {
                    update.setTargetTable(buildAsNode(targetTable));
                } else {
                    throw new UnsupportedOperationException("Unsupported update syntax.");
                }
            } else {
                /**
                 * Multi table update
                 */
                final SqlKind targetKind = targetTable.getKind();
                // 单表
                if (targetKind == SqlKind.IDENTIFIER) {
                    final SqlIdentifier identifier = (SqlIdentifier) targetTable;
                    update.setTargetTable(buildNewIdentifier(identifier));
                } else if (targetKind == SqlKind.AS) {
                    update.setTargetTable(buildAsNode(targetTable));
                } else if (targetKind == SqlKind.JOIN) {
                    // 多表
                    update.setTargetTable(visit((SqlJoin) targetTable));
                }
            }

            /**
             * Remove schema name in target column list
             */
            final SqlNodeList targetColumnList = update.getTargetColumnList();
            update.setTargetColumnList((SqlNodeList) visit(targetColumnList));

            /**
             * Remove schema name in source expression list of set clause
             */
            final SqlNodeList sourceExpressionList = update.getSourceExpressionList();
            update.setSourceExpressionList((SqlNodeList) visit(sourceExpressionList));

            /**
             * Replace tableName at where subSelect
             * Remove schema in where condition
             */
            SqlNode where = update.getCondition();
            if (where instanceof SqlCall) {
                update.setCondition(visit((SqlCall) where));
            }

            /**
             * Remove schema in order list
             */
            final SqlNodeList orderList = update.getOrderList();
            if (null != orderList) {
                update.setOrderList((SqlNodeList) visit(orderList));
            }

            return update;
        }

        if (kind == SqlKind.INSERT || kind == SqlKind.REPLACE) {
            return buildInsertNode(call);
        }

        if (kind == SqlKind.AS) {
            return buildAsNode(call);
        }

        if (kind == SqlKind.JOIN) {
            return buildJoinNode(call);
        }

        return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlLiteral literal) {
        return SqlNode.clone(literal);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        List<String> names = id.names;
        if (names.size() == 3) {
            // Column reference with schema name specified
            return new SqlIdentifier(names.subList(1, names.size()), SqlParserPos.ZERO);
        } else {
            return SqlNode.clone(id);
        }
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
            sqlJoin.setLeft(visit((SqlCall) left));
        } else if (left instanceof SqlIdentifier) {
            sqlJoin.setLeft(buildNewIdentifier((SqlIdentifier) left));
        }

        /**
         * Right
         */
        SqlNode right = sqlJoin.getRight();
        if (right instanceof SqlCall) {
            sqlJoin.setRight(visit((SqlCall) right));
        } else if (right instanceof SqlIdentifier) {
            sqlJoin.setRight(buildNewIdentifier((SqlIdentifier) right));
        }

        return sqlJoin;
    }

    public SqlNode buildAsNode(SqlNode oldNode) {
        SqlBasicCall call = (SqlBasicCall) oldNode;
        SqlNode aliasNode = call.getOperandList().get(1);
        String aliasName;
        SqlIdentifier sqlIdentifier = null;
        if (aliasNode instanceof SqlIdentifier) {
            aliasName = ((SqlIdentifier) aliasNode).getSimple();
            sqlIdentifier = new SqlIdentifier(ImmutableList
                .of(aliasName), null, aliasNode.getParserPosition(), null, ((SqlIdentifier) aliasNode).indexNode);
            if (StringUtils.isEmpty(aliasName)) {
                String errMsg = "The alias name is null for Native Sql";
                logger.error(errMsg);
                throw new OptimizerException(errMsg);
            }
        }

        SqlNode leftNode = call.getOperandList().get(0);
        // 子查询
        if (leftNode instanceof SqlSelect) {
            leftNode = visit((SqlSelect) leftNode);
        } else if (leftNode instanceof SqlIdentifier) {
            leftNode = buildNewIdentifier((SqlIdentifier) leftNode);
        } else if (leftNode instanceof SqlCall) {
            leftNode = visit((SqlCall) leftNode);
        } else if (leftNode instanceof SqlDynamicParam) {
            // do nothing
        }
        return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, leftNode, sqlIdentifier);
    }

    public SqlNode buildNewIdentifier(SqlIdentifier identifier) {
        List<String> names = identifier.names;
        if (names.size() == 1) {
            return identifier;
        } else if (names.size() == 2) {
            String schemaName = names.get(0);
            /**
             * If schemaName equals with the logicalSchemaName, remove it.
             */
            if (schemaName.equalsIgnoreCase(logicalSchemaName)) {
                return new SqlIdentifier(names.get(1), SqlParserPos.ZERO, identifier.indexNode);
            }
        } else {
            // Impossible.
            throw new OptimizerException("Size of identifier names is not 1 or 2.");
        }
        return identifier;
    }

    public SqlNode buildInsertNode(SqlNode oldNode) {
        SqlInsert oldInsert = (SqlInsert) oldNode;
        List<SqlNode> operandList = oldInsert.getOperandList();
        SqlNode childNode = operandList.get(1);
        if (childNode instanceof SqlCall) {
            childNode = visit((SqlCall) childNode);
        } else if (childNode instanceof SqlIdentifier) {
            childNode = buildNewIdentifier((SqlIdentifier) childNode);
        }
        SqlNode source = operandList.get(2);
        if (source instanceof SqlCall) {
            source = visit((SqlCall) source);
        }
        SqlInsert sqlInsert;
        if (oldInsert.getKind() == SqlKind.INSERT) {
            sqlInsert = new SqlInsert(oldInsert.getParserPosition(),
                (SqlNodeList) operandList.get(0),
                childNode,
                source,
                (SqlNodeList) operandList.get(3),
                (SqlNodeList) operandList.get(4),
                oldInsert.getBatchSize(),
                oldInsert.getHints());
        } else {
            sqlInsert = new SqlReplace(oldInsert.getParserPosition(),
                (SqlNodeList) operandList.get(0),
                childNode,
                source,
                (SqlNodeList) operandList.get(3),
                oldInsert.getBatchSize(),
                oldInsert.getHints());
        }
        return sqlInsert;
    }
}
