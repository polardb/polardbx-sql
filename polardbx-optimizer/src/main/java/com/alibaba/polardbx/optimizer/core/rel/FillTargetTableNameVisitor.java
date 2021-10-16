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

import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.calcite.sql.SqlAsOperator;
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
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chuanqin on 18/6/29.
 */
public class FillTargetTableNameVisitor extends SqlShuttle {

    private final static Logger logger = LoggerFactory.getLogger(PlannerUtils.class);

    public int TABLE_NAME_PARAM_INDEX = 0;

    private List<String> tableNames = new ArrayList<>();

    public FillTargetTableNameVisitor(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        SqlCall copy = SqlNode.clone(call);
        SqlKind kind = call.getKind();
        if (kind == SqlKind.SELECT) {
            SqlSelect select = (SqlSelect) copy;

            /**
             * Replace tableName at Select.
             */
            if (select.getSelectList() != null) {
                select.setSelectList((SqlNodeList) visit(select.getSelectList()));
            }

            /**
             * Replace tableName at From.
             */
            SqlNode from = select.getFrom();
            if (from != null) {
                SqlKind fromKind = from.getKind();
                // 单表查询
                if (fromKind == SqlKind.DYNAMIC_PARAM) {
                    SqlDynamicParam dynamicParam = (SqlDynamicParam) from;
                    select.setFrom(buildForIdentifier(dynamicParam));
                } else if (fromKind == SqlKind.AS) {
                    SqlNode asNode = buildAsNode((SqlCall) from);
                    select.setFrom(asNode);
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
            SqlDelete delete = (SqlDelete) call;
            Preconditions.checkArgument(delete.getTargetTable().getKind() == SqlKind.DYNAMIC_PARAM);
            /**
             * set TargetTable, DELETE DO NOT use alias for MYSQL.
             */

            delete.setOperand(0, buildForIdentifier((SqlDynamicParam) delete.getTargetTable()));
            return delete;
        }

        if (kind == SqlKind.UPDATE) {
            SqlUpdate update = (SqlUpdate) call;
            SqlNode targetTable = update.getTargetTable();
            if (targetTable.getKind() == SqlKind.DYNAMIC_PARAM) {
                update.setOperand(0, buildForIdentifier((SqlDynamicParam) update.getTargetTable()));
            } else if (targetTable.getKind() == SqlKind.AS) {
                update.setOperand(1,
                    buildForIdentifier((SqlDynamicParam) ((SqlCall) targetTable).getOperandList().get(0)));
            } else {
                throw new UnsupportedOperationException("Unsupported update syntax.");
            }
            return update;
        }
        // if (SqlKind.SEQUENCE_DDL.contains(kind)) {
        // return call;
        // } else if (SqlKind.SUPPORT_DDL.contains(kind)) {
        // SqlDdl ddl = (SqlDdl) call;
        // SqlNode targetTable = ddl.getTargetTable();
        // if (targetTable.getKind() == SqlKind.IDENTIFIER) {
        // tableNames.add(Util.last(((SqlIdentifier) targetTable).names));
        // ddl.setTargetTable(buildDynamicParam());
        // } else if (targetTable instanceof SqlDynamicParam) {
        // } else {
        // throw new UnsupportedOperationException("Unsupported DDL syntax.");
        // }
        // return ddl;
        // } else if (kind == SqlKind.RENAME_TABLE) {
        // SqlDdl ddl = (SqlDdl) call;
        // SqlNode targetTable = ddl.getTargetTable();
        // if (targetTable.getKind() == SqlKind.IDENTIFIER) {
        // tableNames.add(Util.last(((SqlIdentifier) targetTable).names));
        // ddl.setTargetTable(buildDynamicParam());
        // } else if (targetTable instanceof SqlDynamicParam) {
        // } else {
        // throw new UnsupportedOperationException("Unsupported DDL syntax.");
        // }
        // return ddl;
        // }

        if (kind == SqlKind.INSERT || kind == SqlKind.REPLACE) {
            return buildInsertNode(call);
        }

        // if (kind == SqlKind.AS) {
        // return buildAsNodeForSelect(call);
        // }

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
        SqlJoin joinNode = (SqlJoin) join;
        SqlJoin sqlJoin = new SqlJoin(join.getParserPosition(),
            SqlNode.clone(joinNode.getLeft()),
            joinNode.isNaturalNode(),
            joinNode.getJoinTypeNode(),
            SqlNode.clone(joinNode.getRight()),
            joinNode.getConditionTypeNode(),
            joinNode.getCondition());

        /**
         * Left
         */
        SqlNode left = sqlJoin.getLeft();
        if (left instanceof SqlCall) {
            if (((SqlCall) left).getOperator() instanceof SqlAsOperator) {
                sqlJoin.setLeft(buildAsNode((SqlCall) left));
            } else {
                sqlJoin.setLeft(visit((SqlCall) left));
            }
        } else if (left instanceof SqlDynamicParam) {
            sqlJoin.setLeft(buildForIdentifier((SqlDynamicParam) left));
        }

        /**
         * Right
         */
        SqlNode right = sqlJoin.getRight();
        if (right instanceof SqlCall) {
            if (((SqlCall) right).getOperator() instanceof SqlAsOperator) {
                sqlJoin.setRight(buildAsNode((SqlCall) right));
            } else {
                sqlJoin.setRight(visit((SqlCall) right));
            }
        } else if (right instanceof SqlDynamicParam) {
            sqlJoin.setRight(buildForIdentifier((SqlDynamicParam) right));
        }

        return sqlJoin;
    }

    private SqlNode buildAsNode(SqlCall asNode) {
        List<SqlNode> operands = new ArrayList<SqlNode>(asNode.getOperandList());
        SqlCall asNodeCopy = new SqlBasicCall(asNode.getOperator(),
            operands.toArray(new SqlNode[operands.size()]),
            asNode.getParserPosition());
        if (asNode.getOperandList().get(0) instanceof SqlDynamicParam) {
            SqlNode identifier = buildForIdentifier((SqlDynamicParam) asNode.getOperandList().get(0));
            asNodeCopy.setOperand(0, identifier);
        } else if (asNode.getOperandList().get(0) instanceof SqlCall) {
            SqlNode firstOp = visit((SqlCall) asNode.getOperandList().get(0));
            asNodeCopy.setOperand(0, firstOp);
        }

        return asNodeCopy;
    }

    private SqlIdentifier buildForIdentifier(SqlDynamicParam dynamicParam) {
        String tableName = tableNames.get(TABLE_NAME_PARAM_INDEX).replaceAll("`", "");
        SqlIdentifier tableIdentifier = new SqlIdentifier(tableName, dynamicParam.getParserPosition());
        TABLE_NAME_PARAM_INDEX++;
        return tableIdentifier;
    }

    private SqlNode buildTargetTable(SqlNode targetTable) {
        if (targetTable.getKind() == SqlKind.DYNAMIC_PARAM) {
            return buildForIdentifier((SqlDynamicParam) targetTable);
        } else if (targetTable.getKind() == SqlKind.AS) {
            return buildAsNode((SqlCall) targetTable);
        } else {
            throw new UnsupportedOperationException("Unsupported syntax.");
        }
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    private SqlNode buildInsertNode(SqlNode oldNode) {
        SqlInsert oldInsert = (SqlInsert) oldNode;

        List<SqlNode> operandList = oldInsert.getOperandList();
        SqlNode targetTable = buildTargetTable(oldInsert.getTargetTable());
        SqlNode source = operandList.get(2);
        if (source instanceof SqlCall) {
            source = visit((SqlCall) source);
        }
        SqlInsert sqlInsert = null;
        if (oldInsert.getKind() == SqlKind.INSERT) {
            sqlInsert = new SqlInsert(oldInsert.getParserPosition(),
                (SqlNodeList) operandList.get(0),
                targetTable,
                source,
                (SqlNodeList) operandList.get(3),
                (SqlNodeList) operandList.get(4),
                oldInsert.getBatchSize(),
                oldInsert.getHints());
        } else {
            sqlInsert = new SqlReplace(oldInsert.getParserPosition(),
                (SqlNodeList) operandList.get(0),
                targetTable,
                source,
                (SqlNodeList) operandList.get(3),
                oldInsert.getBatchSize(),
                oldInsert.getHints());
        }
        return sqlInsert;
    }
}
