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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta.IndexType;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.ScalarSubQueryExecContext;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdIndex;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAsOfOperator;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexHint;
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
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql2rel.SqlToRelConverter.HintBlackboard;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author xiaoying 2018-09-28 15:52
 */
public abstract class ReplaceTableNameWithSomethingVisitor extends SqlShuttle {

    private final static Logger logger = LoggerFactory.getLogger(PlannerUtils.class);

    public final static int TABLE_NAME_PARAM_INDEX = -1;

    protected List<String> tableNames = new ArrayList<>();

    protected Map<RexFieldAccess, RexNode> correlateFieldInViewMap;
    //protected Map<Integer, Object> scalarSubqueryValsMap;
    protected Map<Integer, ScalarSubQueryExecContext> scalarSubqueryExecCtxMap;

    protected final boolean handleIndexHint;

    protected final HintBlackboard hintBlackboard = new HintBlackboard();

    protected boolean clearTableNames = false;

    protected boolean withLock = false;

    protected SqlKind sqlKind = SqlKind.SELECT;

    protected boolean indexScan = false;

    private final String defaultSchemaName;

    protected final ExecutionContext ec;

    protected ReplaceTableNameWithSomethingVisitor(Map<RexFieldAccess, RexNode> correlateFieldInViewMap,
                                                   String defaultSchemaName, ExecutionContext ec) {
        this.handleIndexHint = false;
        this.defaultSchemaName = defaultSchemaName;
        this.ec = ec;
        this.correlateFieldInViewMap = correlateFieldInViewMap;
        this.scalarSubqueryExecCtxMap = ec.getScalarSubqueryCtxMap();
    }

    protected ReplaceTableNameWithSomethingVisitor(String defaultSchemaName, boolean handleIndexHint,
                                                   ExecutionContext ec) {
        this.defaultSchemaName = defaultSchemaName;
        this.handleIndexHint = handleIndexHint;
        this.ec = ec;
        this.correlateFieldInViewMap = ec.getCorrelateFieldInViewMap();
        this.scalarSubqueryExecCtxMap = ec.getScalarSubqueryCtxMap();
    }

    protected ReplaceTableNameWithSomethingVisitor(String defaultSchemaName, ExecutionContext ec) {
        this.defaultSchemaName = defaultSchemaName;
        this.handleIndexHint = false;
        this.ec = ec;
        this.correlateFieldInViewMap = ec.getCorrelateFieldInViewMap();
        this.scalarSubqueryExecCtxMap = ec.getScalarSubqueryCtxMap();
    }

    @Override
    public SqlNode visit(SqlCall call) {
        SqlKind kind = call.getKind();
        if (kind == SqlKind.SELECT) {
            this.sqlKind = kind;

            SqlCall copy = SqlNode.clone(call);
            SqlSelect select = (SqlSelect) copy;

            if (select instanceof TDDLSqlSelect) {
                this.withLock |= ((TDDLSqlSelect) call).withLock();
            }
            this.hintBlackboard.beginSelect(select);

            /**
             * Replace tableName at Select.
             */
            if (select.getSelectList() != null) {
                select.setSelectList((SqlNodeList) visit(select.getSelectList()));
            }

            /**
             * Replace tableName at From.
             */
            final SqlNode from = select.getFrom();
            try {
                convertFrom(select, from);
            } finally {
                this.hintBlackboard.endFrom();
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

            /**
             * order
             */
            SqlNode orderList = select.getOrderList();
            if (orderList != null) {
                select.setOrderBy((SqlNodeList) visit((SqlNodeList) orderList));
            }

            return select;
        }

        if (kind == SqlKind.DELETE) {
            this.sqlKind = kind;

            final SqlDelete delete = (SqlDelete) SqlNode.clone(call);

            if (delete.singleTable()) {
                if (delete.getTargetTable().getKind() == SqlKind.IDENTIFIER) {
                    // Single table delete
                    final SqlIdentifier identifier = (SqlIdentifier) delete.getTargetTable();
                    if (!addAliasForDelete(delete)) {
                        tableNames.add(Util.last(identifier.names));
                        delete.setOperand(0, buildSth(identifier));
                    } else {
                        delete.setOperand(0, buildSth(identifier));
                        final SqlCall identifierWithAlias = buildForIdentifier(identifier);
                        delete.setFrom(identifierWithAlias);
                        delete.setOperand(9, SqlNodeList.of(identifier));
                        delete.initTableInfo(delete.getSourceTables(), SqlNodeList.of(identifierWithAlias),
                            delete.getSubQueryTableMap(), true);
                    }
                    clearTableNames = delete.getSubQueryTableMap().isEmpty();
                }
            } else {
                /**
                 * Multi table delete
                 */
                final SqlNode from = delete.getSourceTableNode();
                SqlNode newFrom = from;
                if (from != null) {
                    SqlKind fromKind = from.getKind();
                    // 单表查询
                    if (fromKind == SqlKind.IDENTIFIER) {
                        SqlIdentifier identifier = (SqlIdentifier) from;
                        newFrom = buildForIdentifier(identifier);
                    } else if (fromKind == SqlKind.AS) {
                        newFrom = buildAsNode(from);
                    } else if (fromKind == SqlKind.JOIN) {
                        // 多表JOIN
                        newFrom = visit((SqlJoin) from);
                    }
                }
                if (delete.fromUsing()) {
                    if (newFrom != from) {
                        delete.setUsing(newFrom);
                    }
                } else {
                    if (newFrom != from) {
                        delete.setFrom(newFrom);
                    }
                }
            }

            // Replace table-refers on WHERE condition
            if (delete.getCondition() instanceof SqlCall) {
                delete.setCondition(visit((SqlCall) delete.getCondition()));
            }

            // Replace table-refers on ORDER BY column
            if (delete.getOrderList() != null && delete.getOrderList().size() > 0) {
                delete.setOperand(4, visit(delete.getOrderList()));
            }

            return delete;
        }

        if (kind == SqlKind.UPDATE) {
            this.sqlKind = kind;

            final SqlUpdate update = (SqlUpdate) SqlNode.clone(call);
            final SqlNode targetTable = update.getTargetTable();
            if (update.singleTable()) {
                if (update.getAlias() != null) {
                    final SqlIdentifier identifier = (SqlIdentifier) targetTable;
                    tableNames.add(Util.last(identifier.names));
                    update.setTargetTable(buildSth(targetTable));
                } else if (targetTable.getKind() == SqlKind.AS) {
                    update.setTargetTable(buildAsNode(targetTable));
                } else if (targetTable.getKind() == SqlKind.IDENTIFIER) {
                    final SqlIdentifier identifier = (SqlIdentifier) targetTable;
                    update.setTargetTable(buildForIdentifier(identifier));
                } else if (targetTable instanceof SqlDynamicParam) {
                } else {
                    throw new UnsupportedOperationException("Unsupported update syntax.");
                }
            } else {
                /**
                 * Multi table update
                 */
                final SqlKind targetKind = targetTable.getKind();
                // 单表查询
                if (targetKind == SqlKind.IDENTIFIER) {
                    final SqlIdentifier identifier = (SqlIdentifier) targetTable;
                    update.setTargetTable(buildForIdentifier(identifier));
                } else if (targetKind == SqlKind.AS) {
                    update.setTargetTable(buildAsNode(targetTable));
                } else if (targetKind == SqlKind.JOIN) {
                    // 多表JOIN
                    update.setTargetTable(visit((SqlJoin) targetTable));
                }
            }

            // Replace table-refers on WHERE condition
            if (update.getCondition() instanceof SqlCall) {
                update.setCondition(visit((SqlCall) update.getCondition()));
            }

            // Replace table-refers on ORDER BY column
            if (update.getOrderList() != null && update.getOrderList().size() > 0) {
                update.setOperand(6, visit(update.getOrderList()));
            }

            // Replace table-refers on SET column
            if (update.getSourceExpressionList() != null && update.getSourceExpressionList().size() > 0) {
                update.setOperand(2, visit(update.getSourceExpressionList()));
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
            if (targetTable.getKind() == SqlKind.IDENTIFIER) {
                tableNames.add(Util.last(((SqlIdentifier) targetTable).names));
                ddl.setTargetTable(buildSth(targetTable));
            } else if (targetTable instanceof SqlDynamicParam) {
            } else {
                throw new UnsupportedOperationException("Unsupported DDL syntax.");
            }
            return ddl;
        } else if (kind == SqlKind.RENAME_TABLE) {
            this.sqlKind = kind;

            SqlDdl ddl = (SqlDdl) call;
            SqlNode targetTable = ddl.getTargetTable();
            if (targetTable.getKind() == SqlKind.IDENTIFIER) {
                tableNames.add(Util.last(((SqlIdentifier) targetTable).names));
                ddl.setTargetTable(buildSth(targetTable));
            } else if (targetTable instanceof SqlDynamicParam) {
            } else {
                throw new UnsupportedOperationException("Unsupported DDL syntax.");
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

        SqlNode rs = super.visit(call);
        if (rs instanceof SqlCall && ((SqlCall) rs).getOperandList().size() == 2) {
            for (SqlNode op : ((SqlCall) rs).getOperandList()) {
                if (rowWithEmpty(op) || emptyLiteral(op)) {
                    if (call.getKind() == SqlKind.IN) {
                        return SqlLiteral.createBoolean(false, rs.getParserPosition());
                    } else if (call.getKind() == SqlKind.NOT_IN) {
                        return SqlLiteral.createBoolean(true, rs.getParserPosition());
                    } else {
                        return SqlLiteral.createNull(rs.getParserPosition());
                    }

                }
            }
        } else if (rs instanceof SqlCall && rs.getKind() == SqlKind.ROW
            && ((SqlCall) rs).getOperandList().size() == 1) {
            SqlNode l = ((SqlCall) rs).getOperandList().get(0);

            if (l instanceof SqlDynamicParam && scalarSubqueryExecCtxMap != null) {
                ScalarSubQueryExecContext ctx = scalarSubqueryExecCtxMap.get(((SqlDynamicParam) l).getDynamicKey());
                if (ctx != null && ctx.getSubQueryResult() != null && (ctx.getSubQueryResult() instanceof Collection)) {
                    // change list type objects to sqlnodelist
                    Collection collection = (Collection) ctx.getSubQueryResult();
                    SqlNode[] list = new SqlNode[collection.size()];
                    int j = 0;
                    for (Object o : collection) {
                        list[j++] = ((SqlDynamicParam) l).getTypeName().createLiteral(o, l.getParserPosition());
                    }
                    return new SqlBasicCall(((SqlCall) rs).getOperator(), list, rs.getParserPosition());
                }
            }

//            if (l instanceof SqlDynamicParam
//                && scalarSubqueryValsMap != null
//                && scalarSubqueryValsMap.get(((SqlDynamicParam) l).getDynamicKey()) != null
//                && scalarSubqueryValsMap.get(((SqlDynamicParam) l).getDynamicKey()) instanceof Collection) {
//                // change list type objects to sqlnodelist
//                Collection collection = (Collection) scalarSubqueryValsMap.get(((SqlDynamicParam) l).getDynamicKey());
//                SqlNode[] list = new SqlNode[collection.size()];
//                int j = 0;
//                for (Object o : collection) {
//                    list[j++] = ((SqlDynamicParam) l).getTypeName().createLiteral(o, l.getParserPosition());
//                }
//                return new SqlBasicCall(((SqlCall) rs).getOperator(), list, rs.getParserPosition());
//            }
        }
        return rs;
    }

    private boolean rowWithEmpty(SqlNode op) {
        return op != null && op instanceof SqlCall && op.getKind() == SqlKind.ROW
            && ((SqlCall) op).getOperandList().size() == 1
            && ((SqlCall) op).getOperandList().get(0) instanceof SqlLiteral
            && ((SqlLiteral) ((SqlCall) op).getOperandList().get(0)).getValue()
            == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY;
    }

    private boolean emptyLiteral(SqlNode op) {
        return op != null && op instanceof SqlLiteral
            && ((SqlLiteral) op).getValue() == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY;
    }

    public void convertFrom(SqlSelect select, SqlNode from) {
        if (null == from) {
            return;
        }

        SqlKind fromKind = from.getKind();
        // 单表查询
        if (fromKind == SqlKind.IDENTIFIER) {
            SqlIdentifier identifier = (SqlIdentifier) from;
            select.setFrom(buildForIdentifier(identifier));
        } else if (fromKind == SqlKind.AS) {
            final SqlCall as = (SqlCall) from;
            this.hintBlackboard.beginAlias(as.operand(0), as.operand(1));
            try {
                select.setFrom(buildAsNode(as));
            } finally {
                this.hintBlackboard.endAlias();
            }
        } else if (fromKind == SqlKind.JOIN) {
            // 多表JOIN
            select.setFrom(visit((SqlJoin) from));
        } else if (fromKind == SqlKind.SELECT) {
            select.setFrom(visit((SqlCall) from));
        }
    }

    @Override
    public SqlNode visit(SqlLiteral literal) {
        return SqlNode.clone(literal);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        // Removing tableName for DELETE
        List<String> names = id.names;
        if (clearTableNames && names.size() == 2) {
            return new SqlIdentifier(names.get(1), id.getParserPosition());
        }
        return SqlNode.clone(id);
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
        return SqlNode.clone(type);
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
        if (param.getIndex() == -4 && correlateFieldInViewMap != null) {
            return SqlImplementor.buildSqlLiteral((RexLiteral) correlateFieldInViewMap.get(param.getValue()));
        }

        if (param.getIndex() == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX && scalarSubqueryExecCtxMap != null) {
            ScalarSubQueryExecContext ctx = scalarSubqueryExecCtxMap.get(param.getDynamicKey());
            if (ctx != null && ctx.getSubQueryResult() != null) {
                Object sbResult = ctx.getSubQueryResult();
                if (sbResult == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY) {
                    return SqlLiteral.createSymbol(RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY,
                        param.getParserPosition());
                } else if (!(sbResult instanceof Collection)) {
                    return param.getTypeName()
                        .createLiteral(sbResult, param.getParserPosition());
                }
            }
        }

//        if (param.getIndex() == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX && scalarSubqueryValsMap != null
//            && scalarSubqueryValsMap.get(param.getDynamicKey()) != null) {
//            if (scalarSubqueryValsMap.get(param.getDynamicKey()) == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY) {
//                return SqlLiteral.createSymbol(RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY, param.getParserPosition());
//            } else if (!(scalarSubqueryValsMap.get(param.getDynamicKey()) instanceof Collection)) {
//                return param.getTypeName()
//                    .createLiteral(scalarSubqueryValsMap.get(param.getDynamicKey()), param.getParserPosition());
//            }
//        }

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
                sqlJoin.setLeft(buildAsNode(left));
            } else {
                sqlJoin.setLeft(visit((SqlCall) left));
            }
        } else if (left instanceof SqlIdentifier) {
            sqlJoin.setLeft(buildForIdentifier((SqlIdentifier) left));
        }

        /**
         * Right
         */
        SqlNode right = sqlJoin.getRight();
        if (right instanceof SqlCall) {
            if (((SqlCall) right).getOperator() instanceof SqlAsOperator) {
                sqlJoin.setRight(buildAsNode(right));
            } else {
                sqlJoin.setRight(visit((SqlCall) right));
            }
        } else if (right instanceof SqlIdentifier) {
            sqlJoin.setRight(buildForIdentifier((SqlIdentifier) right));
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
            SqlIdentifier idOfLeft = (SqlIdentifier) leftNode;
            int lastNamesIdx = idOfLeft.names.size() - 1;
            if (idOfLeft.names.get(lastNamesIdx).equalsIgnoreCase(SqlFunction.NEXTVAL_FUNC_NAME)) {
                oldNode = buildNextVal(idOfLeft);
            }
            return oldNode;
        } else if (leftNode instanceof SqlCall) {
            leftNode = visit((SqlCall) leftNode);
        } else if (leftNode instanceof SqlDynamicParam) {

        } else if (leftNode instanceof SqlLiteral) {

        } else {
            throw new TddlNestableRuntimeException("should not be here");
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
            if (StringUtils.isEmpty(aliasName)) {
                String errMsg = "The alias name is null for Native Sql";
                logger.error(errMsg);
                throw new OptimizerException(errMsg);
            }
        }

        SqlNode leftNode = call.getOperandList().get(0);
        SqlNode rightNode = null;
        boolean unwrapTablename = false;
        if (leftNode instanceof SqlCall && ((SqlCall) leftNode).getOperator() instanceof SqlAsOfOperator) {
            rightNode = ((SqlCall) leftNode).getOperandList().get(1);
            leftNode = ((SqlCall) leftNode).getOperandList().get(0);
            unwrapTablename = true;
        }
        // 子查询
        if (leftNode instanceof SqlSelect) {
            leftNode = visit((SqlSelect) leftNode);
        } else if (leftNode instanceof SqlIdentifier) {
            SqlNode indexNode = null;
            if (aliasNode instanceof SqlIdentifier) {
                indexNode = ((SqlIdentifier) aliasNode).indexNode;
            }
            if (storeTableOrIndexName((SqlIdentifier) leftNode, indexNode)) {
                // remove force index with gsi
                sqlIdentifier.indexNode = null;
            }
            leftNode = buildFlashback(leftNode, buildSth(leftNode));
        } else if (leftNode instanceof SqlCall) {
            leftNode = visit((SqlCall) leftNode);
        } else if (leftNode instanceof SqlDynamicParam) {

        } else {
            throw new TddlNestableRuntimeException("should not be here");
        }
        if (unwrapTablename) {
            leftNode = new SqlBasicCall(SqlStdOperatorTable.AS_OF, new SqlNode[] {
                leftNode,
                rightNode,
            }, SqlParserPos.ZERO);
        }

        return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, leftNode, sqlIdentifier);
    }

    /**
     * store table names actually used
     *
     * @return true if force index should be removed
     */
    private boolean storeTableOrIndexName(SqlIdentifier tableIdentifier, SqlNode indexNode) {
        final ImmutableList<String> qualifiedName = tableIdentifier.names;
        final String schemaName = qualifiedName.size() == 2 ? qualifiedName.get(0) : defaultSchemaName;

        boolean removeForceIndex = false;
        String tableName = Util.last(qualifiedName);
        if (this.handleIndexHint && !withLock) {
            // handle index hint (FORCE/USE/IGNORE INDEX)
            if (indexNode instanceof SqlNodeList && ((SqlNodeList) indexNode).size() > 0) {
                final SqlIndexHint sqlIndexHint = (SqlIndexHint) ((SqlNodeList) indexNode).get(0);
                final String indexName = GlobalIndexMeta
                    .getIndexName(RelUtils.lastStringValue(sqlIndexHint.getIndexList().get(0)));

                final String unwrapped = GlobalIndexMeta.getGsiWrappedName(tableName, indexName, schemaName, this.ec);
                final String realIndexName = null == unwrapped ? indexName : unwrapped;
                final IndexType indexType = GlobalIndexMeta.getIndexType(tableName, realIndexName, schemaName, this.ec);

                switch (indexType) {
                case PUBLISHED_GSI:
                    this.tableNames.add(realIndexName);
                    this.indexScan = true;
                    return true;
                case UNPUBLISHED_GSI:
                    // Gsi whose table not finished creating
                case NONE:
                    // Gsi is removed but sql not updated or
                    removeForceIndex = true;
                case LOCAL:
                default:
                    break;
                }
            }

            // handle index hint
            final SqlNodeList hints = this.hintBlackboard.currentHints(tableName);

            final List<HintCmdOperator> cmdHints = HintConverter.convertCmd(hints,
                new ArrayList<>(), false, ec).cmdHintResult;

            for (HintCmdOperator hint : cmdHints) {
                if (hint instanceof HintCmdIndex) {
                    final HintCmdIndex indexHint = (HintCmdIndex) hint;
                    final String indexHintTableName = indexHint.tableNameLast();
                    final String indexHintIndexName = indexHint.indexNameLast();

                    if (TStringUtil.equalsIgnoreCase(tableName, indexHintTableName) && GlobalIndexMeta
                        .isPublishedPrimaryAndIndex(indexHintTableName, indexHintIndexName, schemaName, ec)) {
                        // only first index hint works
                        this.tableNames.add(indexHintIndexName);
                        this.indexScan = true;
                        return removeForceIndex;
                    }
                }
            }

        }

        this.tableNames.add(tableName);
        return removeForceIndex;
    }

    protected SqlCall buildForIdentifier(SqlIdentifier identifier) {
        return buildForIdentifier(identifier, sqlKind == SqlKind.SELECT);
    }

    protected SqlCall buildForIdentifier(SqlIdentifier identifier, boolean removeGsiHint) {
        SqlNode indexNode = identifier.indexNode;
        if (removeGsiHint) {
            if (storeTableOrIndexName(identifier, indexNode)) {
                // remove force index with gsi
                indexNode = null;
            }
        } else {
            this.tableNames.add(Util.last(identifier.names));
        }

        SqlNode clone = buildSth(identifier).clone(SqlParserPos.ZERO);
        if (clone instanceof SqlIdentifier) {
            ((SqlIdentifier) clone).indexNode = null;
        }
        clone = buildFlashback(identifier, clone);
        SqlIdentifier asName = new SqlIdentifier(ImmutableList
            .of(Util.last(identifier.names)), null, identifier.getParserPosition(), null, indexNode);
        return SqlStdOperatorTable.AS.createCall(identifier.getParserPosition(), clone, asName);
    }

    protected abstract <T extends SqlNode> T buildSth(SqlNode sqlNode);

    public List<String> getTableNames() {
        return tableNames;
    }

    protected SqlNode buildInsertNode(SqlNode oldNode) {
        SqlInsert oldInsert = (SqlInsert) oldNode;
        if (oldInsert.getTargetTable() instanceof SqlIdentifier) {
            tableNames.add(Util.last(((SqlIdentifier) oldInsert.getTargetTable()).names));
        }

        List<SqlNode> operandList = oldInsert.getOperandList();
        SqlNode targetTable = buildSth(oldInsert.getTargetTable());
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

    protected SqlNode buildNextVal(SqlIdentifier nextVal) {
        return nextVal;
    }

    protected boolean addAliasForDelete(SqlNode delete) {
        return false;
    }

    protected SqlNode buildFlashback(SqlNode origin, SqlNode converted) {
        if (origin instanceof SqlIdentifier && ((SqlIdentifier) origin).flashback != null) {
            return new SqlBasicCall(SqlStdOperatorTable.AS_OF,
                new SqlNode[] {converted, ((SqlIdentifier) origin).flashback}, SqlParserPos.ZERO);
        }
        return converted;
    }
}
