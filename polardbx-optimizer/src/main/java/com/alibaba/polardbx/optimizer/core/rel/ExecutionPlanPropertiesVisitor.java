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

import com.alibaba.polardbx.common.utils.ConcurrentHashSet;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExecutionPlanPropertiesVisitor extends SqlBasicVisitor<SqlNode> {

    private List<String> modifiedTableNames = new LinkedList<>();
    private List<String> tableNames = new LinkedList<>();
    private SqlKind sqlKind;
    private SqlSelect.LockMode lockMode = SqlSelect.LockMode.UNDEF;

    public ExecutionPlanPropertiesVisitor() {
    }

    @Override
    public SqlNode visit(SqlCall call) {
        final SqlKind kind = call.getKind();
        switch (kind) {
        case SELECT:
            updateSqlKind(kind);
            lockMode = ((SqlSelect) call).getLockMode();
            break;
        case DELETE:
            updateSqlKind(kind);
            SqlDelete delete = (SqlDelete) call;
            if (delete.singleTable()) {
                Preconditions.checkArgument(delete.getTargetTable().getKind() == SqlKind.IDENTIFIER);
                final String deleteTable = ((SqlIdentifier) delete.getTargetTable()).getLastName();
                tableNames.add(deleteTable);
                modifiedTableNames.add(deleteTable);
            } else {
                List<String> sourceTables = new ArrayList<>();
                for (SqlNode sourceTable : delete.getSourceTables()) {
                    if (sourceTable instanceof SqlIdentifier) {
                        sourceTables.add(((SqlIdentifier) sourceTable).getLastName());
                    } else {
                        // handle subquery
                        sourceTables.addAll(delete.subQueryTables(sourceTable)
                            .stream()
                            .map(SqlIdentifier::getLastName)
                            .collect(Collectors.toList()));
                    }
                }
                tableNames.addAll(sourceTables);

                final SqlNodeList aliases = delete.getAliases();
                final Map<String, String> aliasTableMap = buildAliasesMap(sourceTables, aliases);

                for (SqlNode targetTable : delete.getTargetTables()) {
                    final String alias = ((SqlIdentifier) targetTable).getLastName();
                    if (aliasTableMap.containsKey(alias)) {
                        modifiedTableNames.add(aliasTableMap.get(alias));
                    }
                }
            }
            return call;
        case UPDATE:
            updateSqlKind(kind);
            SqlUpdate update = (SqlUpdate) call;
            if (update.singleTable()) {
                SqlNode targetTable = unwrapAs(update.getTargetTable());
                if (targetTable.getKind() == SqlKind.IDENTIFIER) {
                    final String updateTable = ((SqlIdentifier) targetTable).getLastName();
                    tableNames.add(Util.last(((SqlIdentifier) targetTable).names));
                    modifiedTableNames.add(updateTable);
                }
            } else {
                for (SqlNode sourceTable : update.getSourceTables()) {
                    if (sourceTable instanceof SqlIdentifier) {
                        tableNames.add(((SqlIdentifier) sourceTable).getLastName());
                    } else if (sourceTable instanceof SqlSelect) {
                        // handle subquery
                        tableNames.addAll(update.subQueryTables(sourceTable)
                            .stream()
                            .map(SqlIdentifier::getLastName)
                            .collect(Collectors.toList()));
                    }
                }

                modifiedTableNames.addAll(getModifiedTables(update));
            }
            return call;
        case INSERT:
        case REPLACE:
            updateSqlKind(kind);
            SqlInsert insert = (SqlInsert) call;
            final String insertTable = ((SqlIdentifier) insert.getTargetTable()).getLastName();
            tableNames.add(insertTable);
            modifiedTableNames.add(insertTable);
            return call;
        default:
            return super.visit(call);
        }

        return super.visit(call);
    }

    public ConcurrentHashSet<Integer> appendPlanProperties(ConcurrentHashSet<Integer> old,
                                                           String schemaName,
                                                           ExecutionContext ec) {
        final ConcurrentHashSet<Integer> result = (old == null ? new ConcurrentHashSet<>() : old);

        if (null != sqlKind && sqlKind.belongsTo(SqlKind.DML)) {
            Map<String, RelUtils.TableProperties> tableProperties =
                RelUtils.buildTablePropertiesMap(this.tableNames, schemaName, ec);
            if (RelUtils.containsBroadcastTable(tableProperties, this.modifiedTableNames)) {
                result.add(ExecutionPlanProperties.MODIFY_BROADCAST_TABLE);
            }
            if (RelUtils.containsGsiTable(tableProperties, this.modifiedTableNames)) {
                result.add(ExecutionPlanProperties.MODIFY_GSI_TABLE);
            }
            if (RelUtils.containsReplicateWriableTable(tableProperties, this.modifiedTableNames, ec)) {
                result.add(ExecutionPlanProperties.REPLICATE_TABLE);
            }
        }

        return result;
    }

    private SqlNode unwrapAs(SqlNode as) {
        if (as.getKind() != SqlKind.AS) {
            return as;
        }

        final SqlBasicCall call = (SqlBasicCall) as;
        return call.getOperandList().get(0);
    }

    private void updateSqlKind(SqlKind sqlKind) {
        if (null == this.sqlKind) {
            this.sqlKind = sqlKind;
        }
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public List<String> getModifiedTableNames() {
        return modifiedTableNames;
    }

    private Set<String> getModifiedTables(SqlUpdate call) {
        List<String> targetTables = new ArrayList<>();
        for (SqlNode targetTable : call.getSourceTables()) {
            if (targetTable instanceof SqlIdentifier) {
                targetTables.add(((SqlIdentifier) targetTable).getLastName());
            } else {
                // skip subquery
                targetTables.add("");
            }
        }

        /**
         * map column to table it belongs to
         */
        final SqlNodeList aliases = call.getAliases();
        final Map<String, String> aliasTableMap = buildAliasesMap(targetTables, aliases);

        Set<String> modifiedTableNames = new HashSet<>();
        for (SqlNode node : call.getTargetColumnList()) {
            final SqlIdentifier id = (SqlIdentifier) node;
            if (id.names.size() == 2) {
                /**
                 * UPDATE t1 a , t2 b SET a.id = 1
                 */
                final String relTable = aliasTableMap.get(id.names.get(0));
                if (null == relTable) {
                    throw new AssertionError("unknown table name: " + id.names.get(0));
                }

                modifiedTableNames.add(relTable);
            } else {
                /**
                 * UPDATE t1 a , t2 b SET id = 1
                 */
                modifiedTableNames.addAll(aliasTableMap.values());
                break;
            }
        }

        return modifiedTableNames;
    }

    private Map<String, String> buildAliasesMap(List<String> sourceTables, SqlNodeList aliases) {
        final Map<String, String> aliasTableMap = new HashMap<>();
        int targetIndex = 0;
        for (SqlNode alias : aliases) {
            final String aliasString = SqlValidatorUtil.getAlias(alias, -1);
            final boolean subqueryWithAlias = alias.getKind() == SqlKind.AS
                && !(((SqlCall) alias).getOperandList().get(0) instanceof SqlIdentifier);
            if (subqueryWithAlias) {
                // ignore subquery
            } else {
                aliasTableMap.put(aliasString, sourceTables.get(targetIndex++));
            }
        }
        return aliasTableMap;
    }


    public SqlKind getSqlKind() {
        return sqlKind;
    }

    public SqlSelect.LockMode getLockMode() {
        return lockMode;
    }
}
