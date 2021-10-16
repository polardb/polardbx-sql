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

package com.alibaba.polardbx.optimizer.core;

import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorTable;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * @author lingce.ldm 2017-12-29 10:29
 */
public class TddlValidator extends SqlValidatorImpl {

    public TddlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
                         RelDataTypeFactory typeFactory, SqlConformance conformance) {
        super(opTab, catalogReader, typeFactory, conformance);
    }

    @Override
    protected void checkFieldCount(SqlNodeList targetColumnList, SqlNode node, SqlValidatorTable table, SqlNode source,
                                   RelDataType logicalSourceRowType, RelDataType logicalTargetRowType) {
        checkFieldCount(targetColumnList, node, logicalSourceRowType, logicalTargetRowType);

        // Auto_increment column
        final Set<String> autoIncrementColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final TableMeta tableMeta = table.unwrap(TableMeta.class);
        if (null != tableMeta) {
            autoIncrementColumns.addAll(tableMeta.getAutoIncrementColumns());
        }

        final List<ColumnStrategy> strategies = table.unwrap(RelOptTable.class).getColumnStrategies();
        for (final RelDataTypeField field : table.getRowType().getFieldList()) {
            final String fieldName = field.getName();
            final RelDataTypeField targetField = logicalTargetRowType.getField(fieldName, true, false);
            switch (strategies.get(field.getIndex())) {
            case NOT_NULLABLE:
                assert !field.getType().isNullable();
                // Do not check here
//                if (targetField == null && !autoIncrementColumns.contains(fieldName) && null != tableMeta) {
//                    final Field column = tableMeta.getColumn(fieldName).getField();
//                    if (null == column.getDefault()) {
//                        throw newValidationError(node, RESOURCE.columnNotNullable(fieldName));
//                    }
//                }
                break;
            case NULLABLE:
                assert field.getType().isNullable();
                break;
            case VIRTUAL:
            case STORED:
                if (targetField != null && !isValuesWithDefault(source, targetField.getIndex())) {
                    throw newValidationError(node, RESOURCE.insertIntoAlwaysGenerated(fieldName));
                }
            default:
                break;
            }
        }
    }

    @Override
    protected void checkNullable(SqlCall node, RelDataType targetRowType, SqlCall rowConstructor) {
        final SqlKind kind = getTop().getKind();
        if (kind == SqlKind.INSERT || kind == SqlKind.REPLACE) {
            return;
        }
        super.checkNullable(node, targetRowType, rowConstructor);
    }
}
