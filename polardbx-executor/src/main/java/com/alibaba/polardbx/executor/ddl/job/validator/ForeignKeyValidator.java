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

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.utils.DdlCharsetInfo;
import com.alibaba.polardbx.optimizer.utils.DdlCharsetInfoUtil;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ForeignKeyValidator {

    public static void validateFkConstraints(SqlCreateTable sqlCreateTableOrigin, String schemaName, String tableName,
                                             ExecutionContext executionContext) {
        SqlCreateTable sqlCreateTable = getCharsetAndCollation(sqlCreateTableOrigin, schemaName, executionContext);

        final boolean checkForeignKey =
            executionContext.foreignKeyChecks();

        Map<String, SqlColumnDeclaration> colDefMap = sqlCreateTable.getColDefs()
            .stream()
            .collect(Collectors.toMap(p -> p.getKey().getLastName(),
                Pair::getValue,
                (x, y) -> y,
                TreeMaps::caseInsensitiveMap));

        Set<String> generatedReferencedColumns = new TreeSet<>(String::compareToIgnoreCase);
        for (SqlColumnDeclaration col : colDefMap.values()) {
            if (col.isGeneratedAlways()) {
                generatedReferencedColumns.add(col.toString());
                SqlCall expr = col.getGeneratedAlwaysExpr();
                GeneratedColumnUtil.validateGeneratedColumnExpr(expr);
                generatedReferencedColumns.addAll(GeneratedColumnUtil.getReferencedColumns(expr));
            }
        }

        Set<String> constraintNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (ForeignKeyData data : sqlCreateTable.getAddedForeignKeys()) {
            // engine must be innodb
            if (Engine.isFileStore(sqlCreateTable.getEngine())) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_ENGINE, sqlCreateTable.getEngine().toString());
            }

            // constraint name cannot be same
            if (constraintNames.contains(data.constraint)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_NAME_FK_CONSTRAINT, data.constraint);
            }
            constraintNames.add(data.constraint);

            LimitValidator.validateDatabaseNameLength(data.refSchema);
            LimitValidator.validateTableNameLength(data.refTableName);
            LimitValidator.validateConstraintNameLength(data.constraint);
            data.refColumns.forEach(LimitValidator::validateColumnNameLength);

            TableMeta referringTableMeta =
                executionContext.getSchemaManager(data.refSchema).getTableWithNull(data.refTableName);

            // fk columns must exist in table
            for (String column : data.columns) {
                if (!colDefMap.containsKey(column)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                        String.format("Column `%s` not exist in table `%s`.", column, data.tableName));
                }
            }

            if (checkForeignKey) {
                // ref Table must exists
                boolean referencingTableExists = TableValidator.checkIfTableExists(data.refSchema, data.refTableName);
                if (!referencingTableExists && !data.refTableName.equalsIgnoreCase(tableName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, data.refSchema, data.refTableName);
                }
            }

            // ref table fk column index must exist
            validateAddReferredTableFkIndex(data, executionContext, tableName, sqlCreateTable);

            // columns and referenced table columns size must same
            if (data.refColumns.size() != data.columns.size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                    String.format("Table '%s'.'%s' and '%s'.'%s' foreign key columns size must be same", schemaName,
                        tableName, data.refSchema, data.refTableName));
            }

            // innodb do not support set default
            if (data.onUpdate == ForeignKeyData.ReferenceOptionType.SET_DEFAULT ||
                data.onDelete == ForeignKeyData.ReferenceOptionType.SET_DEFAULT) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT, "Do not support SET DEFAULT");
            }

            // fk columns can not be null or primary key when option is set null
            if (data.onUpdate == ForeignKeyData.ReferenceOptionType.SET_NULL ||
                data.onDelete == ForeignKeyData.ReferenceOptionType.SET_NULL) {
                for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : sqlCreateTable.getColDefs()) {
                    if (data.columns.stream().anyMatch(colDef.getKey().getLastName()::equalsIgnoreCase) && (
                        colDef.getValue().getNotNull() != SqlColumnDeclaration.ColumnNull.NULL
                            && colDef.getValue().getNotNull() != null)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                            "Foreign key columns can not be NULL when option is SET NULL");
                    }
                }
                for (String column : data.columns) {
                    if (sqlCreateTable.getPrimaryKey() != null && sqlCreateTable.getPrimaryKey().getColumns().stream()
                        .anyMatch(c -> c.getColumnNameStr().equalsIgnoreCase(column))) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                            "Foreign key columns can not be PRIMARY KEY when option is SET NULL");
                    }
                }
            }

            // BLOB/TEXT can not be fk columns
            for (String column : data.columns) {
                SqlDataTypeSpec.DrdsTypeName columnTypeName =
                    SqlDataTypeSpec.DrdsTypeName.from(
                        colDefMap.get(column).getDataType().getTypeName().toString().toUpperCase());
                if (columnTypeName.equals(SqlDataTypeSpec.DrdsTypeName.BLOB) || columnTypeName.equals(
                    SqlDataTypeSpec.DrdsTypeName.TEXT)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                        "Foreign key columns do not support BLOB/TEXT");
                }
            }

            // can not add fk on generated column
            for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(
                sqlCreateTable.getColDefs())) {
                if (data.columns.stream().anyMatch(colDef.getKey().getLastName()::equalsIgnoreCase) && (
                    colDef.getValue().isGeneratedAlways()
                        || colDef.getValue().isGeneratedAlwaysLogical())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_GENERATED_COLUMN,
                        colDef.getKey().getLastName());
                }
                if (data.refTableName.equalsIgnoreCase(tableName)) {
                    if (data.refColumns.stream().anyMatch(colDef.getKey().getLastName()::equalsIgnoreCase) && (
                        colDef.getValue().isGeneratedAlways()
                            || colDef.getValue().isGeneratedAlwaysLogical())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_GENERATED_COLUMN,
                            colDef.getKey().getLastName());
                    }
                }
            }

            Map<String, String> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (int i = 0; i < data.columns.size(); i++) {
                columnMap.put(data.refColumns.get(i), data.columns.get(i));
            }

            // self reference
            if (data.refTableName.equalsIgnoreCase(tableName)) {
                // columns and referenced columns type must be similar
                for (Map.Entry<String, String> entry : columnMap.entrySet()) {
                    SqlDataTypeSpec.DrdsTypeName columnTypeNameLeft = SqlDataTypeSpec.DrdsTypeName.from(
                        colDefMap.get(entry.getKey()).getDataType().getTypeName().getLastName()
                            .toUpperCase());
                    SqlDataTypeSpec.DrdsTypeName columnTypeNameRight = SqlDataTypeSpec.DrdsTypeName.from(
                        colDefMap.get(entry.getValue()).getDataType().getTypeName().getLastName()
                            .toUpperCase());
                    if (!columnTypeNameLeft.equals(columnTypeNameRight)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_CHANGE_COLUMN_FK_CONSTRAINT,
                            entry.getKey(), schemaName, tableName, entry.getKey(), data.refSchema,
                            data.refTableName, data.constraint);
                    }
                }
            }

            if (referringTableMeta == null) {
                continue;
            }

            for (String column : data.refColumns) {
                if (referringTableMeta != null && !referringTableMeta.getAllColumns().stream()
                    .map(c -> c.getName().toLowerCase()).collect(Collectors.toList())
                    .contains(column.toLowerCase())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                        String.format("Column `%s` not exist in table `%s`.", column, data.refTableName));
                }
            }

            if (!data.refTableName.equalsIgnoreCase(tableName)) {
                // charset and collation must be same

                if (!StringUtils.equalsIgnoreCase(sqlCreateTable.getDefaultCharset(),
                    referringTableMeta.getDefaultCharset())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CHARSET_COLLATION,
                        schemaName, tableName, data.refSchema, data.refTableName);
                }
                if (!StringUtils.equalsIgnoreCase(sqlCreateTable.getDefaultCollation(),
                    referringTableMeta.getDefaultCollation())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CHARSET_COLLATION,
                        schemaName, tableName, data.refSchema, data.refTableName);
                }
            }

            // engine must be innodb
            if (Engine.isFileStore(referringTableMeta.getEngine())) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_ENGINE, referringTableMeta.getEngine().toString());
            }

            // columns and referenced columns type must be similar
            for (ColumnMeta column : referringTableMeta.getAllColumns()) {
                if (data.refColumns.stream().anyMatch(column.getName()::equalsIgnoreCase)) {
                    SqlColumnDeclaration def = colDefMap.get(columnMap.get(column.getName()));

                    SqlDataTypeSpec.DrdsTypeName columnTypeName = SqlDataTypeSpec.DrdsTypeName.from(
                        def.getDataType().getTypeName().getLastName()
                            .toUpperCase());

                    String charSetName = def.getDataType().getCharSetName();
                    String collationName =
                        def.getDataType().getCollationName();

                    RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
                    boolean nullable =
                        Optional.ofNullable(def.getNotNull()).map(cn -> SqlColumnDeclaration.ColumnNull.NULL == cn)
                            .orElse(true);
                    RelDataType type = def.getDataType().deriveType(factory, nullable);

                    if (charSetName == null && SqlTypeUtil.inCharFamily(type)) {
                        charSetName = sqlCreateTable.getDefaultCharset();
                        collationName = sqlCreateTable.getDefaultCollation();
                    }

                    if (!columnTypeName.equals(
                        SqlDataTypeSpec.DrdsTypeName.from(column.getDataType().getStringSqlType().toUpperCase()))) {
                        throw new TddlRuntimeException(ErrorCode.ERR_CHANGE_COLUMN_FK_CONSTRAINT,
                            column.getName(), schemaName, tableName, columnMap.get(column.getName()), data.refSchema,
                            data.refTableName, data.constraint);
                    }

                    if (charSetName != null && !StringUtils.equalsIgnoreCase(charSetName,
                        column.getDataType().getCharsetName().name())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CHARSET_COLLATION,
                            schemaName, tableName, data.refSchema, data.refTableName);
                    }

                    if (collationName != null && !StringUtils.equalsIgnoreCase(collationName,
                        column.getDataType().getCollationName().name())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CHARSET_COLLATION,
                            schemaName, tableName, data.refSchema, data.refTableName);
                    }

                    // can not add fk on generated column
                    if (column.isGeneratedColumn()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_GENERATED_COLUMN, column.getName());
                    }
                }
            }

            // can not add fk on generated column
            Set<String> refGeneratedReferencedColumns = new TreeSet<>(String::compareToIgnoreCase);
            refGeneratedReferencedColumns.addAll(
                GeneratedColumnUtil.getAllReferencedColumnByRef(referringTableMeta).keySet());
            GeneratedColumnUtil.getAllReferencedColumnByRef(referringTableMeta).values()
                .forEach(r -> refGeneratedReferencedColumns.addAll(r));
            for (String column : data.refColumns) {
                if (refGeneratedReferencedColumns.contains(column)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_GENERATED_COLUMN, column);
                }
            }

            for (String column : data.columns) {
                if (generatedReferencedColumns.contains(column)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_GENERATED_COLUMN, column);
                }
            }
        }
    }

    public static void validateFkConstraints(SqlAlterTable sqlAlterTable, String schemaName, String tableName,
                                             ExecutionContext executionContext) {
        // Even disable foreign_key_checks, you still can't drop the index used for foreign key constrain.
        if (sqlAlterTable.getAlters().size() == 1 && sqlAlterTable.getAlters()
            .get(0) instanceof SqlAlterTableDropIndex) {
            SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) sqlAlterTable.getAlters().get(0);
            TableMeta tableMeta =
                executionContext.getSchemaManager(schemaName).getTable(dropIndex.getTableName().toString());
            if (tableMeta.getReferencedForeignKeys().isEmpty()) {
                return;
            }

            validateDropReferredTableFkIndex(tableMeta, dropIndex.getIndexName().getLastName());
        }

        final boolean checkForeignKey =
            executionContext.foreignKeyChecks();
        if (sqlAlterTable.getAlters().size() != 1 || (!(sqlAlterTable.getAlters()
            .get(0) instanceof SqlAddForeignKey) && !(sqlAlterTable.getAlters().get(0) instanceof SqlModifyColumn))) {
            return;
        }

        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

        // modify partition key
        if (sqlAlterTable.getAlters().get(0) instanceof SqlModifyColumn) {
            SqlModifyColumn alterItem = (SqlModifyColumn) sqlAlterTable.getAlters().get(0);
            Map<String, ForeignKeyData> referencedColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (ForeignKeyData data : tableMeta.getReferencedForeignKeys().values()) {
                for (String refColumn : data.refColumns) {
                    referencedColumns.put(refColumn, data);
                }
            }
            Map<String, ForeignKeyData> referencingColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (ForeignKeyData data : tableMeta.getForeignKeys().values()) {
                for (String refColumn : data.columns) {
                    referencingColumns.put(refColumn, data);
                }
            }
            Map<String, SqlDataTypeSpec.DrdsTypeName> columnsBeforeDdlType =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            tableMeta.getAllColumns().forEach(c -> columnsBeforeDdlType.put(c.getName(),
                SqlDataTypeSpec.DrdsTypeName.from(c.getDataType().getStringSqlType().toUpperCase())));
            String columnName = alterItem.getColName().getSimple();
            if (referencedColumns.containsKey(columnName)) {
                checkColumnType(alterItem, columnsBeforeDdlType, columnName, referencedColumns, true);
            }
            if (referencingColumns.containsKey(columnName)) {
                checkColumnType(alterItem, columnsBeforeDdlType, columnName, referencingColumns, false);
            }
            return;
        }

        ForeignKeyData data = ((SqlAddForeignKey) sqlAlterTable.getAlters().get(0)).getForeignKeyData();

        // engine must be innodb
        if (Engine.isFileStore(tableMeta.getEngine())) {
            throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_ENGINE, tableMeta.getEngine().toString());
        }

        if (data.constraint == null) {
            data.constraint = ForeignKeyUtils.getForeignKeyConstraintName(schemaName, tableName);
        }
        Set<String> constraintNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        constraintNames.addAll(
            tableMeta.getForeignKeys().values().stream().map(c -> c.constraint).collect(Collectors.toList()));
        // constraint name cannot be same
        if (constraintNames.contains(data.constraint)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_NAME_FK_CONSTRAINT, data.constraint);
        }

        if (checkForeignKey) {
            // ref Table must exists
            boolean referencingTableExists = TableValidator.checkIfTableExists(data.refSchema, data.refTableName);
            if (!referencingTableExists && !data.refTableName.equalsIgnoreCase(tableName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, data.refSchema, data.refTableName);
            }
        }

        LimitValidator.validateDatabaseNameLength(data.refSchema);
        LimitValidator.validateTableNameLength(data.refTableName);
        LimitValidator.validateConstraintNameLength(data.constraint);
        data.refColumns.forEach(LimitValidator::validateColumnNameLength);

        // ref table fk column index must exist
        validateAddReferredTableFkIndex(data, executionContext, tableName, null);

        // columns and referenced table columns size must same
        if (data.refColumns.size() != data.columns.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                String.format("Table '%s'.'%s' and '%s'.'%s' foreign key columns size must be same", schemaName,
                    tableName, data.refSchema, data.refTableName));
        }

        // innodb do not support set default
        if (data.onUpdate == ForeignKeyData.ReferenceOptionType.SET_DEFAULT ||
            data.onDelete == ForeignKeyData.ReferenceOptionType.SET_DEFAULT) {
            throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT, "Do not support SET DEFAULT");
        }

        // engine must be innodb
        if (Engine.isFileStore(tableMeta.getEngine())) {
            throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_ENGINE, tableMeta.getEngine().toString());
        }

        // fk columns must exist in table
        for (String column : data.columns) {
            if (!tableMeta.getAllColumns().stream().map(c -> c.getName().toLowerCase()).collect(Collectors.toList())
                .contains(column.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                    String.format("Column `%s` not exist in table `%s`.", column, data.tableName));
            }
        }

        // fk columns can not be null or primary key when option is set null
        if (data.onUpdate == ForeignKeyData.ReferenceOptionType.SET_NULL ||
            data.onDelete == ForeignKeyData.ReferenceOptionType.SET_NULL) {
            for (String column : data.columns) {
                ColumnMeta columnMeta = tableMeta.getColumn(column);
                if (!columnMeta.isNullable()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                        "Foreign key columns can not be NULL when option is SET NULL");
                }
                if (columnMeta.getField().isPrimary()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                        "Foreign key columns can not be PRIMARY KEY when option is SET NULL");
                }
            }
        }

        // BLOB/TEXT can not be fk columns
        for (String column : data.columns) {
            SqlDataTypeSpec.DrdsTypeName columnTypeName = SqlDataTypeSpec.DrdsTypeName.from(
                tableMeta.getColumn(column).getField().getRelType().getSqlTypeName().getName().toUpperCase());
            if (columnTypeName.equals(SqlDataTypeSpec.DrdsTypeName.BLOB) || columnTypeName.equals(
                SqlDataTypeSpec.DrdsTypeName.TEXT)) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                    "Foreign key columns do not support BLOB/TEXT");
            }
        }

        // columns and referenced columns type must be similar
        Map<String, String> columnMap = IntStream.range(0, data.columns.size()).collect(TreeMaps::caseInsensitiveMap,
            (m, i) -> m.put(data.refColumns.get(i), data.columns.get(i)), Map::putAll);

        TableMeta referringTableMeta =
            executionContext.getSchemaManager(data.refSchema).getTableWithNull(data.refTableName);
        if (referringTableMeta == null) {
            return;
        }

        for (String column : data.refColumns) {
            if (referringTableMeta != null && !referringTableMeta.getAllColumns().stream()
                .map(c -> c.getName().toLowerCase()).collect(Collectors.toList())
                .contains(column.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                    String.format("Column `%s` not exist in table `%s`.", column, data.refTableName));
            }
        }

        // charset and collation must be same
        String charset = tableMeta.getDefaultCharset();
        String collation = tableMeta.getDefaultCollation();
        if (!StringUtils.equalsIgnoreCase(charset, referringTableMeta.getDefaultCharset())) {
            throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CHARSET_COLLATION,
                schemaName, tableName, data.refSchema, data.refTableName);
        }
        if (!StringUtils.equalsIgnoreCase(collation, referringTableMeta.getDefaultCollation())) {
            throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CHARSET_COLLATION,
                schemaName, tableName, data.refSchema, data.refTableName);
        }

        // engine must be innodb
        if (Engine.isFileStore(referringTableMeta.getEngine())) {
            throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_ENGINE, referringTableMeta.getEngine().toString());
        }

        for (ColumnMeta column : referringTableMeta.getAllColumns()) {
            if (data.refColumns.stream().anyMatch(column.getName()::equalsIgnoreCase)) {
                SqlDataTypeSpec.DrdsTypeName columnTypeName = SqlDataTypeSpec.DrdsTypeName.from(
                    tableMeta.getColumn(columnMap.get(column.getName())).getField().getRelType().getSqlTypeName()
                        .getName().toUpperCase());

                String charSetName =
                    tableMeta.getColumn(columnMap.get(column.getName())).getDataType().getCharsetName().name();
                String collationName =
                    tableMeta.getColumn(columnMap.get(column.getName())).getDataType().getCollationName().name();

                if (!columnTypeName.equals(
                    SqlDataTypeSpec.DrdsTypeName.from(column.getDataType().getStringSqlType().toUpperCase()))) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CHANGE_COLUMN_FK_CONSTRAINT,
                        column.getName(), schemaName, tableName, columnMap.get(column.getName()), data.refSchema,
                        data.refTableName, data.constraint);
                }

                if (!StringUtils.equalsIgnoreCase(charSetName, column.getDataType().getCharsetName().name())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CHARSET_COLLATION,
                        schemaName, tableName, data.refSchema, data.refTableName);
                }

                if (!StringUtils.equalsIgnoreCase(collationName, column.getDataType().getCollationName().name())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CHARSET_COLLATION,
                        schemaName, tableName, data.refSchema, data.refTableName);
                }

                // can not add fk on generated column
                if (column.isGeneratedColumn()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_GENERATED_COLUMN, column.getName());
                }
            }
        }

        // can not add fk on generated column
        Set<String> refGeneratedReferencedColumns = new TreeSet<>(String::compareToIgnoreCase);
        refGeneratedReferencedColumns.addAll(
            GeneratedColumnUtil.getAllReferencedColumnByRef(referringTableMeta).keySet());
        GeneratedColumnUtil.getAllReferencedColumnByRef(referringTableMeta).values()
            .forEach(r -> refGeneratedReferencedColumns.addAll(r));
        for (String column : data.refColumns) {
            if (refGeneratedReferencedColumns.contains(column)) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_GENERATED_COLUMN, column);
            }
        }

        for (String column : data.columns) {
            ColumnMeta columnMeta = tableMeta.getColumn(column);
            if (columnMeta.isGeneratedColumn()) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_GENERATED_COLUMN, columnMeta.getName());
            }
        }

        Set<String> generatedReferencedColumns = new TreeSet<>(String::compareToIgnoreCase);
        generatedReferencedColumns.addAll(GeneratedColumnUtil.getAllReferencedColumnByRef(tableMeta).keySet());
        GeneratedColumnUtil.getAllReferencedColumnByRef(tableMeta).values()
            .forEach(r -> generatedReferencedColumns.addAll(r));
        for (String column : data.columns) {
            if (generatedReferencedColumns.contains(column)) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_GENERATED_COLUMN, column);
            }
        }
    }

    public static void validateAddReferredTableFkIndex(ForeignKeyData data, ExecutionContext executionContext,
                                                       String tableName, SqlCreateTable sqlCreateTable) {
        // ref table fk column index must exist
        Set<String> columnsHash = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        columnsHash.addAll(data.refColumns);

        // table referenced itself
        if (sqlCreateTable != null && data.refTableName.equalsIgnoreCase(tableName)) {
            if (sqlCreateTable.getKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> key : sqlCreateTable.getKeys()) {
                    if (hasFkColumnIndex(key.getValue(), columnsHash)) {
                        return;
                    }
                }
            }

            if (sqlCreateTable.getPrimaryKey() != null) {
                if (hasFkColumnIndex(sqlCreateTable.getPrimaryKey(), columnsHash)) {
                    return;
                }
            }

            for (Pair<SqlIdentifier, SqlColumnDeclaration> col : sqlCreateTable.getColDefs()) {
                if (col.right.getSpecialIndex() != null && col.right.getSpecialIndex()
                    .equals(SqlColumnDeclaration.SpecialIndex.PRIMARY)
                    && data.refColumns.stream().anyMatch(c -> c.equalsIgnoreCase(col.left.getLastName()))) {
                    return;
                }
            }

            throw new TddlRuntimeException(ErrorCode.ERR_CREATE_FK_MISSING_INDEX);
        }

        TableMeta tableMeta = executionContext.getSchemaManager(data.refSchema).getTableWithNull(data.refTableName);
        if (tableMeta == null) {
            return;
        }

        List<IndexMeta> indexes = tableMeta.getIndexes();

        for (IndexMeta im : indexes) {
            boolean hasFkColumnIndex = true;

            final List<String> indexColumnList = new ArrayList<>();
            im.getKeyColumns().stream().map(ColumnMeta::getName).forEach(indexColumnList::add);

            final Set<String> indexColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            indexColumns.addAll(indexColumnList);

            if (indexColumns.containsAll(columnsHash)) {
                for (int i = 0; i < columnsHash.size(); i++) {
                    if (!columnsHash.contains(indexColumnList.get(i))) {
                        hasFkColumnIndex = false;
                        break;
                    }
                }
            } else {
                hasFkColumnIndex = false;
            }

            if (hasFkColumnIndex) {
                return;
            }
        }

        throw new TddlRuntimeException(ErrorCode.ERR_CREATE_FK_MISSING_INDEX);
    }

    private static boolean hasFkColumnIndex(SqlIndexDefinition key, Set<String> columnsHash) {
        boolean hasFkColumnIndex = true;

        final List<String> indexColumnList = new ArrayList<>();
        key.getColumns().stream().map(c -> c.getColumnName().getLastName())
            .forEach(indexColumnList::add);

        final Set<String> indexColumns = new HashSet<>(indexColumnList);

        if (indexColumns.containsAll(columnsHash)) {
            for (int i = 0; i < columnsHash.size(); i++) {
                if (!columnsHash.contains(indexColumnList.get(i))) {
                    hasFkColumnIndex = false;
                    break;
                }
            }
        }

        if (hasFkColumnIndex) {
            return true;
        }

        return false;
    }

    public static void validateDropReferredTableFkIndex(TableMeta tableMeta, String indexName) {
        List<String> indexColumnList = new ArrayList<>();
        List<String> otherIndexColumnListString = new ArrayList<>();
        List<IndexMeta> indexes = tableMeta.getIndexes();
        for (IndexMeta im : indexes) {
            if (im.getPhysicalIndexName().equals(indexName)) {
                im.getKeyColumns().stream().map(ColumnMeta::getName).forEach(indexColumnList::add);
            } else {
                List<String> otherIndexColumnList = new ArrayList<>();
                im.getKeyColumns().stream().map(ColumnMeta::getName).forEach(otherIndexColumnList::add);
                otherIndexColumnListString.add(otherIndexColumnList.toString());
            }
        }

        boolean usedIndex = false;

        for (Map.Entry<String, ForeignKeyData> e : tableMeta.getReferencedForeignKeys().entrySet()) {
            HashSet<String> columnsHash = new HashSet<>(e.getValue().refColumns);

            boolean hasFkColumnIndex = true;
            final Set<String> indexColumns = new HashSet<>(indexColumnList);

            if (indexColumns.containsAll(columnsHash)) {
                for (int i = 0; i < columnsHash.size(); i++) {
                    if (!columnsHash.contains(indexColumnList.get(i))) {
                        hasFkColumnIndex = false;
                        break;
                    }
                }
            } else {
                hasFkColumnIndex = false;
            }

            if (hasFkColumnIndex) {
                usedIndex = true;
                break;
            }
        }

        if (usedIndex) {
            int cnt = 0;

            String indexString = indexColumnList.toString();
            for (String s : otherIndexColumnListString) {
                if (s.startsWith(indexString)) {
                    cnt++;
                }
            }

            if (cnt == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_DROP_FK_INDEX, indexName);
            }
        }
    }

    private static void checkColumnType(SqlModifyColumn alterItem,
                                        Map<String, SqlDataTypeSpec.DrdsTypeName> columnsBeforeDdlType,
                                        String columnName,
                                        Map<String, ForeignKeyData> columns, boolean referenced) {
        SqlDataTypeSpec.DrdsTypeName columnType = SqlDataTypeSpec.DrdsTypeName.from(
            alterItem.getColDef().getDataType().getTypeName().getLastName().toUpperCase());
        if (!columnType.equals(columnsBeforeDdlType.get(columnName))) {
            int columnIndex = referenced ? columns.get(columnName).refColumns.indexOf(columnName) :
                columns.get(columnName).columns.indexOf(columnName);
            String schemaName = columns.get(columnName).schema;
            String tableName = columns.get(columnName).tableName;
            String referencingColumnName = columns.get(columnName).refColumns.get(columnIndex);
            String referencedColumnName = columns.get(columnName).columns.get(columnIndex);
            if (referenced) {
                throw new TddlRuntimeException(ErrorCode.ERR_CHANGE_COLUMN_FK_CONSTRAINT, referencingColumnName,
                    schemaName,
                    tableName, referencedColumnName, columns.get(columnName).schema, columns.get(columnName).tableName,
                    columns.get(columnName).constraint);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_CHANGE_COLUMN_FK_CONSTRAINT,
                    columns.get(columnName).refSchema, columns.get(columnName).refTableName, referencingColumnName,
                    schemaName, tableName, referencedColumnName, columns.get(columnName).constraint);
            }
        }
    }

    private static SqlCreateTable getCharsetAndCollation(SqlCreateTable sqlCreateTableOrigin, String schemaName,
                                                         ExecutionContext executionContext) {
        SqlCreateTable sqlCreateTable = sqlCreateTableOrigin.clone(SqlParserPos.ZERO);
        String defaultCharset = sqlCreateTable.getDefaultCharset();
        String defaultCollation = sqlCreateTable.getDefaultCollation();
        String charset;
        String collation;
        StringBuilder builder = new StringBuilder();
        charset = DbInfoManager.getInstance().getDbChartSet(schemaName);
        collation = DbInfoManager.getInstance().getDbCollation(schemaName);

        if (StringUtils.isEmpty(charset) || StringUtils.isEmpty(collation)) {
            /**
             * For some unit-test, its dbInfo is mock,so its charset & collation maybe null
             */
            // Fetch server default collation
            DdlCharsetInfo serverDefaultCharsetInfo =
                DdlCharsetInfoUtil.fetchServerDefaultCharsetInfo(executionContext, true);

            // Fetch db collation by charset & charset defined by user and server default collation
            DdlCharsetInfo createDbCharInfo =
                DdlCharsetInfoUtil.decideDdlCharsetInfo(executionContext, serverDefaultCharsetInfo.finalCharset,
                    serverDefaultCharsetInfo.finalCollate,
                    charset, collation, true);
            charset = createDbCharInfo.finalCharset;
            collation = createDbCharInfo.finalCollate;
        }

        // Fetch tbl collation by charset & charset defined by user and db collation
        DdlCharsetInfo createTbCharInfo =
            DdlCharsetInfoUtil.decideDdlCharsetInfo(executionContext, charset, collation,
                defaultCharset, defaultCollation, true);

        if (defaultCharset == null && defaultCollation == null && createTbCharInfo.finalCharset != null) {
            builder.append(" CHARSET `").append(createTbCharInfo.finalCharset.toLowerCase()).append("`");
            sqlCreateTable.setDefaultCharset(createTbCharInfo.finalCharset.toLowerCase());
        }

        if (defaultCollation == null && createTbCharInfo.finalCollate != null) {
            builder.append(" COLLATE `").append(createTbCharInfo.finalCollate.toLowerCase()).append("`");
            sqlCreateTable.setDefaultCollation(createTbCharInfo.finalCollate.toLowerCase());
        }

        return sqlCreateTable;
    }
}

