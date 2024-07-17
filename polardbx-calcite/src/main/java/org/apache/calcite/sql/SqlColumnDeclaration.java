/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Parse tree for {@code UNIQUE}, {@code PRIMARY KEY} constraints.
 *
 * <pre>
 * column_definition:
 *     data_type [NOT NULL | NULL] [DEFAULT {literal | (expr)} ]
 *       [AUTO_INCREMENT] [UNIQUE [KEY]] [[PRIMARY] KEY]
 *       [COMMENT 'string']
 *       [COLLATE collation_name]
 *       [COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}]
 *       [STORAGE {DISK|MEMORY|DEFAULT}]
 *       [reference_definition]
 *   | data_type
 *       [GENERATED ALWAYS] AS (expression)
 *       [VIRTUAL | STORED] [NOT NULL | NULL]
 *       [UNIQUE [KEY]] [[PRIMARY] KEY]
 *       [COMMENT 'string']
 * </pre>
 */
public class SqlColumnDeclaration extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
        new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

    private final SqlIdentifier name;
    private final SqlDataTypeSpec dataType;

    private final ColumnNull notNull;
    /**
     * DEFAULT {literal | (expr)}
     */
    private final SqlLiteral defaultVal;
    private final SqlCall defaultExpr;
    private final boolean autoIncrement;
    /**
     * [UNIQUE [KEY]] [[PRIMARY] KEY]
     */
    private final SpecialIndex specialIndex;
    private final SqlLiteral comment;
    /**
     * for MySQL NDB only
     */
    private final ColumnFormat columnFormat;
    /**
     * for MySQL NDB only
     */
    private final Storage storage;
    private final SqlReferenceDefinition referenceDefinition;
    private final boolean onUpdateCurrentTimestamp;
    private final Type autoIncrementType;
    private final int unitCount;
    private final int unitIndex;
    private final int innerStep;

    private final boolean generatedAlways;
    private final boolean generatedAlwaysLogical;
    private final SqlCall generatedAlwaysExpr;
    /**
     * [VIRTUAL | STORED]
     */
    private final ColumnStrategy strategy;

    private String securedWith;

    /**
     * <pre>
     * data_type [NOT NULL | NULL] [DEFAULT {literal | (expr)} ]
     *   [AUTO_INCREMENT] [UNIQUE [KEY]] [[PRIMARY] KEY]
     *   [COMMENT 'string']
     *   [COLLATE collation_name]
     *   [COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}]
     *   [STORAGE {DISK|MEMORY|DEFAULT}]
     *   [reference_definition]
     * </pre>
     */
    public SqlColumnDeclaration(SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, ColumnNull notNull,
                                SqlLiteral defaultVal, SqlCall defaultExpr, boolean autoIncrement,
                                SpecialIndex specialIndex, SqlLiteral comment, ColumnFormat columnFormat,
                                Storage storage, SqlReferenceDefinition referenceDefinition,
                                boolean onUpdateCurrentTimestamp, Type autoIncrementType, int unitCount, int unitIndex,
                                int innerStep, boolean generatedAlways, boolean generatedAlwaysLogical,
                                SqlCall generatedAlwaysExpr) {
        super(pos);
        this.name = name;
        this.dataType = dataType;
        this.notNull = notNull;
        this.defaultVal = defaultVal;
        this.defaultExpr = defaultExpr;
        this.autoIncrement = autoIncrement;
        this.specialIndex = specialIndex;
        this.comment = comment;
        this.columnFormat = columnFormat;
        this.storage = storage;
        this.referenceDefinition = referenceDefinition;
        this.onUpdateCurrentTimestamp = onUpdateCurrentTimestamp;
        this.autoIncrementType = autoIncrementType;
        this.unitCount = unitCount;
        this.unitIndex = unitIndex;
        this.innerStep = innerStep;
        this.generatedAlways = generatedAlways;
        this.generatedAlwaysLogical = generatedAlwaysLogical;
        this.generatedAlwaysExpr = generatedAlwaysExpr;
        this.strategy = null;
    }

    /**
     * <pre>
     * data_type
     *   [GENERATED ALWAYS] AS (expression)
     *   [VIRTUAL | STORED] [NOT NULL | NULL]
     *   [UNIQUE [KEY]] [[PRIMARY] KEY]
     *   [COMMENT 'string']
     * </pre>
     */
    public SqlColumnDeclaration(SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType,
                                boolean generatedAlways, boolean generatedAlwaysLogical, SqlCall generatedAlwaysExpr,
                                ColumnStrategy columnStrategy, ColumnNull notNull, SpecialIndex specialIndex,
                                SqlLiteral comment) {
        super(pos);
        this.name = name;
        this.dataType = dataType;
        this.notNull = notNull;
        this.defaultVal = null;
        this.defaultExpr = null;
        this.autoIncrement = false;
        this.specialIndex = specialIndex;
        this.comment = comment;
        this.columnFormat = null;
        this.storage = null;
        this.referenceDefinition = null;
        this.onUpdateCurrentTimestamp = false;
        this.autoIncrementType = null;
        this.unitCount = SequenceAttribute.UNDEFINED_UNIT_COUNT;
        this.unitIndex = SequenceAttribute.UNDEFINED_UNIT_INDEX;
        this.innerStep = SequenceAttribute.UNDEFINED_INNER_STEP;
        this.generatedAlways = generatedAlways;
        this.generatedAlwaysLogical = generatedAlwaysLogical;
        this.generatedAlwaysExpr = generatedAlwaysExpr;
        this.strategy = columnStrategy;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name,
            dataType,
            SqlUtil.wrapSqlLiteralSymbol(notNull),
            defaultExpr,
            SqlUtil.wrapSqlLiteralSymbol(autoIncrementType),
            SqlUtil.wrapSqlLiteralSymbol(specialIndex),
            comment,
            SqlUtil.wrapSqlLiteralSymbol(columnFormat),
            SqlUtil.wrapSqlLiteralSymbol(storage),
            referenceDefinition,
            generatedAlwaysExpr,
            SqlUtil.wrapSqlLiteralSymbol(strategy));
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public Type getAutoIncrementType() {
        return autoIncrementType;
    }

    public int getUnitCount() {
        return unitCount;
    }

    public int getUnitIndex() {
        return unitIndex;
    }

    public int getInnerStep() {
        return innerStep;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, leftPrec, rightPrec);
        dataType.unparse(writer, leftPrec, rightPrec);

        if (null == generatedAlwaysExpr) {
            unparseNotNull(writer);

            if (null != defaultExpr || null != defaultVal) {
                writer.keyword("DEFAULT");
                if (null != defaultVal) {
                    defaultVal.unparse(writer, leftPrec, rightPrec);
                } else {
                    final Frame frame = writer.startList(FrameTypeEnum.FUN_CALL, "(", ")");
                    defaultExpr.unparse(writer, leftPrec, rightPrec);
                    writer.endList(frame);
                }
            }

            if (autoIncrement) {
                writer.keyword("AUTO_INCREMENT");
                // TODO: print SEQUENCE config
            }

            if (onUpdateCurrentTimestamp) {
                writer.keyword("ON UPDATE CURRENT_TIMESTAMP");
            }

            unparseSpecialIndex(writer);
            unparseComment(writer, leftPrec, rightPrec);

            if (null != columnFormat) {
                writer.keyword("COLUMN_FORMAT");
                SqlUtil.wrapSqlLiteralSymbol(columnFormat).unparse(writer, leftPrec, rightPrec);
            }

            if (null != storage) {
                writer.keyword("STORAGE");
                SqlUtil.wrapSqlLiteralSymbol(storage).unparse(writer, leftPrec, rightPrec);
            }

            if (null != referenceDefinition) {
                referenceDefinition.unparse(writer, leftPrec, rightPrec);
            }
        } else {
            if (generatedAlways) {
                writer.keyword("GENERATED ALWAYS");
            }

            writer.keyword("AS");

            final Frame frame = writer.startList(FrameTypeEnum.FUN_CALL, "(", ")");
            generatedAlwaysExpr.unparse(writer, leftPrec, rightPrec);
            writer.endList(frame);

            if (strategy != null) {
                switch (strategy) {
                case VIRTUAL:
                case STORED:
                    SqlUtil.wrapSqlLiteralSymbol(strategy).unparse(writer, leftPrec, rightPrec);
                    break;
                default:
                }
            }

            unparseNotNull(writer);
            unparseSpecialIndex(writer);
            unparseComment(writer, leftPrec, rightPrec);
        }
    }

    private void unparseComment(SqlWriter writer, int leftPrec, int rightPrec) {
        if (null != comment) {
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }
    }

    private void unparseSpecialIndex(SqlWriter writer) {
        if (null != specialIndex) {
            switch (specialIndex) {
            case PRIMARY:
                writer.keyword("PRIMARY KEY");
                break;
            case UNIQUE:
                writer.keyword("UNIQUE KEY");
                break;
            default:
            }
        }
    }

    private void unparseNotNull(SqlWriter writer) {
        if (null != notNull) {
            switch (notNull) {
            case NULL:
                writer.keyword("NULL");
                break;
            case NOTNULL:
                writer.keyword("NOT NULL");
                break;
            default:
            }
        }
    }

    public static enum SpecialIndex {
        PRIMARY, UNIQUE,
    }

    public static enum ColumnFormat {
        FIXED, DYNAMIC, DEFAULT,
    }

    public static enum Storage {
        DISK, MEMORY, DEFAULT,
    }

    /**
     * 对于没有指定NULL/NOT NULL时不输出
     */
    public static enum ColumnNull {
        NULL, NOTNULL,
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlDataTypeSpec getDataType() {
        return dataType;
    }

    public ColumnNull getNotNull() {
        return notNull;
    }

    public SqlLiteral getDefaultVal() {
        return defaultVal;
    }

    public SqlCall getDefaultExpr() {
        return defaultExpr;
    }

    public SpecialIndex getSpecialIndex() {
        return specialIndex;
    }

    public SqlLiteral getComment() {
        return comment;
    }

    public ColumnFormat getColumnFormat() {
        return columnFormat;
    }

    public Storage getStorage() {
        return storage;
    }

    public SqlReferenceDefinition getReferenceDefinition() {
        return referenceDefinition;
    }

    public boolean isOnUpdateCurrentTimestamp() {
        return onUpdateCurrentTimestamp;
    }

    public boolean isGeneratedAlways() {
        return generatedAlways;
    }

    public boolean isGeneratedAlwaysLogical() {
        return generatedAlwaysLogical;
    }

    public SqlCall getGeneratedAlwaysExpr() {
        return generatedAlwaysExpr;
    }

    public ColumnStrategy getStrategy() {
        return strategy;
    }

    public String getSecuredWith() {
        return securedWith;
    }

    public void setSecuredWith(String securedWith) {
        this.securedWith = securedWith;
    }
}

// End SqlColumnDeclaration.java
