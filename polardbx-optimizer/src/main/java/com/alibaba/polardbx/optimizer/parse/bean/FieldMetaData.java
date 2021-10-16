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

package com.alibaba.polardbx.optimizer.parse.bean;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/**
 * @author hongxi.chx
 */
public class FieldMetaData extends TableMetaData {
    private String fieldName;
    private String originFieldName;
    private long fieldLength;
    private DataType type;
    private boolean autoincrement;
    private int scale;
    private int precision;
    private String collation;
    private boolean isPrimaryKey;
    private boolean isNullable;
    private boolean isUnSigned;

    public static FieldMetaData copyFrom(ColumnMeta columnMeta) {
        final FieldMetaData fieldMetaData = new FieldMetaData();
        Field field = columnMeta.getField();
        fieldMetaData.setFieldName(columnMeta.getName());
        // FIXME field.getOriginColumnName()无法获取原字段名
        fieldMetaData.setOriginFieldName(field.getOriginColumnName());
        fieldMetaData.setFieldLength(field.getLength());
        fieldMetaData.setType(field.getDataType() != null ? field.getDataType() : DataTypes.UndecidedType);
        fieldMetaData.setAutoincrement(field.isAutoIncrement());
        fieldMetaData.setScale(field.getScale());
        fieldMetaData.setPrecision(field.getPrecision());
        fieldMetaData.setCollation(field.getCollationName());
        fieldMetaData.setPrimaryKey(field.isPrimary());
        fieldMetaData.setNullable(field.isNullable());

        if (columnMeta.getDataType() != null) {
            fieldMetaData.setUnSigned(columnMeta.getDataType().isUnsigned());
        }
        return fieldMetaData;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getOriginFieldName() {
        return originFieldName;
    }

    public void setOriginFieldName(String originFieldName) {
        this.originFieldName = originFieldName;
    }

    public long getFieldLength() {
        return fieldLength;
    }

    public DataType getType() {
        return type;
    }

    public boolean isAutoincrement() {
        return autoincrement;
    }

    public void setFieldLength(long fieldLength) {
        this.fieldLength = fieldLength;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public void setAutoincrement(boolean autoincrement) {
        this.autoincrement = autoincrement;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public void setNullable(boolean nullable) {
        isNullable = nullable;
    }

    public boolean isUnSigned() {
        return isUnSigned;
    }

    public void setUnSigned(boolean unSigned) {
        isUnSigned = unSigned;
    }
}
