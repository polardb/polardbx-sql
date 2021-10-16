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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import org.apache.calcite.rel.type.RelDataType;

public class Field {

    public static final int DEFAULT_COLUMN_SIZE = 65535;

    /**
     * 原始表名, mysql生成返回的resultSetMetaData中需要记录当前列的原始物理库信息
     */
    private String originTableName = null;

    /**
     * 原始列名
     */
    private String originColumnName = null;

    /**
     * 当前列的类型
     */
    private RelDataType relDataType = null;

    private DataType dataType = null;

    /**
     * 是否为自增
     */
    private boolean autoIncrement = false;

    /**
     * 是否是主键
     */
    private boolean primary = false;

    /**
     * 字符集类型
     */
    private String collationName;

    private Integer collationIndex;

    // Extra Info
    // e.g. AUTO_INCREMENT or ON UPDATE CURRENT_TIMESTAMP
    private String extra;

    // the default info for a column
    // e.g. col timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
    // the default value for col is CURRENT_TIMESTAMP
    private String defaultStr;

    public Field copy() {
        return new Field(originTableName,
            originColumnName,
            collationName,
            extra,
            defaultStr,
            relDataType,
            autoIncrement,
            primary
        );
    }

    public Field(RelDataType relDataType) {
        this.relDataType = relDataType;
        this.autoIncrement = false;
        this.primary = false;
        this.collationName = relDataType.getCollation() == null ? null : relDataType.getCollation().getCollationName();
    }

    @Deprecated
    public Field(DataType dataType) {
        this(null, null, dataType);
    }

    public Field() {
    }

    public Field(String originTableName, String originColumnName, RelDataType relDataType,
                 boolean autoIncrement, boolean primary) {
        this.collationName = relDataType.getCollation() == null ? null : relDataType.getCollation().getCollationName();
        this.originTableName = originTableName;
        this.originColumnName = originColumnName;
        this.relDataType = relDataType;
        this.autoIncrement = autoIncrement;
        this.primary = primary;
    }

    public Field(String originTableName, String originColumnName, String collationName, String extra,
                 String defaultStr, RelDataType relDataType, boolean autoIncrement, boolean primary) {
        this.originTableName = originTableName;
        this.originColumnName = originColumnName;
        this.relDataType = relDataType;
        this.autoIncrement = autoIncrement;
        this.primary = primary;
        this.collationName = collationName;
        this.extra = extra;
        this.defaultStr = defaultStr;
    }

    public Field(String originTableName, String originColumnName, Integer collationIndex, String extra,
                 String defaultStr, RelDataType relDataType, boolean autoIncrement, boolean primary) {
        this.originTableName = originTableName;
        this.originColumnName = originColumnName;
        this.relDataType = relDataType;
        this.autoIncrement = autoIncrement;
        this.primary = primary;
        this.collationIndex = collationIndex;
        this.extra = extra;
        this.defaultStr = defaultStr;
    }

    public Field(String originTableName, String originColumnName, RelDataType relDataType) {
        this.originTableName = originTableName;
        this.originColumnName = originColumnName;
        this.relDataType = relDataType;
        this.autoIncrement = false;
        this.primary = false;
        this.collationName = relDataType.getCollation() == null ? null : relDataType.getCollation().getCollationName();
    }

    @Deprecated
    public Field(String originTableName, String originColumnName, DataType dataType) {
        this.originTableName = originTableName;
        this.originColumnName = originColumnName;
        // 兼容老版本用法 addColumn(DataType.StringType) 直接通过DataType.StringType指定列类型
        this.dataType = dataType;
        this.relDataType = DataTypeUtil.tddlTypeToRelDataType(dataType, 0);
        this.autoIncrement = false;
        this.primary = false;
    }

    public String getOriginTableName() {
        return originTableName;
    }

    public String getOriginColumnName() {
        return originColumnName;
    }

    public DataType getDataType() {
        if (dataType == null) {
            dataType = DataTypeUtil.calciteToDrdsType(relDataType);
        }
        return dataType;
    }

    public boolean isNullable() {
        return relDataType.isNullable();
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public long getLength() {
        return getFieldLength(relDataType);
    }

    public boolean isPrimary() {
        return primary;
    }

    public void setOriginTableName(String originTableName) {
        this.originTableName = originTableName;
    }

    public void setOriginColumnName(String originColumnName) {
        this.originColumnName = originColumnName;
    }

    public void setRelDataType(RelDataType relDataType) {
        this.relDataType = relDataType;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    @Override
    public String toString() {
        return "Field [originTableName=" + originTableName + ", originColumnName=" + originColumnName + ", dataType="
            + relDataType + ", autoIncrement=" + autoIncrement + ", primary=" + primary + "]";
    }

    public int getPrecision() {
        return relDataType.getPrecision();
    }

    public int getScale() {
        return relDataType.getScale();
    }

    public String getCollationName() {
        return collationName;
    }

    public String getExtra() {
        return extra;
    }

    public Integer getCollationIndex() {
        return collationIndex;
    }

    public RelDataType getRelType() {
        return relDataType;
    }

    public String getDefault() {
        return defaultStr;
    }

    private static int getFieldLength(RelDataType type) {
        switch (type.getSqlTypeName()) {
        case BOOLEAN:
            return 1;
        case TINYINT:
        case TINYINT_UNSIGNED:
        case SMALLINT_UNSIGNED:
        case SMALLINT:
        case MEDIUMINT:
        case MEDIUMINT_UNSIGNED:
        case INTEGER:
        case INTEGER_UNSIGNED:
        case BIGINT:
        case BIGINT_UNSIGNED:
        case UNSIGNED:
        case SIGNED:
            return type.getPrecision();
        case YEAR:
            return type.getPrecision();
        case DECIMAL:
            return type.getPrecision() + type.getScale(); // incorrect
        case FLOAT:
        case REAL:
            return 12;
        case DOUBLE:
            return 22;
        case DATETIME:
            return type.getPrecision() != 0 ? type.getPrecision() + 20 : 19;
        case DATE:
            return 10;
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:
            return 10;
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            return 19;
        case CHAR:
        case VARCHAR:
            return type.getPrecision() * 4; // should multiply with max length of encoding
        case BINARY_VARCHAR:
        case BLOB:
        case BINARY:
        case BIT:
        case BIG_BIT:
        case VARBINARY:
            return type.getPrecision();
        }
        return DEFAULT_COLUMN_SIZE;
    }
}
