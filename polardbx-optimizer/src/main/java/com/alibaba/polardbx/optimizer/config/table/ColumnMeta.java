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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.io.Serializable;

/**
 * Column 的元信息描述
 *
 * @author whisper
 */
public class ColumnMeta implements Serializable {

    private static final long serialVersionUID = 1748510851861759314L;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 列名
     */
    protected final String name;

    /**
     * 当前列的别名
     */
    protected final String alias;

    private String fullName;

    private Field field;

    private final ColumnStatus status;

    private final long flag;

    /**
     * 映射列名 online change column
     */
    private final String mappingName;

    public ColumnMeta(String tableName, String name, String alias, Field field) {
        this.tableName = (tableName);
        this.name = (name);
        this.alias = alias;
        this.field = field;
        this.status = ColumnStatus.PUBLIC;   //兼容以前
        this.flag = 0;
        this.mappingName = null;
    }

    public ColumnMeta(String tableName, String name, String alias, Field field, ColumnStatus status, long flag,
                      String mappingName) {
        this.tableName = (tableName);
        this.name = (name);
        this.alias = alias;
        this.field = field;
        this.status = status;
        this.flag = flag;
        this.mappingName = mappingName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return field.getDataType();
    }

    public String getAlias() {
        return alias;
    }

    public boolean isNullable() {
        return field.isNullable();
    }

    public boolean isAutoIncrement() {
        return field.isAutoIncrement();
    }

    public String getOriginTableName() {
        return field.getOriginTableName();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((alias == null) ? 0 : alias.toLowerCase().hashCode());
        result = prime * result + (this.isAutoIncrement() ? 1231 : 1237);
        result = prime * result + ((this.getDataType() == null) ? 0 : this.getDataType().getSqlType());
        result = prime * result + ((name == null) ? 0 : name.toLowerCase().hashCode());
        result = prime * result + (this.isNullable() ? 1231 : 1237);
        result = prime * result + ((tableName == null) ? 0 : tableName.toLowerCase().hashCode());
        if (ColumnStatus.PUBLIC == status && 0 == flag) {
            return result; // Keep original hash code when default.
        }
        result = prime * result + status.hashCode();
        result = prime * result + Long.hashCode(flag);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ColumnMeta other = (ColumnMeta) obj;
        if (alias == null) {
            if (other.alias != null) {
                return false;
            }
        } else if (!alias.equalsIgnoreCase(other.alias)) {
            return false;
        }
        if (this.isAutoIncrement() != other.isAutoIncrement()) {
            return false;
        }
        if (this.getDataType() == null) {
            if (other.getDataType() != null) {
                return false;
            }
        } else if (!DataTypeUtil.equalsSemantically(this.getDataType(), other.getDataType())) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equalsIgnoreCase(other.name)) {
            return false;
        }
        if (this.isNullable() != other.isNullable()) {
            return false;
        }
        if (tableName == null) {
            if (other.tableName != null) {
                return false;
            }
        } else if (!tableName.equalsIgnoreCase(other.tableName)) {
            return false;
        }
        return true;
    }

    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = GeneralUtil.getTab(inden);
        sb.append(tabTittle).append(tableName).append(".");
        sb.append(name);
        if (alias != null) {
            sb.append(" as ").append(alias);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    public String getFullName() {
        if (this.fullName == null) {
            String cn = this.getAlias() != null ? this.getAlias() : this.getName();
            String tableName = this.getTableName() == null ? "" : this.getTableName();
            StringBuilder sb = new StringBuilder(tableName.length() + 1 + cn.length());
            sb.append(this.getTableName());
            sb.append('.');
            sb.append(cn);

            this.fullName = sb.toString();
        }
        return this.fullName;
    }

    public String getOriginColumnName() {
        return field.getOriginColumnName();
    }

    public long getLength() {
        return field.getLength();
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Field getField() {
        return this.field;
    }

    public ColumnStatus getStatus() {
        return status;
    }

    public long getFlag() {
        return flag;
    }

    public boolean isFillDefault() {
        return (flag & ColumnsRecord.FLAG_FILL_DEFAULT) != 0L;
    }

    public boolean isBinaryDefault() {
        return (flag & ColumnsRecord.FLAG_BINARY_DEFAULT) != 0L;
    }

    public boolean isLogicalGeneratedColumn() {
        return (flag & ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN) != 0L;
    }

    public boolean isGeneratedColumn() {
        return field.getExtra() != null && (field.getExtra().equalsIgnoreCase("VIRTUAL GENERATED") || field.getExtra()
            .equalsIgnoreCase("STORED GENERATED"));
    }

    public boolean isDefaultExpr() {
        return (flag & ColumnsRecord.FLAG_DEFAULT_EXPR) != 0L && !StringUtils.isEmpty(field.getUnescapeDefault())
            && !isGeneratedColumn();
    }

    public String getMappingName() {
        return mappingName;
    }

    public boolean isAutoUpdateColumn() {
        return field.getExtra() != null && (TStringUtil.containsIgnoreCase(field.getExtra(), "on update"));
    }

    public boolean isTimestampColumn() {
        return DataTypeUtil.equalsSemantically(getDataType(), DataTypes.TimestampType);
    }

    public boolean withImplicitDefault(boolean withIgnoreKeyword) {
        return (isTimestampColumn() || withIgnoreKeyword) && !isNullable();
    }

    public boolean withDynamicImplicitDefault() {
        return isTimestampColumn() && !isNullable();
    }
}
