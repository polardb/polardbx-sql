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

package com.alibaba.polardbx.optimizer.core.expression.bean;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.IColumn;

/**
 * 描述一个列
 *
 * @author jianghang 2013-11-13 下午5:03:15
 * @since 5.0.0
 */
public class Column implements IColumn {

    protected String alias;
    protected String columName;
    protected String tableName;
    protected boolean distinct;
    protected Field field;
    protected int cachedHashCode = 0;

    public Column(Field field) {
        this.field = field.copy();
    }

    @Override
    public DataType getDataType() {
        return field.getDataType();
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getColumnName() {
        return columName;
    }

    @Override
    public IColumn setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    @Override
    public IColumn setTableName(String tableName) {
        this.tableName = tableName;
        this.cachedHashCode = 0;
        return this;
    }

    @Override
    public IColumn setColumnName(String columnName) {
        this.columName = columnName;
        this.cachedHashCode = 0;
        return this;
    }

    @Override
    public IColumn copy() {
        Column newColumn = new Column(field);
        newColumn.setColumnName(columName);
        newColumn.setAlias(alias);
        newColumn.setTableName(tableName);
        return newColumn;
    }

    @Override
    public int compareTo(Object o) {
        throw new NotSupportException();
    }

    // ================== hashcode/equals/toString不要随意改动=================

    /**
     * 这个方法不要被自动修改！ 在很多地方都有用到。
     */
    @Override
    public int hashCode() {
        if (cachedHashCode != 0) {
            return cachedHashCode;
        }
        final int prime = 31;
        int result = 1;
        result = prime * result + (distinct ? 1231 : 1237);
        result = prime * result + ((columName == null) ? 0 : columName.toUpperCase().hashCode());
        // result = prime * result + ((tableName == null) ? 0 : tableName.toUpperCase().hashCode());
        cachedHashCode = result;
        return result;
    }

    /**
     * 这个方法不要被自动修改！ 在很多地方都有用到。
     */
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

        Column other = (Column) obj;
        // alias 都不为空的时候，进行比较，如果不匹配则返回false,其余时候都跳过alias匹配
        if (alias != null && other.alias != null) {
            if (!alias.equalsIgnoreCase(other.alias)) {
                return false;
            }
        }

        if (columName == null) {
            if (other.columName != null) {
                return false;
            }
        } else if (!columName.equalsIgnoreCase(other.columName)) {
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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (this.getTableName() != null) {
            builder.append(this.getTableName()).append(".");
        }

        builder.append(this.getColumnName());
        if (this.getAlias() != null) {
            builder.append(" as ").append(this.getAlias());
        }
        return builder.toString();
    }

    @Override
    public IColumn setField(Field field) {
        if (field != null) {
            this.field = field.copy();
        }

        return this;
    }

    @Override
    public Field getField() {
        return this.field;
    }
}
