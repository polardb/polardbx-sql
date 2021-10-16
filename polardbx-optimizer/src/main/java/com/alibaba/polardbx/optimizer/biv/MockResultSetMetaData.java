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

package com.alibaba.polardbx.optimizer.biv;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

public class MockResultSetMetaData implements ResultSetMetaData {

    private List<ColumnMeta> columnMetas;
    private boolean columnLabelInSensitive = true;

    public MockResultSetMetaData(List<ColumnMeta> columns, Map<String, Object> connectionProperties) {
        this.columnMetas = columns;
        this.columnLabelInSensitive = GeneralUtil.getPropertyBoolean(connectionProperties,
            ConnectionProperties.COLUMN_LABEL_INSENSITIVE,
            true);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.columnMetas.size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        column--;
        ColumnMeta columnObj = this.columnMetas.get(column);
        return columnObj.getName();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        column--;
        String alias = this.columnMetas.get(column).getAlias();
        String name = this.columnMetas.get(column).getName();
        String label = StringUtils.isBlank(alias) ? name : alias;
        if (columnLabelInSensitive) {
            return label;
        } else {
            return StringUtils.upperCase(label);
        }
    }

    @Override
    public String getTableName(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        return c.getTableName() == null ? "" : c.getTableName();
    }

    public String getOriginTableName(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        return c.getOriginTableName() == null ? "" : c.getOriginTableName();
    }

    public String getOriginColumnName(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        return c.getOriginColumnName() == null ? c.getName() : c.getOriginColumnName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "TDDL";
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        return c.isAutoIncrement();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumnType(column, false);
    }

    public int getColumnType(int column, boolean originType) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        DataType type = c.getDataType();
        if (type == null) {
            throw new SQLException("data type is null, column is: " + c);
        }
        int sqlType = type.getSqlType();
        if (!originType) {
            sqlType = convertToSqlTypeIfNeed(sqlType);
        }
        return sqlType;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        return c.isNullable() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        DataType type = c.getDataType();
        if (type == null) {
            throw new SQLException("data type is null, column is: " + c);
        }

        return !type.isUnsigned();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        return c.getDataType().getClass().getSimpleName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);

        long fieldLength = c.getLength();

        if (DataTypeUtil.equalsSemantically(c.getDataType(), DataTypes.BooleanType)) {
            return 1;
        }
        if (fieldLength > Integer.MAX_VALUE) {
            fieldLength = Integer.MAX_VALUE;
        }

        return (int) (fieldLength <= 0 ? Field.DEFAULT_COLUMN_SIZE :
            fieldLength);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        DataType type = c.getDataType();
        if (type == null) {
            throw new SQLException("data type is null, column is: " + c);
        }

        return type.getJavaClass().getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "TDDL";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);

        return c.getField().getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);

        return c.getField().getScale();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    public List<ColumnMeta> getColumnMetas() {
        return columnMetas;
    }

    public void setColumnMetas(List<ColumnMeta> columnMetas) {
        this.columnMetas = columnMetas;
    }

    protected int convertToSqlTypeIfNeed(int sqlType) {

        // 将自定义的SqlType转为真正的java.sql.Types
        switch (sqlType) {
        case DataType.YEAR_SQL_TYPE:
            sqlType = Types.SMALLINT;
            break;
        case DataType.MEDIUMINT_SQL_TYPE:
            sqlType = Types.INTEGER;
            break;
        case DataType.DATETIME_SQL_TYPE:
            sqlType = Types.TIMESTAMP;
            break;
        default:
            break;
        }

        return sqlType;
    }
}
