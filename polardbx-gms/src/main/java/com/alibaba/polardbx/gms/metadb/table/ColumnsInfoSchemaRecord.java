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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ColumnsInfoSchemaRecord implements SystemTableRecord {

    public String tableSchema;
    public String tableName;
    public String columnName;
    public long ordinalPosition;
    public String columnDefault;
    public String isNullable;
    public String dataType;
    public long characterMaximumLength;
    public long characterOctetLength;
    public long numericPrecision;
    public long numericScale;
    public boolean numericScaleNull;
    public long datetimePrecision;
    public String characterSetName;
    public String collationName;
    public String columnType;
    public String columnKey;
    public String extra;
    public String privileges;
    public String columnComment;
    public String generationExpression;

    @Override
    public ColumnsInfoSchemaRecord fill(ResultSet rs) throws SQLException {
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.columnName = rs.getString("column_name");
        this.ordinalPosition = rs.getLong("ordinal_position");
        this.columnDefault = rs.getString("column_default");
        this.isNullable = rs.getString("is_nullable");
        this.dataType = rs.getString("data_type");
        this.characterMaximumLength = rs.getLong("character_maximum_length");
        this.characterOctetLength = rs.getLong("character_octet_length");
        this.numericPrecision = rs.getLong("numeric_precision");
        this.numericScale = rs.getLong("numeric_scale");
        // record the null state of scale
        this.numericScaleNull = rs.wasNull();
        this.datetimePrecision = rs.getLong("datetime_precision");
        this.characterSetName = rs.getString("character_set_name");
        this.collationName = rs.getString("collation_name");
        this.columnType = rs.getString("column_type");
        this.columnKey = rs.getString("column_key");
        this.extra = rs.getString("extra");
        this.privileges = rs.getString("privileges");
        this.columnComment = rs.getString("column_comment");
        this.generationExpression = rs.getString("generation_expression");
        return this;
    }

    protected Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(20);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        setCommonParams(params, index, true);
        return params;
    }

    protected Map<Integer, ParameterContext> buildUpdateParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(18);
        int index = 0;
        setCommonParams(params, index, false);
        return params;
    }

    private void setCommonParams(Map<Integer, ParameterContext> params, int index, boolean forInsert) {
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnName);
        if (forInsert) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.ordinalPosition);
        }
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnDefault);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.isNullable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.dataType);
        if (forInsert && this.characterMaximumLength == 0) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setNull1, null);
        } else {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.characterMaximumLength);
        }
        if (forInsert && this.characterOctetLength == 0) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setNull1, null);
        } else {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.characterOctetLength);
        }
        if (forInsert && this.numericPrecision == 0) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setNull1, null);
        } else {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.numericPrecision);
        }
        if (forInsert && !isDataTypeForZeroNumericScale() && this.numericScale == 0) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setNull1, null);
        } else {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.numericScale);
        }
        if (forInsert && !isDataTypeForZeroDatetimePrecision() && this.datetimePrecision == 0) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setNull1, null);
        } else {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.datetimePrecision);
        }
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.characterSetName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.collationName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnKey);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extra);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.privileges);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnComment);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.generationExpression);
    }

    private boolean isDataTypeForZeroNumericScale() {
        return TStringUtil.equalsIgnoreCase(this.dataType, "bigint")
            || TStringUtil.equalsIgnoreCase(this.dataType, "mediumint")
            || TStringUtil.equalsIgnoreCase(this.dataType, "smallint")
            || TStringUtil.equalsIgnoreCase(this.dataType, "tinyint")
            || TStringUtil.equalsIgnoreCase(this.dataType, "int")
            || TStringUtil.equalsIgnoreCase(this.dataType, "decimal");
    }

    private boolean isDataTypeForZeroDatetimePrecision() {
        return TStringUtil.equalsIgnoreCase(this.dataType, "time")
            || TStringUtil.equalsIgnoreCase(this.dataType, "datetime")
            || TStringUtil.equalsIgnoreCase(this.dataType, "timestamp");
    }

}
