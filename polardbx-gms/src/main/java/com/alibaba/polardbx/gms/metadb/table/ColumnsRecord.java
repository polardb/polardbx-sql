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
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class ColumnsRecord extends ColumnsInfoSchemaRecord {

    public int jdbcType;
    public String jdbcTypeName;
    public long fieldLength;
    public long version;
    public int status;
    public long flag;

    public static final long FLAG_FILL_DEFAULT = 0x1;
    public static final long FLAG_BINARY_DEFAULT = 0x2; // If default value is hex string

    @Override
    public ColumnsRecord fill(ResultSet rs) throws SQLException {
        super.fill(rs);
        this.jdbcType = rs.getInt("jdbc_type");
        this.jdbcTypeName = rs.getString("jdbc_type_name");
        this.fieldLength = rs.getLong("field_length");
        this.version = rs.getLong("version");
        this.status = rs.getInt("status");
        this.flag = rs.getLong("flag");
        return this;
    }

    @Override
    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = super.buildInsertParams();
        int index = params.size();
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.jdbcType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.jdbcTypeName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.fieldLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.version);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.flag);
        return params;
    }

    @Override
    protected Map<Integer, ParameterContext> buildUpdateParams() {
        Map<Integer, ParameterContext> params = super.buildUpdateParams();
        int index = params.size();
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.jdbcType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.jdbcTypeName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.fieldLength);
        return params;
    }

    public boolean isFillDefault() {
        return (flag & FLAG_FILL_DEFAULT) != 0L;
    }

    public void setFillDefault() {
        flag |= FLAG_FILL_DEFAULT;
    }

    public void clearFillDefault() {
        flag &= ~FLAG_FILL_DEFAULT;
    }

    public boolean isBinaryDefault() {
        return (flag & FLAG_BINARY_DEFAULT) != 0L;
    }

    public void setBinaryDefault() {
        flag |= FLAG_BINARY_DEFAULT;
    }

    public void clearBinaryDefault() {
        flag &= ~FLAG_BINARY_DEFAULT;
    }
}
