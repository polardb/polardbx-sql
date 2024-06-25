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

package org.apache.calcite.sql;

import com.alibaba.polardbx.common.utils.TStringUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlSubPartition extends SqlNode {
    protected SqlNode name;
    protected SqlPartitionValue values;
    protected String comment;
    protected String locality;

    protected boolean isAddValues;

    public SqlSubPartition(SqlParserPos sqlParserPos, SqlNode name, SqlPartitionValue values) {
        super(sqlParserPos);
        this.name = name;
        this.values = values;
    }

    public SqlSubPartition(SqlParserPos sqlParserPos, SqlNode name, SqlPartitionValue values, boolean isAddValues) {
        this(sqlParserPos, name, values);
        this.isAddValues = isAddValues;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append("SUBPARTITION ");
        sb.append(name);
        if (values != null) {
            sb.append(" ");
            sb.append(values.toString());
        }
        if (TStringUtil.isNotEmpty(locality)) {
            sb.append(" LOCALITY=");
            sb.append(TStringUtil.quoteString(locality));
        }
        return sb.toString();
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (values != null) {
            values.validate(validator, scope);
        }
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        return false;
    }

    public SqlNode getName() {
        return name;
    }

    public void setName(SqlNode name) {
        this.name = name;
    }

    public SqlPartitionValue getValues() {
        return values;
    }

    public String getLocality() {
        return locality;
    }

    public boolean isAddValues() {
        return isAddValues;
    }
}
