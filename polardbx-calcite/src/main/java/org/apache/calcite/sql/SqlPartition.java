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
import com.alibaba.polardbx.druid.util.StringUtils;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlPartition extends SqlNode {
    protected SqlNode name;
    protected SqlNode subPartitionCount;
    protected List<SqlNode> subPartitions = new ArrayList<>();
    protected SqlPartitionValue values;
    protected String comment;
    protected String locality;

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public SqlPartition(SqlNode name, SqlPartitionValue values, SqlParserPos sqlParserPos) {
        super(sqlParserPos);
        this.name = name;
        this.values = values;
    }

    public List<SqlNode> getSubPartitions() {
        return subPartitions;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append("PARTITION ");
        sb.append(name);
        if (values != null) {
            sb.append(" ");
            sb.append(values.toString());
        }
        if (TStringUtil.isNotEmpty(locality)) {
            sb.append(" LOCALITY=");
            sb.append(TStringUtil.quoteString(locality));
        }

        if (subPartitionCount != null) {
            sb.append(" ");
            sb.append("SUBPARTITIONS ");
            sb.append(subPartitionCount.toString());
        }
        if (subPartitions != null && !subPartitions.isEmpty()) {
            sb.append(" ");
            sb.append("(");
            for (int i = 0; i < subPartitions.size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }
                SqlNode subPart = subPartitions.get(i);
                sb.append(subPart.toString());
            }
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {

        // Validate bound values
        if (values != null) {
            values.validate(validator, scope);
        }
        // Validate partition name
        // To be impl

        // Validate partition comment
        // To be impl

        // Validate subPartitions
        // To be impl

        // Validate subPartitionCount
        // To be impl

    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        if (this == node) {
            return true;
        }

        if (this.getClass() != node.getClass()) {
            return false;
        }

        SqlPartition sqlPart = (SqlPartition) node;

        if (!equalDeep(name, sqlPart.name, litmus, context)) {
            return false;
        }

        // TODO: support subpartition

        if (!StringUtils.equalsIgnoreCase(comment, sqlPart.comment)
            || !StringUtils.equalsIgnoreCase(locality, sqlPart.locality)) {
            return false;
        }

        return equalDeep(values, sqlPart.values, litmus, context);
    }

    public SqlNode getName() {
        return name;
    }

    public void setName(SqlNode name) {
        this.name = name;
    }

    public SqlNode getSubPartitionCount() {
        return subPartitionCount;
    }

    public void setSubPartitionCount(SqlNode subPartitionCount) {
        this.subPartitionCount = subPartitionCount;
    }

    public void setSubPartitions(List<SqlNode> subPartitions) {
        this.subPartitions = subPartitions;
    }

    public SqlPartitionValue getValues() {
        return values;
    }

    public void setValues(SqlPartitionValue values) {
        this.values = values;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public String getLocality() {
        return this.locality;
    }
}
