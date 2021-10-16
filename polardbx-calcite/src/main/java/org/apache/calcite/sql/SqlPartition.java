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
    protected List<SqlSubPartition> subPartitions = new ArrayList<>();
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

    public List<SqlSubPartition> getSubPartitions() {
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
        sb.append(" ");
        sb.append(values.toString());
        if (TStringUtil.isNotEmpty(locality)) {
            sb.append(" LOCALITY=");
            sb.append(TStringUtil.quoteString(locality));
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
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        return false;
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

    public void setSubPartitions(List<SqlSubPartition> subPartitions) {
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
