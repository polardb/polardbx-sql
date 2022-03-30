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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableGroupExtractPartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("EXTRACT PARTITION", SqlKind.EXTRACT_PARTITION);
    private final List<SqlNode> hotKeys;
    private SqlAlterTableGroup parent;
    private final List<SqlPartition> newPartitions;

    private String extractPartitionName;
    private final SQLName hotKeyPartitionName;

    public SqlAlterTableGroupExtractPartition(SqlParserPos pos, List<SqlNode> hotKeys, SQLName hotKeyPartitionName) {
        super(pos);
        this.hotKeys = hotKeys;
        this.hotKeyPartitionName = hotKeyPartitionName;
        newPartitions = new ArrayList<>();
    }

    public String getExtractPartitionName() {
        return extractPartitionName;
    }

    public void setExtractPartitionName(String extractPartitionName) {
        this.extractPartitionName = extractPartitionName;
    }

    public SQLName getHotKeyPartitionName() {
        return hotKeyPartitionName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlAlterTableGroup getParent() {
        return parent;
    }

    public void setParent(SqlAlterTableGroup parent) {
        this.parent = parent;
    }

    public List<SqlNode> getHotKeys() {
        return hotKeys;
    }

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);
        for (SqlNode hotKey : hotKeys) {
            if (hotKey != null) {
                RelDataType dataType = validator.deriveType(scope, hotKey);
                if (dataType == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                        "The hot value is invalid"));
                }
            }
        }
    }
}