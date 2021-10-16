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

package com.alibaba.polardbx.optimizer.core.sequence.bean;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;

/**
 * @author chensr 2016年12月2日 上午11:49:13
 * @since 5.0.0
 */
public class AlterGroupSequence extends AlterSequence {

    public AlterGroupSequence(String schemaName, String name) {
        super(schemaName, name);
        this.type = Type.GROUP;
    }

    @Override
    protected AlterSequence getAlterSequence(String schemaName, String name) {
        return new AlterGroupSequence(schemaName, name);
    }

    @Override
    protected String getSqlConvertedFromGroup() {
        return getSqlWithoutConversion();
    }

    @Override
    protected String getSqlConvertedFromSimple() {
        // Create a new GROUP sequence and drop the existing SIMPLE sequence.
        StringBuilder sb = new StringBuilder();
        sb.append(getSqlCreateGroup());
        sb.append(STMT_SEPARATOR);
        sb.append(getSqlDropSimple());
        return sb.toString();
    }

    @Override
    protected String getSqlConvertedFromTimeBased() {
        // Create a new GROUP sequence and drop the existing TIME sequence.
        StringBuilder sb = new StringBuilder();
        sb.append(getSqlCreateGroup());
        sb.append(STMT_SEPARATOR);
        sb.append(getSqlDropTimeBased());
        return sb.toString();
    }

    @Override
    protected String getSqlWithoutConversion() {
        // Update the existing GROUP sequence directly.
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(SequenceAttribute.DEFAULT_GROUP_TABLE_NAME).append(" SET ");
        sb.append(SequenceAttribute.DEFAULT_VALUE_COLUMN).append(" = ");
        sb.append(start > 0 ? start : SequenceAttribute.DEFAULT_VALUE_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_GMT_MODIFIED_COLUMN).append(" = NOW()");
        sb.append(" WHERE ").append(SequenceAttribute.DEFAULT_NAME_COLUMN).append(" = '").append(name).append("'");
        return sb.toString();
    }

    private String getSqlCreateGroup() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(SequenceAttribute.DEFAULT_GROUP_TABLE_NAME).append("(");
        sb.append(SequenceAttribute.DEFAULT_NAME_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_VALUE_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_GMT_MODIFIED_COLUMN).append(") ");
        sb.append("VALUES('%s', %s, NOW())");
        return String.format(sb.toString(), name, String.valueOf(start > 1 ? start : 0));
    }

    private String getSqlDropSimple() {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(SequenceAttribute.DEFAULT_TABLE_NAME);
        sb.append(" WHERE ").append(SequenceAttribute.DEFAULT_NAME_COLUMN).append(" = '").append(name).append("'");
        return sb.toString();
    }

    private String getSqlDropTimeBased() {
        return getSqlDropSimple();
    }

}
