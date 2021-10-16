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

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;

/**
 * @author chensr 2016年12月2日 上午11:50:20
 * @since 5.0.0
 */
public class AlterSimpleSequence extends AlterSequenceOpt {

    public AlterSimpleSequence(String schemaName, String name) {
        super(schemaName, name);
        this.type = Type.SIMPLE;
    }

    @Override
    protected AlterSequence getAlterSequence(String schemaName, String name) {
        return new AlterSimpleSequence(schemaName, name);
    }

    @Override
    protected String getSqlConvertedFromGroup() {
        // Create a new SIMPLE sequence and drop the existing GROUP sequence.
        StringBuilder sb = new StringBuilder();
        sb.append(getSqlCreateSimple(false));
        sb.append(STMT_SEPARATOR);
        sb.append(getSqlDropGroup());
        return sb.toString();
    }

    @Override
    protected String getSqlConvertedFromSimple() {
        return getSqlWithoutConversion();
    }

    @Override
    protected String getSqlConvertedFromTimeBased() {
        // Update the existing TIME sequence directly for conversion.
        return getSqlAlterFromTimeBased(false);
    }

    @Override
    protected String getSqlWithoutConversion() {
        // Update the existing SIMPLE sequence directly.
        return getSqlAlterSimple(false);
    }

}
