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

/**
 * @author chensr 2016年12月2日 上午11:07:50
 * @since 5.0.0
 */
public class DropGroupSequence extends DropSequence {

    public DropGroupSequence(String schemaName, String name) {
        super(schemaName, name);
    }

    @Override
    public String getSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(SequenceAttribute.DEFAULT_GROUP_TABLE_NAME);
        sb.append(" WHERE ").append(SequenceAttribute.DEFAULT_NAME_COLUMN).append(" = '").append(name).append("'");
        return sb.toString();
    }

}
