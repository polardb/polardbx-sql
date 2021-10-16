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

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_NAME_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_TABLE_NAME;

import com.alibaba.polardbx.common.utils.Pair;

/**
 * Created by chensr on 2017/6/10.
 */
public abstract class RenameSequenceOpt extends RenameSequence {

    public RenameSequenceOpt(Pair<String, String> namePair) {
        super(namePair);
    }

    @Override
    public String getSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(DEFAULT_TABLE_NAME);
        sb.append(" SET ").append(DEFAULT_NAME_COLUMN).append(" = '").append(namePair.getValue()).append("'");
        sb.append(" WHERE ").append(DEFAULT_NAME_COLUMN).append(" = '").append(namePair.getKey()).append("'");
        return sb.toString();
    }

}
