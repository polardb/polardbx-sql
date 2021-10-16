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

import com.alibaba.polardbx.optimizer.core.sequence.sequence.IDropSequence;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;

/**
 * drop sequence
 *
 * @author agapple 2014年12月18日 下午7:00:51
 * @since 5.1.17
 */
public abstract class DropSequence extends Sequence<IDropSequence> implements IDropSequence {

    public DropSequence(String schemaName, String name) {
        this.name = name;
        this.schemaName = schemaName;
    }

    @Override
    public String toStringWithInden(int inden, ExplainResult.ExplainMode mode) {
        return "DROP SEQUENCE " + name;
    }

    @Override
    public SEQUENCE_DDL_TYPE getSequenceDdlType() {
        return SEQUENCE_DDL_TYPE.DROP_SEQUENCE;
    }

}
