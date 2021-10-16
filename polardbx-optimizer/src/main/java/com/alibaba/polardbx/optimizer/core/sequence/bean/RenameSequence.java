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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.sequence.sequence.IRenameSequence;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;

/**
 * Created by chensr on 2017/6/9.
 */
public abstract class RenameSequence extends Sequence<IRenameSequence> implements IRenameSequence {

    protected Pair<String, String> namePair;

    public RenameSequence(Pair<String, String> namePair) {
        this.name = namePair.getKey();
        this.namePair = namePair;
    }

    @Override
    public Pair<String, String> getNamePair() {
        return namePair;
    }

    @Override
    public void setNamePair(Pair<String, String> namePair) {
        this.namePair = namePair;
    }

    @Override
    public SEQUENCE_DDL_TYPE getSequenceDdlType() {
        return SEQUENCE_DDL_TYPE.RENAME_SEQUENCE;
    }

    @Override
    public String toStringWithInden(int inden, ExplainResult.ExplainMode mode) {
        StringBuilder sb = new StringBuilder();
        sb.append("RENAME SEQUENCE ");
        sb.append(namePair.getKey());
        sb.append(" TO ");
        sb.append(namePair.getValue());
        return sb.toString();
    }

    protected abstract RenameSequence getRenameSequence(Pair<String, String> namePair);
}
