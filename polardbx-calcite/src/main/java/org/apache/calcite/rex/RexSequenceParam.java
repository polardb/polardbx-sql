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

package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;

/**
 * Parameter which will be replaced with sequence value in executor
 *
 * @author chenmo.cm
 */
public class RexSequenceParam extends RexDynamicParam {
    private final RexNode sequenceCall;

    public RexSequenceParam(RelDataType type, int index, RexNode sequenceCall) {
        super(type, index, DYNAMIC_TYPE_VALUE.SEQUENCE);
        this.sequenceCall = sequenceCall;
    }

    public RexNode getSequenceCall() {
        return sequenceCall;
    }

    public String getSequenceName() {
        final RexCall seqCall = (RexCall) this.getSequenceCall();
        final RexLiteral seqNameLiteral = (RexLiteral) seqCall.getOperands().get(0);
        return seqNameLiteral.getValueAs(String.class);
    }

    @Override
    public boolean literal() {
        return false;
    }
}
