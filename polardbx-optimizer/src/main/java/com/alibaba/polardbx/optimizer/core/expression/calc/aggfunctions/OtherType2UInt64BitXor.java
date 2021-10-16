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

package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;

public class OtherType2UInt64BitXor extends SpecificType2UInt64BitXor {
    public OtherType2UInt64BitXor(int index, DataType inputTypes, int filterArg) {
        super(index, inputTypes, filterArg);
    }

    @Override
    protected UInt64 getUInt64(Block block, int position) {
        return ULongType.instance.convertFrom(block.getObject(position));
    }
}
