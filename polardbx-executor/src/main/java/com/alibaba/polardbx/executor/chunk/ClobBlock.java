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

package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.sql.Clob;

/**
 * Clob Block
 *
 */
public class ClobBlock extends ReferenceBlock<Clob> {

    public ClobBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, Object[] values) {
        super(arrayOffset, positionCount, valueIsNull, values, DataTypes.ClobType);
    }

    @Override
    public Clob getClob(int position) {
        return getReference(position);
    }

    @Override
    public Clob getObject(int position) {
        return isNull(position) ? null : getClob(position);
    }
}

