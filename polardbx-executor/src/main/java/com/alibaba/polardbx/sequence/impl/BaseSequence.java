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

package com.alibaba.polardbx.sequence.impl;

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.sequence.Sequence;
import com.alibaba.polardbx.sequence.exception.SequenceException;

/**
 * @author chensr 2016/10/19 17:17:03
 * @since 5.0.0
 */
public abstract class BaseSequence implements Sequence {

    protected String name;
    protected Type type;

    protected volatile long currentValue;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public long currValue() throws SequenceException {
        return currentValue;
    }

    @Override
    public void updateValue(long value) {
        // Nothing updated by default.
    }

}
