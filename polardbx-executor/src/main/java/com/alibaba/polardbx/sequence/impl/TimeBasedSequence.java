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

import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;

/**
 * This type of sequence doesn't rely on database and has the following format:
 * timestamp base + worker id + sequence within the period of the timestamp.
 *
 * @author chensr 2016年10月17日 下午1:58:00
 * @since 5.0.0
 */
public class TimeBasedSequence extends BaseSequence {

    private IdGenerator idGenerator;

    public TimeBasedSequence(String name) {
        this.type = Type.TIME;
        this.name = name;
        this.idGenerator = IdGenerator.getIdGenerator();
    }

    @Override
    public long nextValue() throws SequenceException {
        return idGenerator.nextId();
    }

    @Override
    public long nextValue(int size) throws SequenceException {
        return idGenerator.nextId(size);
    }

    @Override
    public boolean exhaustValue() throws SequenceException {
        return true;
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

}
