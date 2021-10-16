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

import com.alibaba.polardbx.sequence.Sequence;
import com.alibaba.polardbx.sequence.exception.SequenceException;

/**
 * Work as a placeholder for NULL_OBJ.
 */
public class DefaultSequence implements Sequence {

    @Override
    public long nextValue() throws SequenceException {
        throw new SequenceException(new UnsupportedOperationException());
    }

    @Override
    public long nextValue(int size) throws SequenceException {
        throw new SequenceException(new UnsupportedOperationException());
    }

    @Override
    public boolean exhaustValue() throws SequenceException {
        throw new SequenceException(new UnsupportedOperationException());
    }

}
