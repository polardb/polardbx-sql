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

import org.openjdk.jol.info.ClassLayout;

/**
 * A Block contains all Nulls
 */
public class NullBlock implements Block {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(NullBlock.class).instanceSize();

    final int positionCount;

    public NullBlock(int positionCount) {
        this.positionCount = positionCount;
    }

    @Override
    public boolean isNull(int position) {
        checkReadablePosition(position);
        return true;
    }

    @Override
    public Object getObject(int position) {
        checkReadablePosition(position);
        return null;
    }

    @Override
    public long estimateSize() {
        return INSTANCE_SIZE;
    }

    @Override
    public long getElementUsedBytes() {
        return 4;
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        checkReadablePosition(position);
        blockBuilder.appendNull();
    }

    void checkReadablePosition(int position) {
        if (position < 0 || position >= positionCount) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public long hashCodeUseXxhash(int pos) {
        return NULL_HASH_CODE;
    }

    @Override
    public boolean mayHaveNull() {
        return true;
    }
}
