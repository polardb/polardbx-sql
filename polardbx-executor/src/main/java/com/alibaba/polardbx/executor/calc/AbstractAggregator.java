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

package com.alibaba.polardbx.executor.calc;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;

/**
 * Created by chuanqin on 17/8/9.
 */
public abstract class AbstractAggregator implements Aggregator {
    protected final int filterArg;

    /**
     * Input type, may be not always same with
     */
    protected DataType[] inputType;

    protected DataType returnType;

    /**
     * Original target index
     */
    protected int[] originTargetIndexes;

    /**
     * Target index used to get block when accumulating
     */
    protected int[] aggIndexInChunk;

    protected boolean isDistinct;

    public AbstractAggregator(int[] targetIndexes, boolean distinct, DataType[] inputType, DataType returnType,
                              int filterArg) {
        this.originTargetIndexes = targetIndexes;
        this.aggIndexInChunk = originTargetIndexes.clone();
        this.isDistinct = distinct;
        this.inputType = inputType;
        this.returnType = returnType;
        this.filterArg = filterArg;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public int getFilterArg() {
        return filterArg;
    }

    public DataType[] getInputType() {
        return inputType;
    }

    public int[] getOriginTargetIndexes() {
        return originTargetIndexes;
    }

    public void setAggIndexInChunk(int[] aggIndexInChunk) {
        this.aggIndexInChunk = aggIndexInChunk;
    }

    /**
     * not always same with aggTargetIndexes, see function: GroupConcat
     */
    public int[] getInputColumnIndexes() {
        return originTargetIndexes;
    }
}