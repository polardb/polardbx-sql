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

package com.alibaba.polardbx.optimizer.partition;

/**
 * @author chenghui.lch
 */
public enum PartitionBoundValueKind {

    /* less than any other value */
    DATUM_MIN_VALUE(-1),
    /* a specific (bounded) value */
    DATUM_NORMAL_VALUE(0),
    /* greater than any other value */
    DATUM_MAX_VALUE(1);

    protected int kind = 0;

    PartitionBoundValueKind(int kindVal) {
        this.kind = kindVal;
    }

    public int getKind() {
        return kind;
    }
}
