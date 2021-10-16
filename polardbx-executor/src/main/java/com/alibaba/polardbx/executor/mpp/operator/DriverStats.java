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

package com.alibaba.polardbx.executor.mpp.operator;

public class DriverStats {

    private final long inputDataSize;
    private final long inputPositions;
    private final long outputDataSize;
    private final long outputPositions;

    public DriverStats(long inputDataSize, long inputPositions, long outputDataSize, long outputPositions) {
        this.inputDataSize = inputDataSize;
        this.inputPositions = inputPositions;
        this.outputDataSize = outputDataSize;
        this.outputPositions = outputPositions;
    }

    public long getInputDataSize() {
        return inputDataSize;
    }

    public long getInputPositions() {
        return inputPositions;
    }

    public long getOutputDataSize() {
        return outputDataSize;
    }

    public long getOutputPositions() {
        return outputPositions;
    }
}
