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
 * Use partition count to
 *  split all the hash value space and compute hash partition bound value of hash space
 *
 * @author chenghui.lch
 */
public class KeyPartBoundValBuilder implements PartBoundValBuilder {

    protected int partColCnt;
    protected int partitionCount;
    protected Long[][] upBoundValArr;
    
    public KeyPartBoundValBuilder(int partitionCount, int partColCnt) {
        this.partitionCount = partitionCount;
        this.partColCnt = partColCnt;
        this.upBoundValArr = initUpBoundValArr();
        partitionLongValueSpace(this.partitionCount);
    }
    
    @Override
    public Object getPartBoundVal(int partPosition) {
        return this.upBoundValArr[partPosition-1];
    }

    protected Long[][] initUpBoundValArr() {
        Long[][] upBoundValArr = new Long[partitionCount][partColCnt];
        for (int i = 0; i < partitionCount; i++) {
            for (int j = 0; j < partColCnt; j++) {
                upBoundValArr[i][j] = Long.MAX_VALUE;
            }
        }
        return upBoundValArr;
    }
    
    protected Long[][] partitionLongValueSpace(int partitionCount) {

        assert partitionCount > 0;
        if (partitionCount == 1) {
            upBoundValArr[0][0] = Long.MAX_VALUE;
            return upBoundValArr;
        }
        long minVal = Long.MIN_VALUE;
        long maxVal = Long.MAX_VALUE;
        long valInterval = 2 * (maxVal / partitionCount);
        long lastBoundVal = maxVal;

        upBoundValArr[partitionCount-1][0] = lastBoundVal;
        for (int j = partitionCount - 2; j >= 0; j--) {
            lastBoundVal = lastBoundVal - valInterval;
            upBoundValArr[j][0] = lastBoundVal;
        }
        return upBoundValArr;
    }
    
}
