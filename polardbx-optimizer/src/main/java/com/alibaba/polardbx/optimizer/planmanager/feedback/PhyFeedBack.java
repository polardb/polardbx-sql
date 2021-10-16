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

package com.alibaba.polardbx.optimizer.planmanager.feedback;

import java.util.List;

public class PhyFeedBack {
    //    private long baselineId;
//    private Point bucket;
    private long examinedRowCount;
    private List<String[]> chosenIndexes;

    public PhyFeedBack(long examinedRowCount, List<String[]> chosenIndexes) {
//        this.baselineId = baselineId;
//        this.bucket = bucket;
        this.examinedRowCount = examinedRowCount;
        this.chosenIndexes = chosenIndexes;
    }
//
//    public long getBaselineId() {
//        return baselineId;
//    }
//
//    public Point getBucket() {
//        return bucket;
//    }

    public long getExaminedRowCount() {
        return examinedRowCount;
    }

    public List<String[]> getChosenIndexes() {
        return chosenIndexes;
    }
}
