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

package com.alibaba.polardbx.executor.backfill;

public class ThrottleInfo {

    private Long backFillId;
    private double speed;
    private long totalRows;

    public ThrottleInfo(Long backFillId, double speed, long totalRows) {
        this.backFillId = backFillId;
        this.speed = speed;
        this.totalRows = totalRows;
    }

    public Long getBackFillId() {
        return backFillId;
    }

    public void setBackFillId(Long backFillId) {
        this.backFillId = backFillId;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }
}
