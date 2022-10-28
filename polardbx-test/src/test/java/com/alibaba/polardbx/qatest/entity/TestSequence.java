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

package com.alibaba.polardbx.qatest.entity;

public class TestSequence {

    private int id;
    private String name;
    private long value;
    private long incrementBy;
    private long startWith;
    private long maxValue;
    private String cycle;

    private long unitCount;
    private long unitIndex;
    private long innerStep;

    public TestSequence() {

    }

    public TestSequence(int id, String name, long value, long incrementBy, long startWith, long maxValue, String cycle,
                        long unitCount, long unitIndex, long innerStep) {
        this.id = id;
        this.name = name;
        this.value = value;
        this.incrementBy = incrementBy;
        this.startWith = startWith;
        this.maxValue = maxValue;
        this.cycle = cycle;
        this.unitCount = unitCount;
        this.unitIndex = unitIndex;
        this.innerStep = innerStep;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getIncrementBy() {
        return incrementBy;
    }

    public void setIncrementBy(long incrementBy) {
        this.incrementBy = incrementBy;
    }

    public long getStartWith() {
        return startWith;
    }

    public void setStartWith(long start_with) {
        this.startWith = start_with;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        this.maxValue = maxValue;
    }

    public String getCycle() {
        return cycle;
    }

    public void setCycle(String cycle) {
        this.cycle = cycle;
    }

    public long getUnitCount() {
        return unitCount;
    }

    public void setUnitCount(long unitCount) {
        this.unitCount = unitCount;
    }

    public long getUnitIndex() {
        return unitIndex;
    }

    public void setUnitIndex(long unitIndex) {
        this.unitIndex = unitIndex;
    }

    public long getInnerStep() {
        return innerStep;
    }

    public void setInnerStep(long innerStep) {
        this.innerStep = innerStep;
    }
}
