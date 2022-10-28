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

package com.alibaba.polardbx.common.statementsummary.model;

import lombok.Data;

import java.util.Iterator;

/**
 * @author busu
 * date: 2021/11/8 5:38 下午
 */
@Data
public class StatementSummaryByDigest implements Iterable<StatementSummaryElementByDigest> {

    private final String schema;
    private final int templateHash;
    private final int planHash;
    private final String sqlSample;
    private final String sqlTemplateText;
    private final String sqlType;
    private final int prevTemplateHash;
    private final String prevSqlTemplate;
    private final String sampleTraceId;
    private final String workloadType;
    private final String executeMode;
    /**
     * A cyclic queue
     */
    private StatementSummaryElementByDigest[] data;
    private int size;
    private int endIndex;
    private int capacity;

    public StatementSummaryByDigest() {
        this("", 0, 0, "", "", "", 0, "", "", "", "");
    }

    public StatementSummaryByDigest(String schema, int templateHash,
                                    int planHash, String sqlSample, String sqlTemplateText, String sqlType,
                                    int prevTemplateHash,
                                    String prevSqlTemplate, String sampleTraceId, String workloadType,
                                    String executeMode) {
        this.schema = schema;
        this.templateHash = templateHash;
        this.planHash = planHash;
        this.sqlSample = sqlSample;
        this.sqlTemplateText = sqlTemplateText;
        this.sqlType = sqlType;
        this.capacity = 1;
        this.data = new StatementSummaryElementByDigest[this.capacity];
        this.endIndex = 0;
        this.size = 0;
        this.prevTemplateHash = prevTemplateHash;
        this.prevSqlTemplate = prevSqlTemplate;
        this.sampleTraceId = sampleTraceId;
        this.workloadType = workloadType;
        this.executeMode = executeMode;
    }

    public synchronized void setHistorySize(int historySize) {
        int newCapacity = historySize + 1;
        if (newCapacity != this.capacity) {
            StatementSummaryElementByDigest[] newData = new StatementSummaryElementByDigest[newCapacity];
            int startIndex = (this.endIndex - this.size + this.capacity) % this.capacity;
            int newSize = Math.min(this.size, newCapacity);
            for (int i = 0; i < newSize; ++i) {
                newData[i] = this.data[(startIndex + i) % this.capacity];
            }
            this.data = newData;
            this.size = newSize;
            this.capacity = newCapacity;
            this.endIndex = this.size % this.capacity;
        }
    }

    public synchronized void add(ExecInfo execInfo, int historySize, long refreshInterval) {
        //historySize which might be changed when the server is running
        setHistorySize(historySize);
        long alignedBeginTime = execInfo.getTimestamp() / refreshInterval * refreshInterval;
        StatementSummaryElementByDigest element = null;

        for (int i = 0; i < this.size; ++i) {
            int index = (endIndex - 1 + this.capacity - i) % this.capacity;
            StatementSummaryElementByDigest currentElement = this.data[index];
            if (alignedBeginTime > currentElement.getBeginTime()) {
                if (i == 0) {
                    element = createOneElement(alignedBeginTime);
                }
                break;
            }
            if (alignedBeginTime == currentElement.getBeginTime()) {
                // use the lastest element
                element = currentElement;
                break;
            }
        }

        if (element == null && this.size == 0) {
            // expire the lastest element and create a new one
            element = createOneElement(alignedBeginTime);
        }
        if (element != null) {
            element.merge(execInfo);
        }
    }

    private StatementSummaryElementByDigest createOneElement(long beginTime) {
        StatementSummaryElementByDigest element = this.data[endIndex];
        if (element != null) {
            //reuse the object
            element.clear();
        } else {
            element = this.data[endIndex] = new StatementSummaryElementByDigest();
            this.size++;
        }
        endIndex = (endIndex + 1) % this.capacity;
        element.setBeginTime(beginTime);
        return element;
    }

    public synchronized void merge(StatementSummaryByDigest statementSummaryByDigest, int historySize) {
        Iterator<StatementSummaryElementByDigest> thisElements = this.iterator();
        Iterator<StatementSummaryElementByDigest> otherElements = statementSummaryByDigest.iterator();
        StatementSummaryElementByDigest[] newData = new StatementSummaryElementByDigest[historySize + 1];
        StatementSummaryElementByDigest thisElement = null;
        StatementSummaryElementByDigest otherElement = null;
        int index = 0;
        while (index < newData.length) {
            if (thisElement == null && thisElements.hasNext()) {
                thisElement = thisElements.next();
            }
            if (otherElement == null && otherElements.hasNext()) {
                otherElement = otherElements.next();
            }
            if (thisElement != null && otherElement != null) {
                if (thisElement.getBeginTime() == otherElement.getBeginTime()) {
                    //merge the element
                    thisElement.merge(otherElement);
                    newData[index++] = thisElement;
                    thisElement = otherElement = null;
                } else if (thisElement.getBeginTime() < otherElement.getBeginTime()) {
                    newData[index++] = thisElement;
                    thisElement = null;
                } else {
                    newData[index++] = otherElement;
                    otherElement = null;
                }
            } else if (thisElement == null && otherElement != null) {
                newData[index++] = otherElement;
                otherElement = null;
                continue;
            } else if (thisElement != null && otherElement == null) {
                newData[index++] = thisElement;
                thisElement = null;
            } else {
                break;
            }
        }
        this.size = index;
        this.capacity = newData.length;
        if (data.length >= this.capacity && data.length < this.capacity * 2) {
            //reuse the data array
            System.arraycopy(newData, 0, this.data, 0, newData.length);
            //release obj by set the reference null
            for (int i = newData.length; i < this.data.length; ++i) {
                this.data[i] = null;
            }
        } else {
            this.data = newData;
        }
        this.endIndex = this.size % this.capacity;
    }

    @Override
    public Iterator<StatementSummaryElementByDigest> iterator() {
        return new Iterator<StatementSummaryElementByDigest>() {
            private int remainingCount = size;

            @Override
            public boolean hasNext() {
                return remainingCount > 0;
            }

            @Override
            public StatementSummaryElementByDigest next() {
                return data[(endIndex - (remainingCount--) + capacity) % capacity];
            }
        };
    }

}
