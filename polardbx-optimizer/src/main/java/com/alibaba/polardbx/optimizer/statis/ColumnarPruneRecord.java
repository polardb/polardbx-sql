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

package com.alibaba.polardbx.optimizer.statis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author fangwu
 */
public class ColumnarPruneRecord {
    public String tableName;
    public String filter;
    public AtomicLong initIndexTime = new AtomicLong();
    public AtomicLong indexPruneTime = new AtomicLong();
    public AtomicInteger fileNum = new AtomicInteger();
    public AtomicInteger stripeNum = new AtomicInteger();
    public AtomicInteger rgNum = new AtomicInteger();
    public AtomicInteger rgLeftNum = new AtomicInteger();
    public AtomicInteger sortKeyPruneNum = new AtomicInteger();
    public AtomicInteger zoneMapPruneNum = new AtomicInteger();
    public AtomicInteger bitMapPruneNum = new AtomicInteger();

    public ColumnarPruneRecord(
        String tableName,
        String filter
    ) {
        this.tableName = tableName;
        this.filter = filter;
    }

    @JsonCreator
    public ColumnarPruneRecord(@JsonProperty("tableName") String tableName,
                               @JsonProperty("filter") String filter,
                               @JsonProperty("initIndexTime") AtomicLong initIndexTime,
                               @JsonProperty("indexPruneTime") AtomicLong indexPruneTime,
                               @JsonProperty("fileNum") AtomicInteger fileNum,
                               @JsonProperty("stripeNum") AtomicInteger stripeNum,
                               @JsonProperty("rgNum") AtomicInteger rgNum,
                               @JsonProperty("rgLeftNum") AtomicInteger rgLeftNum,
                               @JsonProperty("sortKeyPruneNum") AtomicInteger sortKeyPruneNum,
                               @JsonProperty("zoneMapPruneNum") AtomicInteger zoneMapPruneNum,
                               @JsonProperty("bitMapPruneNum") AtomicInteger bitMapPruneNum
    ) {
        this.tableName = tableName;
        this.filter = filter;
        this.initIndexTime = initIndexTime;
        this.indexPruneTime = indexPruneTime;
        this.fileNum = fileNum;
        this.stripeNum = stripeNum;
        this.rgNum = rgNum;
        this.rgLeftNum = rgLeftNum;
        this.sortKeyPruneNum = sortKeyPruneNum;
        this.zoneMapPruneNum = zoneMapPruneNum;
        this.bitMapPruneNum = bitMapPruneNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnarPruneRecord)) {
            return false;
        }
        ColumnarPruneRecord that = (ColumnarPruneRecord) o;
        return this.tableName.equals(that.tableName) &&
            this.filter.equals(that.filter) &&
            this.initIndexTime.get() == that.initIndexTime.get() &&
            this.indexPruneTime.get() == that.indexPruneTime.get() &&
            this.fileNum.get() == that.fileNum.get() &&
            this.stripeNum.get() == that.stripeNum.get() &&
            this.rgNum.get() == that.rgNum.get() &&
            this.rgLeftNum.get() == that.rgLeftNum.get() &&
            this.sortKeyPruneNum.get() == that.sortKeyPruneNum.get() &&
            this.zoneMapPruneNum.get() == that.zoneMapPruneNum.get() &&
            this.bitMapPruneNum.get() == that.bitMapPruneNum.get();
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonProperty
    public String getFilter() {
        return filter;
    }

    @JsonProperty
    public void setFilter(String filter) {
        this.filter = filter;
    }

    @JsonProperty
    public AtomicLong getInitIndexTime() {
        return initIndexTime;
    }

    @JsonProperty
    public void setInitIndexTime(AtomicLong initIndexTime) {
        this.initIndexTime = initIndexTime;
    }

    @JsonProperty
    public AtomicLong getIndexPruneTime() {
        return indexPruneTime;
    }

    @JsonProperty
    public void setIndexPruneTime(AtomicLong indexPruneTime) {
        this.indexPruneTime = indexPruneTime;
    }

    @JsonProperty
    public AtomicInteger getFileNum() {
        return fileNum;
    }

    @JsonProperty
    public void setFileNum(AtomicInteger fileNum) {
        this.fileNum = fileNum;
    }

    @JsonProperty
    public AtomicInteger getStripeNum() {
        return stripeNum;
    }

    @JsonProperty
    public void setStripeNum(AtomicInteger stripeNum) {
        this.stripeNum = stripeNum;
    }

    @JsonProperty
    public AtomicInteger getRgNum() {
        return rgNum;
    }

    @JsonProperty
    public void setRgNum(AtomicInteger rgNum) {
        this.rgNum = rgNum;
    }

    @JsonProperty
    public AtomicInteger getRgLeftNum() {
        return rgLeftNum;
    }

    @JsonProperty
    public void setRgLeftNum(AtomicInteger rgLeftNum) {
        this.rgLeftNum = rgLeftNum;
    }

    @JsonProperty
    public AtomicInteger getSortKeyPruneNum() {
        return sortKeyPruneNum;
    }

    @JsonProperty
    public void setSortKeyPruneNum(AtomicInteger sortKeyPruneNum) {
        this.sortKeyPruneNum = sortKeyPruneNum;
    }

    @JsonProperty
    public AtomicInteger getZoneMapPruneNum() {
        return zoneMapPruneNum;
    }

    @JsonProperty
    public void setZoneMapPruneNum(AtomicInteger zoneMapPruneNum) {
        this.zoneMapPruneNum = zoneMapPruneNum;
    }

    @JsonProperty
    public AtomicInteger getBitMapPruneNum() {
        return bitMapPruneNum;
    }

    @JsonProperty
    public void setBitMapPruneNum(AtomicInteger bitMapPruneNum) {
        this.bitMapPruneNum = bitMapPruneNum;
    }
}
