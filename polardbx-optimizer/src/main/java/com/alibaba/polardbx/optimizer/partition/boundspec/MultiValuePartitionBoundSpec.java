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

package com.alibaba.polardbx.optimizer.partition.boundspec;

import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MultiValuePartitionBoundSpec: BoundSpec which contains multiple datums
 * <p>
 * List/List Column Partition
 *
 * @author moyi
 * @since 2021/04
 */
public class MultiValuePartitionBoundSpec extends PartitionBoundSpec {

    protected List<SearchDatumInfo> datums;
    protected boolean isDefault = false;

    public MultiValuePartitionBoundSpec() {
    }

    public MultiValuePartitionBoundSpec(MultiValuePartitionBoundSpec other) {
        super(other);
        this.datums = new ArrayList<>(other.datums.size());
        this.isDefault = other.isDefault();
        for (SearchDatumInfo datum : other.datums) {
            this.datums.add(datum.copy());
        }
    }

    @Override
    public List<SearchDatumInfo> getMultiDatums() {
        return this.datums;
    }

    @Override
    public void setMultiDatums(List<SearchDatumInfo> datums) {
        this.datums = datums;
    }

    @Override
    public SearchDatumInfo getSingleDatum() {
        throw new RuntimeException("MultiValuePartitionBoundSpec not support");
    }

    @Override
    public void setSingleDatum(SearchDatumInfo datum) {
        throw new RuntimeException("MultiValuePartitionBoundSpec not support");
    }

    @Override
    public boolean isSingleValue() {
        return false;
    }

    @Override
    public boolean containNullValues() {
        boolean containNullVal = false;
        for (int i = 0; i < datums.size(); i++) {
            containNullVal = containNullVal || datums.get(i).getSingletonValue().isNullValue();
        }
        return containNullVal;
    }

    @Override
    public boolean containMaxValues() {
        for (int i = 0; i < datums.size(); i++) {
            PartitionBoundVal[] bndArr = datums.get(i).getDatumInfo();
            for (int j = 0; j < bndArr.length; j++) {
                if (bndArr[j].isMaxValue()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isDefaultPartSpec() {
        return isDefault;
    }

    @Override
    public PartitionBoundSpec copy() {
        return new MultiValuePartitionBoundSpec(this);
    }

    /**
     * Multi-Column datum need be surrounded with brace, which single-column datum not.
     */
    @Override
    public String getPartitionBoundDescription(int prefixPartColCnt) {
        return this.datums.stream()
            .map(datum -> datum.getDesc(datum.size() > 1, prefixPartColCnt))
            .collect(Collectors.joining(","));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj != null && obj.getClass() == this.getClass() && this.datums != null
            && this.datums.size() == ((MultiValuePartitionBoundSpec) obj).datums.size()) {
            for (int i = 0; i < datums.size(); i++) {
                if (!datums.get(i).equals(((MultiValuePartitionBoundSpec) obj).datums.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object obj, int prefixPartColCnt) {
        return equals(obj);
    }

    public boolean isDefault() {
        return isDefault;
    }

    public void setDefault(boolean aDefault) {
        isDefault = aDefault;
    }
}
