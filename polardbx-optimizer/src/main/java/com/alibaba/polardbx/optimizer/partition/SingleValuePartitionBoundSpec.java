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

import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;

import java.util.List;

/**
 * SingleValuePartitionBoundSpec: Hash/Key/Range/RangeColumn Partition
 *
 * @author moyi
 * @since 2021/04
 */
public class SingleValuePartitionBoundSpec extends PartitionBoundSpec {
    protected SearchDatumInfo datum;

    SingleValuePartitionBoundSpec() {
    }

    SingleValuePartitionBoundSpec(SingleValuePartitionBoundSpec other) {
        super(other);
        this.datum = other.datum.copy();
    }

    @Override
    public List<SearchDatumInfo> getMultiDatums() {
        throw new RuntimeException("SingleValuePartitionBoundSpec not support");
    }

    @Override
    public void setMultiDatums(List<SearchDatumInfo> datums) {
        throw new RuntimeException("SingleValuePartitionBoundSpec not support");
    }

    @Override
    public SearchDatumInfo getSingleDatum() {
        return this.datum;
    }

    @Override
    public void setSingleDatum(SearchDatumInfo datum) {
        this.datum = datum;
    }

    @Override
    public boolean isSingleValue() {
        return true;
    }

    @Override
    public boolean containNullValues() {
        PartitionBoundVal[] bndArr = datum.getDatumInfo();
        for (int i = 0; i < bndArr.length; i++) {
            if (bndArr[i].isNullValue()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containMaxValues() {
        PartitionBoundVal[] bndArr = datum.getDatumInfo();
        for (int i = 0; i < bndArr.length; i++) {
            if (bndArr[i].isMaxValue()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isDefaultPartSpec() {
        return false;
    }

    @Override
    public String getPartitionBoundDescription(int prefixPartColCnt) {
        return this.datum.getDesc(false, prefixPartColCnt);
    }

    public PartitionBoundVal getBoundValue() {
        return this.datum.getSingletonValue();
    }

    public void setBoundValue(PartitionBoundVal value) {
        this.datum = new SearchDatumInfo(value);
    }

    // TODO too low-level interface, should not be exposed
    public PartitionField getRAWValue() {
        return this.datum.getSingletonValue().getValue();
    }

    // TODO too low-level interface, should not be exposed
    public void setRAWValue(PartitionField value) {
        this.datum.getSingletonValue().setValue(value);
    }

    @Override
    public PartitionBoundSpec copy() {
        return new SingleValuePartitionBoundSpec(this);
    }

    @Override
    public int hashCode() {
        return this.datum.hashCode();
    }

//    @Override
//    public boolean equals(Object obj) {
//        if (this == obj) {
//            return true;
//        }
//        if (obj != null && obj.getClass() == this.getClass() && this.datum != null) {
//            return this.datum.equals(((SingleValuePartitionBoundSpec) obj).getSingleDatum());
//        }
//        return false;
//    }

    @Override
    public boolean equals(Object obj) {
        return equals(obj, -1);
    }

    @Override
    public boolean equals(Object obj, int prefixPartColCnt ) {

        if (this == obj) {
            return true;
        }
        if (obj != null && obj.getClass() == this.getClass() && this.datum != null) {
            if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                return this.datum.equals(((SingleValuePartitionBoundSpec) obj).getSingleDatum());
            } else {
                return this.datum.equals(((SingleValuePartitionBoundSpec) obj).getSingleDatum(), prefixPartColCnt);
            }
        }
        return false;


    }
}
