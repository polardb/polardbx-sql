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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SearchDatumInfo: a tuple/row of values
 * <p>
 * Compare: use SearchDatumComparator
 * Hash: use SearchDatumHasher
 * <p>
 * TODO(moyi) optimize single value DatumInfo
 *
 * @author chenghui.lch
 */
public class SearchDatumInfo {

    protected PartitionBoundVal[] datumInfo;

    public SearchDatumInfo(List<PartitionBoundVal> boundValList) {
        PartitionBoundVal[] bndValArr = new PartitionBoundVal[boundValList.size()];
        for (int i = 0; i < boundValList.size(); i++) {
            bndValArr[i] = boundValList.get(i);
        }
        this.datumInfo = bndValArr;
    }

    public SearchDatumInfo(PartitionBoundVal boundVal) {
        PartitionBoundVal[] datumInfo = new PartitionBoundVal[1];
        datumInfo[0] = boundVal;
        this.datumInfo = datumInfo;
    }

    public SearchDatumInfo(PartitionBoundVal[] datumInfo) {
        this.datumInfo = datumInfo;
    }

    public static SearchDatumInfo createMaxValDatumInfo(int colCnt) {
        PartitionBoundVal[] datumInfo = new PartitionBoundVal[colCnt];
        for (int i = 0; i < colCnt; i++) {
            datumInfo[i] = PartitionBoundVal.createMaxValue();
        }
        return new SearchDatumInfo(datumInfo);
    }

    public static SearchDatumInfo createMinValDatumInfo(int colCnt) {
        PartitionBoundVal[] datumInfo = new PartitionBoundVal[colCnt];
        for (int i = 0; i < colCnt; i++) {
            datumInfo[i] = PartitionBoundVal.createMinValue();
        }
        return new SearchDatumInfo(datumInfo);
    }

    /**
     * Create a datum from raw value, which need inspect types from partition
     */
    public static SearchDatumInfo createFromField(PartitionField value) {
        return new SearchDatumInfo(PartitionBoundVal.createNormalValue(value));
    }

    /**
     * Create a datum from hash code
     */
    public static SearchDatumInfo createFromHashCode(long hashCode) {
        PartitionField field = PartitionFieldBuilder.createField(DataTypes.LongType);
        field.store(hashCode, DataTypes.LongType);
        return SearchDatumInfo.createFromField(field);
    }

    /**
     * Create a datum from multiple hashcode
     */
    public static SearchDatumInfo createFromHashCodes(Long[] hashCodes) {
        List<PartitionField> fields = Arrays.stream(hashCodes).map(x -> {
            PartitionField field = PartitionFieldBuilder.createField(DataTypes.LongType);
            field.store(x, DataTypes.LongType);
            return field;
        }).collect(Collectors.toList());
        return SearchDatumInfo.createFromFields(fields);
    }

    /**
     * Create a datum from fields
     *
     * @param fields datum fields
     * @return created datum
     */
    public static SearchDatumInfo createFromFields(List<PartitionField> fields) {
        List<PartitionBoundVal> boundValues = fields.stream()
            .map(PartitionBoundVal::createNormalValue)
            .collect(Collectors.toList());
        return new SearchDatumInfo(boundValues);
    }

    public static SearchDatumInfo createFromObjects(List<DataType> types, List<Object> objects) {
        assert types.size() == objects.size();

        List<PartitionField> fields = new ArrayList<>(types.size());
        for (int i = 0; i < types.size(); i++) {
            Object value = objects.get(i);
            DataType dataType = types.get(i);
            PartitionField field = PartitionFieldBuilder.createField(dataType);
            field.store(value, dataType);

            fields.add(field);
        }
        return createFromFields(fields);
    }

    public PartitionBoundVal[] getDatumInfo() {
        return datumInfo;
    }

    public int size() {
        return this.datumInfo.length;
    }

    public PartitionBoundVal getSingletonValue() {
        if (this.datumInfo.length > 1) {
            throw new RuntimeException("multiple datum");
        }
        return this.datumInfo[0];
    }

    public SearchDatumInfo copy() {
        PartitionBoundVal[] values = new PartitionBoundVal[this.datumInfo.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = this.datumInfo[i].copy();
        }
        return new SearchDatumInfo(values);
    }

    public String getDesc(boolean withBracket, int prefixPartColCnt) {

        int targetPartColCnt = this.datumInfo.length;
        PartitionBoundVal[] targetDatumInfo = this.datumInfo;
        if (prefixPartColCnt != PartitionInfoUtil.FULL_PART_COL_COUNT) {
            targetPartColCnt = prefixPartColCnt;
            targetDatumInfo = new PartitionBoundVal[prefixPartColCnt];
            for (int i = 0; i < targetPartColCnt; i++) {
                targetDatumInfo[i] = this.datumInfo[i];
            }
        }

        String str = StringUtils.join(targetDatumInfo, ",");
        if (withBracket) {
            return "(" + str + ")";
        } else {
            return str;
        }
    }

    @Override
    public String toString() {
        return getDesc(true, PartitionInfoUtil.FULL_PART_COL_COUNT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SearchDatumInfo)) {
            return false;
        }
        SearchDatumInfo that = (SearchDatumInfo) o;
        return Arrays.equals(datumInfo, that.datumInfo);
    }

    public boolean equals(Object o, int prefixPartColCnt) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SearchDatumInfo)) {
            return false;
        }
        SearchDatumInfo that = (SearchDatumInfo) o;
        if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
            return Arrays.equals(datumInfo, that.datumInfo);
        } else {
            PartitionBoundVal[] thisArr = new PartitionBoundVal[prefixPartColCnt];
            PartitionBoundVal[] thatArr = new PartitionBoundVal[prefixPartColCnt];
            for (int i = 0; i < prefixPartColCnt; i++) {
                thisArr[i] = datumInfo[i];
                thatArr[i] = that.datumInfo[i];
            }
            return Arrays.equals(thisArr, thatArr);
        }
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(datumInfo);
    }
}
