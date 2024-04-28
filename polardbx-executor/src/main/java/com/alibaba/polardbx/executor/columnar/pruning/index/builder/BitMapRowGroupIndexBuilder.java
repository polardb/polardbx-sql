package com.alibaba.polardbx.executor.columnar.pruning.index.builder;

import com.alibaba.polardbx.executor.columnar.pruning.index.BitMapRowGroupIndex;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.roaringbitmap.RoaringBitmap;

import java.util.Map;

/**
 * builder for column bitmap index
 *
 * @author fangwu
 */
public class BitMapRowGroupIndexBuilder {
    private int rgNum = 0;
    private final Map<Integer, Map<String, RoaringBitmap>> valMap = Maps.newHashMap();
    private final Map<Integer, DataType> dtMap = Maps.newHashMap();

    public BitMapRowGroupIndexBuilder appendColumn(int columnId, DataType dataType) {
        Preconditions.checkArgument(columnId > 0 && dataType != null,
            "bad data for zone map index:" + columnId + "," + dataType);
        dtMap.put(columnId, dataType);
        return this;
    }

    public BitMapRowGroupIndexBuilder appendValue(int columnId, String val, RoaringBitmap rb) {
        Preconditions.checkArgument(columnId > 0 && val != null,
            "bad data for bitmap index:" + columnId + "," + val);
        valMap.computeIfAbsent(columnId, i -> Maps.newHashMap()).put(val, rb);
        return this;
    }

    public BitMapRowGroupIndex build() {
        if (valMap.size() == 0 || dtMap.size() == 0 || rgNum == 0) {
            return null;
        }
        return new BitMapRowGroupIndex(rgNum, valMap, dtMap);
    }

    public void setRgNum(int rgNum) {
        this.rgNum = rgNum;
    }

    public boolean supportColumn(int columnId) {
        return dtMap.containsKey(columnId);
    }
}
