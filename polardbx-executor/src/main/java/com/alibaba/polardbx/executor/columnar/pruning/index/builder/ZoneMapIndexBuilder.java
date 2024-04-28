package com.alibaba.polardbx.executor.columnar.pruning.index.builder;

import com.alibaba.polardbx.executor.columnar.pruning.index.ZoneMapIndex;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Map;

/**
 * builder for column zone map index
 *
 * @author fangwu
 */
public class ZoneMapIndexBuilder {
    private final Map<Integer, ArrayList<Object>> dataMap = Maps.newHashMap();
    private final Map<Integer, DataType> dtMap = Maps.newHashMap();
    private final Map<Integer, ArrayList<Boolean>> nullValMap = Maps.newHashMap();

    public ZoneMapIndexBuilder appendColumn(int columnId, DataType dataType) {
        Preconditions.checkArgument(columnId >= 0 && dataType != null,
            "bad data for zone map index:" + columnId + "," + dataType);
        dtMap.put(columnId, dataType);
        return this;
    }

    public ZoneMapIndexBuilder appendNull(int columnId, Boolean hasNull) {
        Preconditions.checkArgument(columnId >= 0 && hasNull != null,
            "bad data for zone map index:" + columnId + "," + hasNull);
        nullValMap.computeIfAbsent(columnId, i -> Lists.newArrayList()).add(hasNull);
        return this;
    }

    public ZoneMapIndexBuilder appendIntegerData(int columnId, Integer data) {
        Preconditions.checkArgument(columnId >= 0 && data != null,
            "bad data for zone map index:" + columnId + "," + data);
        dataMap.computeIfAbsent(columnId, i -> Lists.newArrayList()).add(data);
        return this;
    }

    public ZoneMapIndexBuilder appendLongData(int columnId, Long data) {
        Preconditions.checkArgument(columnId >= 0 && data != null,
            "bad data for zone map index:" + columnId + "," + data);
        dataMap.computeIfAbsent(columnId, i -> Lists.newArrayList()).add(data);
        return this;
    }

    public ZoneMapIndex build() {
        if (dataMap.size() == 0) {
            return null;
        }
        int rgNum = dataMap.values().iterator().next().size() / 2;

        // build null bitset
        Map<Integer, RoaringBitmap> rrMap = Maps.newHashMap();
        for (Map.Entry<Integer, ArrayList<Boolean>> entry : nullValMap.entrySet()) {
            ArrayList<Boolean> booleans = entry.getValue();
            RoaringBitmap rr = RoaringBitmap.bitmapOfRange(0, rgNum);
            for (int i = 0; i < booleans.size(); i++) {
                if (!booleans.get(i)) {
                    rr.flip(i);
                }
            }
        }
        return ZoneMapIndex.build(rgNum, dtMap, dataMap, rrMap);
    }

}
