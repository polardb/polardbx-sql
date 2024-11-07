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

package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import static com.alibaba.polardbx.common.properties.ConnectionParams.ZONEMAP_MAX_GROUP_SIZE;

/**
 * Zone map index for columnar scan pruning
 * this index contains index data values for multi columns
 * in one orc file.
 * it serv two interface:
 * - pruneNull(col id):xx column is null
 * - prune(col id, min value, max value) : get rg range
 * calculate by min/max value
 *
 * @author fangwu
 */
public class ZoneMapIndex extends BaseColumnIndex {
    private final Map<Integer, ArrayList<Object>> dataMap;
    private final Map<Integer, DataType> dtMap;
    private final Map<Integer, RoaringBitmap> nullValMap;

    // internal fields
    private Map<Integer, Collection<RoaringBitmap>> groupDataMap = Maps.newConcurrentMap();

    private ZoneMapIndex(long rgNum, Map<Integer, DataType> dtMap, Map<Integer, ArrayList<Object>> dataMap,
                         Map<Integer, RoaringBitmap> nullValMap) {
        super(rgNum);
        this.dtMap = dtMap;
        this.dataMap = dataMap;
        this.nullValMap = nullValMap;

        int maxGroupSize = InstConfUtil.getInt(ZONEMAP_MAX_GROUP_SIZE);
        for (int index : dataMap.keySet()) {
            ArrayList<Object> dataTemp = dataMap.get(index);
            Map<String, RoaringBitmap> groupData = Maps.newConcurrentMap();
            boolean valid = true;
            Function<Pair<Object, Object>, String> keyFunc = null;
            DataType dt = dtMap.get(index);
            if (DataTypes.LongType.equals(dt) ||
                DataTypes.IntegerType.equals(dt) ||
                DataTypes.TimestampType.equals(dt) ||
                DataTypes.DatetimeType.equals(dt) ||
                DataTypes.DateType.equals(dt) ||
                DataTypes.TimeType.equals(dt)) {
                if (dataTemp.get(0) instanceof Long) {
                    keyFunc = (pair) -> {
                        Long start = (Long) pair.getKey();
                        Long end = (Long) pair.getValue();
                        return start + "_" + end;
                    };
                } else {
                    keyFunc = (pair) -> {
                        Integer start = (Integer) pair.getKey();
                        Integer end = (Integer) pair.getValue();
                        return start + "_" + end;
                    };
                }
            }

            if (keyFunc == null) {
                continue;
            }
            for (int i = 0; i < dataTemp.size() / 2; i++) {
                Object start = dataTemp.get(i * 2);
                Object end = dataTemp.get(i * 2 + 1);
                Pair<Object, Object> pair = Pair.of(start, end);
                String key = keyFunc.apply(pair);
                final int groupIndex = i;
                groupData.compute(key, (s, roaringBitmap) -> {
                    if (roaringBitmap == null) {
                        RoaringBitmap r = new RoaringBitmap();
                        r.add(groupIndex);
                        return r;
                    } else {
                        roaringBitmap.add(groupIndex);
                        return roaringBitmap;
                    }
                });
                if (groupData.size() > (dataTemp.size() / 10) || groupData.size() > maxGroupSize) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                groupDataMap.put(index, groupData.values());
            }
        }
    }

    public static ZoneMapIndex build(long rgNum, Map<Integer, DataType> dtMap, Map<Integer, ArrayList<Object>> dataMap,
                                     Map<Integer, RoaringBitmap> nullValMap) {
        Preconditions.checkArgument(!(dtMap == null && dataMap == null && nullValMap == null),
            "bad data for zone map index");
        Preconditions.checkArgument(rgNum > 0, "bad rg num:" + rgNum);
        return new ZoneMapIndex(rgNum, dtMap, dataMap, nullValMap);
    }

    /**
     * if any row group has null value, then its value is true
     * return true/false array for target column
     */
    public void pruneNull(int colId, RoaringBitmap cur) {
        if (nullValMap.containsKey(colId)) {
            cur.and(nullValMap.get(colId));
        }
    }

    /**
     * prune range
     *
     * @param colId target column id
     * @param start lower value
     * @param includeStart is lower value included
     * @param end upper value
     * @param includeEnd is upper value included
     */
    public void prune(int colId, Object startObj, boolean includeStart, Object endObj, boolean includeEnd,
                      RoaringBitmap cur) {
        //startObj/endObj == null means lowerBound/UpperBound is unlimited
        // paramTransform() == null means type of startObj is unsupported
        Object start;
        if (startObj == null) {
            //lower bound is unlimited
            start = null;
        } else {
            start = paramTransform(startObj, dtMap.get(colId), Long.class);
            //type is unsupported
            if (start == null) {
                return;
            }
        }

        Object end;
        if (endObj == null) {
            //upper bound is unlimited
            end = null;
        } else {
            end = paramTransform(endObj, dtMap.get(colId), Long.class);
            //type is unsupported
            if (end == null) {
                return;
            }
        }
        DataType dt = dtMap.get(colId);
        if (start != null && end != null && dt.compare(start, end) > 0) {
            cur.and(new RoaringBitmap());
            return;
        }
        if (includeStart) {
            search(colId, start, SqlKind.GREATER_THAN_OR_EQUAL, cur);
        } else {
            search(colId, start, SqlKind.GREATER_THAN, cur);
        }

        if (includeEnd) {
            search(colId, end, SqlKind.LESS_THAN_OR_EQUAL, cur);
        } else {
            search(colId, end, SqlKind.LESS_THAN, cur);
        }
    }

    /**
     * search each zone for the target value and sql kind
     *
     * @param colId target column id
     * @param target target value
     * @param sqlKind represent operator: EQUALS,GREATER_THAN,
     * GREATER_THAN_OR_EQUAL,LESS_THAN,LESS_THAN_OR_EQUAL
     */
    private void search(int colId, Object target, SqlKind sqlKind, RoaringBitmap cur) {
        ArrayList<Object> data = dataMap.get(colId);
        DataType dt = dtMap.get(colId);
        long rgNum = rgNum();
        if (data == null || dt == null || target == null) {
            return;
        }
        if (groupDataMap.get(colId) != null) {
            for (RoaringBitmap r : groupDataMap.get(colId)) {
                int targetPair = r.first();
                switch (sqlKind) {
                case EQUALS:
                    if (dt.compare(target, data.get(targetPair * 2)) < 0 ||
                        dt.compare(data.get(targetPair * 2 + 1), target) > 0) {
                        cur.andNot(r);
                    }
                    break;
                case GREATER_THAN:
                    if (dt.compare(data.get(targetPair * 2 + 1), target) <= 0) {
                        cur.andNot(r);
                    }
                    break;
                case GREATER_THAN_OR_EQUAL:
                    if (dt.compare(data.get(targetPair * 2 + 1), target) < 0) {
                        cur.andNot(r);
                    }
                    break;
                case LESS_THAN:
                    if (dt.compare(data.get(targetPair * 2), target) >= 0) {
                        cur.andNot(r);
                    }
                    break;
                case LESS_THAN_OR_EQUAL:
                    if (dt.compare(data.get(targetPair * 2), target) > 0) {
                        cur.andNot(r);
                    }
                }
            }
        } else {
            for (int i = 0; i < rgNum; i++) {
                if (!cur.contains(i)) {
                    continue;
                }
                switch (sqlKind) {
                case EQUALS:
                    if (dt.compare(target, data.get(i * 2)) < 0 ||
                        dt.compare(data.get(i * 2 + 1), target) > 0) {
                        cur.flip(i);
                    }
                    break;
                case GREATER_THAN:
                    if (dt.compare(data.get(i * 2 + 1), target) <= 0) {
                        cur.flip(i);
                    }
                    break;
                case GREATER_THAN_OR_EQUAL:
                    if (dt.compare(data.get(i * 2 + 1), target) < 0) {
                        cur.flip(i);
                    }
                    break;
                case LESS_THAN:
                    if (dt.compare(data.get(i * 2), target) >= 0) {
                        cur.flip(i);
                    }
                    break;
                case LESS_THAN_OR_EQUAL:
                    if (dt.compare(data.get(i * 2), target) > 0) {
                        cur.flip(i);
                    }
                }
            }
        }

    }

    @Override
    public boolean checkSupport(int columnId, SqlTypeName type) {
        if (!dtMap.containsKey(columnId)) {
            return false;
        }
        return type == SqlTypeName.BIGINT ||
            type == SqlTypeName.INTEGER ||
            type == SqlTypeName.YEAR ||
            type == SqlTypeName.DATE ||
            type == SqlTypeName.DATETIME;
    }

    @Override
    public DataType getColumnDataType(int columnId) {
        return dtMap.get(columnId);
    }

    public String colIds() {
        return StringUtils.join(dtMap.keySet(), ",");
    }

    public int groupSize(int colId) {
        if (groupDataMap == null || groupDataMap.get(colId) == null) {
            return 0;
        }
        return groupDataMap.get(colId).size();
    }
}
