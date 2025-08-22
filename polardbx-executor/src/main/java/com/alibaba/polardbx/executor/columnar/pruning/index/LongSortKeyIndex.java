package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import groovy.sql.Sql;
import org.apache.calcite.sql.type.SqlTypeName;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.Objects;

/**
 * Sort Key Index for Columnar pruning, each Sort key index for one orc file.
 * this index only support number/date datatype, which could being easily transformed&compared
 * by long value.
 * <p>
 * data the long array contains both min/max value for each row group,so the rg num equals data.length/2. like:
 * RG data: rg1(1000, 2000), rg2(2000, 3000), rg3(4000, 5000), rg4(5000, 5001)
 * index data: [1000, 2000, 2000, 3000, 4000, 5000, 5000, 5001]
 * <p>
 * Sort key index provided two type pruning interface:pruneEqual/pruneRange
 * <p>
 * pruneEqual will return 0~data_length/2 rgs.
 * - if target value was between any min/max values of rg(not included min/max value), return that rg
 * - if target value was between any two rgs, return 0 rg. like target value(3500) won't return any rg
 * - if target value was contain by multi rgs, return them. like target value(5000) would return rg3 and rg4
 * <p>
 * pruneEqual is a special case for pruneRange.
 * <p>
 * pruneRange will return rgs that ranges intersected
 *
 * @author fangwu
 */

public class LongSortKeyIndex extends SortKeyIndex {
    /**
     * index data
     */
    private long[] data;

    private LongSortKeyIndex(long rgNum, int colId, DataType dt) {
        super(rgNum, colId, dt);
    }

    public static LongSortKeyIndex build(int colId, long[] data, DataType dt) {
        Preconditions.checkArgument(data != null && data.length > 0 && data.length % 2 == 0, "bad sort key index");
        LongSortKeyIndex longSortKeyIndex = new LongSortKeyIndex(data.length / 2, colId, dt);

        longSortKeyIndex.data = data;
        return longSortKeyIndex;
    }

    /**
     * return full rg for null arg.
     *
     * @param param target value
     * @return pruneRange result for the same value
     */
    @Override
    public void pruneEqual(Object param, RoaringBitmap cur) {
        if (param == null) {
            return;
        }
        pruneRange(param, param, cur);
    }

    /**
     * prune range contains two steps.
     * - value transformed to long
     * - prune by long range values
     */
    @Override
    public void pruneRange(Object startObj, Object endObj, RoaringBitmap cur) {
        Preconditions.checkArgument(!(startObj == null && endObj == null), "null val");
        Long start;
        Long end;

        //startObj/endObj == null means lowerBound/upperBound is unlimited
        // paramTransform() == null means type of startObj is unsupported
        start = (startObj == null) ? data[0] : paramTransform(startObj, dt, Long.class);
        end = (endObj == null) ? data[data.length - 1] : paramTransform(endObj, dt, Long.class);
        if (start == null || end == null) {
            return;
        }

        if (start > end) {
            cur.and(new RoaringBitmap());
            return;
        }

        if (end < data[0] ||
            start > data[data.length - 1]) {
            cur.and(new RoaringBitmap());
            return;
        }

        // get lower bound rg index
        Pair<Integer, Boolean> sIndex = binarySearchLowerBound(start);
        // get upper bound rg index
        Pair<Integer, Boolean> eIndex = binarySearchUpperBound(end);
        int startRgIndex;
        int endRgIndex;

        // if lower rg index was not included, plus it was different from upper index, then add 1 to lower rg index
        if (!sIndex.getValue() && !Objects.equals(sIndex.getKey(), eIndex.getKey())) {
            startRgIndex = sIndex.getKey() + 1;
        } else {
            startRgIndex = sIndex.getKey();
        }
        if (eIndex.getValue()) {
            endRgIndex = eIndex.getKey() + 1;
        } else {
            endRgIndex = eIndex.getKey();
        }

        cur.and(RoaringBitmap.bitmapOfRange(startRgIndex, endRgIndex));
    }

    /**
     * binary search target value from data array
     * - if data array wasn't contains target value, then check odd or even.
     * odd meaning target is inside of one row group. even meaning target isn't
     * belong any row group.
     * data [1, 10, 50, 100] and target 5 will return (0,true)
     * data [1, 10, 50, 100] and target 20 will return (0,false)
     * - if data array contains target value, try to find the upper bound value
     * for the same target value
     * data [1, 10, 50, 100, 100, 100] and target 100 will return (3,true)
     *
     * @param target target value to be searched
     */
    private Pair<Integer, Boolean> binarySearchUpperBound(Long target) {
        if (target < data[0]) {
            return Pair.of(0, false);
        }

        int index = Arrays.binarySearch(data, target);

        if (index < 0) {
            index = -(index + 1);
        } else {
            for (int i = index + 1; i < data.length; i++) {
                if (data[i] == target) {
                    index = i;
                } else {
                    break;
                }
            }
            return Pair.of(index / 2, true);
        }
        if (index % 2 == 0) {
            return Pair.of(index / 2, false);
        } else {
            return Pair.of(index / 2, true);
        }
    }

    /**
     * binary search target value from data array
     * - if data array wasn't contains target value, then check odd or even.
     * odd meaning target is inside of one row group. even meaning target isn't
     * belong any row group.
     * data [1, 10, 50, 100] and target 5 will return (0,false)
     * data [1, 10, 50, 100] and target 20 will return (0,true)
     * - if data array contains target value, try to find the lower bound value
     * for the same target value
     * data [1, 10, 50, 100, 100, 100] and target 100 will return (3,true)
     *
     * @param target target value to be searched
     */
    private Pair<Integer, Boolean> binarySearchLowerBound(Long target) {
        if (target > data[data.length - 1]) {
            return Pair.of((data.length - 1) / 2, false);
        } else if (target < data[0]) {
            return Pair.of(0, true);
        }
        int index = Arrays.binarySearch(data, target);

        if (index < 0) {
            index = -index - 1;
        } else {
            for (int i = index - 1; i > 0; i--) {
                if (data[i] == target) {
                    index = i;
                } else {
                    break;
                }
            }
            return Pair.of(index / 2, true);
        }
        if (index % 2 == 0) {
            return Pair.of((index / 2) - 1, false);
        } else {
            return Pair.of(index / 2, true);
        }
    }

    @Override
    public boolean checkSupport(int columnId, SqlTypeName type) {
        if (type != SqlTypeName.BIGINT &&
            type != SqlTypeName.INTEGER &&
            type != SqlTypeName.YEAR &&
            type != SqlTypeName.DATE &&
            type != SqlTypeName.DATETIME &&
            type != SqlTypeName.TIMESTAMP &&
            type != SqlTypeName.TINYINT &&
            type != SqlTypeName.MEDIUMINT) {
            return false;
        }

        // TODO col id might need transform
        return columnId == colId;
    }

    public long[] getData() {
        return data;
    }

    @Override
    public long getSizeInBytes() {
        if (data != null) {
            return (long) data.length * Long.BYTES;
        }
        return 0;
    }
}
