package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.io.WritableComparator;
import org.roaringbitmap.RoaringBitmap;

import java.util.Objects;

public class StringSortKeyIndex extends SortKeyIndex {
    /**
     * index data
     */
    private byte[][] data;
    private long sizeInBytes;

    private StringSortKeyIndex(long rgNum, int colId, DataType dt) {
        super(rgNum, colId, dt);
    }

    //build StringSortKeyIndex base on data
    public static StringSortKeyIndex build(int colId, String[] data, DataType dt) {
        Preconditions.checkArgument(data != null && data.length > 0 && data.length % 2 == 0, "bad sort key index");
        StringSortKeyIndex stringSortKeyIndex = new StringSortKeyIndex(data.length / 2, colId, dt);
        long size = 0;
        stringSortKeyIndex.data = new byte[data.length][];
        for (int i = 0; i < data.length; i++) {
            stringSortKeyIndex.data[i] = data[i].getBytes();
            size += stringSortKeyIndex.data[i].length;
        }
        stringSortKeyIndex.sizeInBytes = size;
        return stringSortKeyIndex;
    }

    @Override
    public void pruneEqual(Object param, RoaringBitmap cur) {
        if (param == null) {
            return;
        }
        pruneRange(param, param, cur);
    }

    @Override
    public void pruneRange(Object startObj, Object endObj, RoaringBitmap cur) {
        Preconditions.checkArgument(!(startObj == null && endObj == null), "null val");
        byte[] start;
        byte[] end;

        //startObj/endObj == null means lowerBound/UpperBound is unlimited
        // paramTransform() == null means type of startObj is unsupported
        if (startObj == null) {
            start = data[0];
        } else {
            String stringStart = paramTransform(startObj, dt, String.class);
            if (stringStart == null) {
                return;
            }
            start = stringStart.getBytes();
        }

        if (endObj == null) {
            end = data[data.length - 1];
        } else {
            String stringEnd = paramTransform(endObj, dt, String.class);
            if (stringEnd == null) {
                return;
            }
            end = stringEnd.getBytes();
        }
        if (bytesCompare(start, end) > 0) {
            cur.and(new RoaringBitmap());
            return;
        }
        if (bytesCompare(end, data[0]) < 0 ||
            bytesCompare(start, data[data.length - 1]) > 0) {
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
    private Pair<Integer, Boolean> binarySearchUpperBound(byte[] target) {
        if (bytesCompare(target, data[0]) < 0) {
            return Pair.of(0, false);
        }

        int index = binarySearch(data, target);

        if (index < 0) {
            index = -(index + 1);
        } else {
            for (int i = index + 1; i < data.length; i++) {
                if (bytesCompare(data[i], target) == 0) {
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
    private Pair<Integer, Boolean> binarySearchLowerBound(byte[] target) {
        if (bytesCompare(target, data[data.length - 1]) > 0) {
            return Pair.of((data.length - 1) / 2, false);
        } else if (bytesCompare(target, data[0]) < 0) {
            return Pair.of(0, true);
        }
        int index = binarySearch(data, target);

        if (index < 0) {
            index = -index - 1;
        } else {
            for (int i = index - 1; i > 0; i--) {
                if (bytesCompare(data[i], target) == 0) {
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
        if (type != SqlTypeName.VARCHAR &&
            type != SqlTypeName.CHAR) {
            return false;
        }

        // TODO col id might need transform
        return columnId == colId;
    }

    public byte[][] getData() {
        return data;
    }

    private int binarySearch(byte[][] a, byte[] key) {
        int low = 0;
        int high = a.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            byte[] midVal = a[mid];
            int cmp = bytesCompare(midVal, key);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1);  // key not found.
    }

    private static int bytesCompare(byte[] a, byte[] b) {
        return WritableComparator.compareBytes(a, 0, a.length, b, 0, b.length);
    }

    @Override
    public long getSizeInBytes() {
        return sizeInBytes;
    }
}
