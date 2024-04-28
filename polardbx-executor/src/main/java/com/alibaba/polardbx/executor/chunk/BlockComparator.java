package com.alibaba.polardbx.executor.chunk;

import io.airlift.slice.Slice;

public interface BlockComparator<T extends Block, R extends Block> {
    int compareTo(T b1, int position1, R b2, int position2);

    static int compareTo(Slice slice1, int beginOffset1, int endOffset1, Slice slice2, int beginOffset2,
                         int endOffset2) {
        return slice1.compareTo(beginOffset1, endOffset1 - beginOffset1, slice2, beginOffset2,
            endOffset2 - beginOffset2);
    }

    BlockComparator SLICE_BLOCK_NO_DICT_SLICE_BLOCK_NO_DICT =
        (BlockComparator<SliceBlock, SliceBlock>) (b1, position1, b2, position2) ->
            compareTo(
                b1.getData(),
                b1.beginOffsetInner(position1),
                b1.endOffsetInner(position1),
                b2.getData(),
                b2.beginOffsetInner(position2),
                b2.endOffsetInner(position2)
            );

    BlockComparator SLICE_BLOCK_DICT_SLICE_BLOCK_NO_DICT =
        (BlockComparator<SliceBlock, SliceBlock>) (b1, position1, b2, position2) -> {
            Slice dictValue = b1.getDictValue(position1);
            return compareTo(
                dictValue,
                0, dictValue.length(),
                b2.getData(),
                b2.beginOffsetInner(position2),
                b2.endOffsetInner(position2)
            );
        };

    BlockComparator SLICE_BLOCK_NO_DICT_SLICE_BLOCK_DICT =
        (BlockComparator<SliceBlock, SliceBlock>) (b1, position1, b2, position2) -> {
            Slice dictValue = b2.getDictValue(position2);
            return compareTo(
                b1.getData(),
                b1.beginOffsetInner(position1),
                b1.endOffsetInner(position1),
                dictValue,
                0,
                dictValue.length()
            );
        };

    BlockComparator SLICE_BLOCK_DICT_SLICE_BLOCK_DICT =
        (BlockComparator<SliceBlock, SliceBlock>) (b1, position1, b2, position2) -> {
            Slice dictValue1 = b1.getDictValue(position1);
            Slice dictValue2 = b2.getDictValue(position2);
            return dictValue1.compareTo(dictValue2);
        };

    BlockComparator SLICE_BLOCK_NO_DICT_SLICE_BLOCK_BUILDER =
        (BlockComparator<SliceBlock, SliceBlockBuilder>) (b1, position1, b2, position2) ->
            compareTo(
                b1.getData(),
                b1.beginOffsetInner(position1),
                b1.endOffsetInner(position1),
                b2.getSliceOutput().getRawSlice(),
                b2.beginOffset(position2),
                b2.endOffset(position2)
            );

    BlockComparator SLICE_BLOCK_DICT_SLICE_BLOCK_BUILDER =
        (BlockComparator<SliceBlock, SliceBlockBuilder>) (b1, position1, b2, position2) -> {
            Slice dictValue = b1.getDictValue(position1);
            return compareTo(
                dictValue,
                0, dictValue.length(),
                b2.getSliceOutput().getRawSlice(),
                b2.beginOffset(position2),
                b2.endOffset(position2)
            );
        };
}
