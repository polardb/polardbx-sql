package com.alibaba.polardbx.common.jdbc;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.stream.IntStream;

/**
 * @author fangwu
 */
public class PruneRawStringTest {

    @Test
    public void testGetObj() {
        PruneRawString pruneRawString = new PruneRawString(ImmutableList.copyOf(IntStream.range(0, 1000).iterator()),
            PruneRawString.PRUNE_MODE.RANGE, 0, 1000, null);
        assert pruneRawString.display().contains("NonPruneRaw");
        try {
            pruneRawString.getObj(1000, -1);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assert e.getMessage().contains("PruneRawString error array index out of bounds");
        }

        pruneRawString = new PruneRawString(ImmutableList.copyOf(IntStream.range(0, 1000).iterator()),
            PruneRawString.PRUNE_MODE.RANGE, 0, 999, null);
        assert !pruneRawString.display().contains("NonPruneRaw");

        pruneRawString = new PruneRawString(ImmutableList.copyOf(IntStream.range(0, 4096).iterator()),
            PruneRawString.PRUNE_MODE.RANGE, 0, 4096, null);
        assert pruneRawString.display().contains("NonPruneRaw");
        assert pruneRawString.display().endsWith("...");
    }

    @Test
    public void testMerge() {
        PruneRawString pruneRawString = new PruneRawString(ImmutableList.copyOf(IntStream.range(0, 1000).iterator()),
            PruneRawString.PRUNE_MODE.RANGE, 0, 300, null);
        PruneRawString pruneRawString2 = new PruneRawString(ImmutableList.copyOf(IntStream.range(0, 1000).iterator()),
            PruneRawString.PRUNE_MODE.RANGE, 100, 500, null);
        pruneRawString.merge(pruneRawString2);

        assert pruneRawString.pruneMode == PruneRawString.PRUNE_MODE.MULTI_INDEX;
        assert pruneRawString2.pruneMode == PruneRawString.PRUNE_MODE.MULTI_INDEX;
        assert pruneRawString.size() == 500;
        PruneRawString pruneRawString3 = new PruneRawString(ImmutableList.copyOf(IntStream.range(0, 1000).iterator()),
            PruneRawString.PRUNE_MODE.RANGE, 500, 1000, null);
        pruneRawString.merge(pruneRawString3);
        assert pruneRawString.pruneMode == PruneRawString.PRUNE_MODE.RANGE;
        assert pruneRawString3.pruneMode == PruneRawString.PRUNE_MODE.MULTI_INDEX;

        PruneRawString pruneRawString4 = new PruneRawString(ImmutableList.copyOf(IntStream.range(0, 1000).iterator()),
            PruneRawString.PRUNE_MODE.RANGE, 0, 1000, null);
        pruneRawString4.merge(pruneRawString);

        assert pruneRawString4.pruneMode == PruneRawString.PRUNE_MODE.RANGE;
        assert pruneRawString.pruneMode == PruneRawString.PRUNE_MODE.RANGE;
    }
}
