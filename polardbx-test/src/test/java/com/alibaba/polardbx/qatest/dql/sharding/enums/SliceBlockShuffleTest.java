package com.alibaba.polardbx.qatest.dql.sharding.enums;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SliceBlockShuffleTest {
    SliceType sliceType;

    SliceBlock block;

    Slice data;

    int[] offsets;

    @Before
    public void prepare() throws IOException {
        SliceOutput output = new DynamicSliceOutput(0);
        output.write("aB".getBytes(StandardCharsets.UTF_8));
        output.write("AB".getBytes(StandardCharsets.UTF_8));
        data = output.slice();
        offsets = new int[] {2, 4};
    }

    @Test
    public void testLatin1Ci() {
        test("latin1", "LATIN1_GENERAL_CI", true, true);
        test("latin1", "LATIN1_GENERAL_CI", false, false);
    }

    @Test
    public void testLatin1Cs() {
        test("latin1", "LATIN1_GENERAL_CS", true, false);
        test("latin1", "LATIN1_GENERAL_CS", false, false);
    }

    @Test
    public void testLatin1Default() {
        test("latin1", null, true, true);
        test("latin1", null, false, false);
    }

    @Test
    public void testUtf8Default() {
        test("utf8", null, true, true);
        test("utf8", null, false, false);
    }

    @Test
    public void testUtf8Ci() {
        test("utf8mb4", "UTF8MB4_GENERAL_CI", true, true);
        test("utf8mb4", "UTF8MB4_GENERAL_CI", false, false);
    }

    @Test
    public void testUtf8mb4Bin() {
        test("utf8mb4", "UTF8MB4_BIN", true, false);
        test("utf8mb4", "UTF8MB4_BIN", false, false);
    }

    @Test
    public void testBinary() {
        test("binary", null, true, false);
        test("binary", null, false, false);
    }

    private void test(String charset, String collation, boolean compatible, boolean equal) {
        CharsetName charsetName = CharsetName.of(charset);
        CollationName collationName =
            collation == null ? charsetName.getDefaultCollationName() : CollationName.of(collation);
        sliceType = new CharType(charsetName, collationName);
        block = new SliceBlock(sliceType, 0, 2, null, offsets, data, compatible);
        if (equal) {
            Assert.assertTrue(block.hashCodeUseXxhash(0) == block.hashCodeUseXxhash(1), "hash code should equal");
        } else {
            Assert.assertTrue(block.hashCodeUseXxhash(0) != block.hashCodeUseXxhash(1), "hash code should not equal");
        }
    }
}
