package com.alibaba.polardbx.optimizer.config.table.collation;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

// From MySQL test unittest/gunit/strings_strnxfrm-t.cc:2383
// Golden hashes for a test string. These may be stored on disk, so we need to
// make sure that they never change.
public class Utf8mb4Zh0900AsCsHashTest {
    @Test
    public void test() {
        long[] expectedHashResult = new long[] {0x23c370d9ac589d1fL, 0x00000001L};

        System.out.println(Arrays.toString(expectedHashResult));

        String test_str =
            "This is a fairly long string. It does not contain any special " +
                "characters since they are probably not universally supported across all " +
                "character sets, but should at least be enough to make the nr1 value go " +
                "up past the 32-bit mark.";

        CollationHandler collationHandler =
            CharsetFactory.INSTANCE.createCollationHandler(CollationName.UTF8MB4_ZH_0900_AS_CS);

        long nr1 = 4;
        long nr2 = 1;
        long[] hashResult = new long[] {nr1, nr2};

        byte[] testBytes = test_str.getBytes();
        collationHandler.hashcode(testBytes, testBytes.length, hashResult);

        System.out.println(Arrays.toString(hashResult));

        Assert.assertArrayEquals(expectedHashResult, hashResult);
    }
}
