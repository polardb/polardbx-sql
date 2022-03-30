package com.alibaba.polardbx.common.utils.hash;

import io.airlift.slice.XxHash64;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * 测试XxHash64实现的正确性
 */
public class XxHash64Test {

    private Random random = new Random(System.currentTimeMillis());

    @Test
    public void testXxHashBlock() throws IOException {
        final int times = 30;
        IBlockHasher myXxHash64 = new XxHash_64Hasher(0);
        for (int i = 0; i < times; i++) {
            long l = random.nextLong();

            long expectHash = XxHash64.hash(l);
            long actualHash = myXxHash64.hashLong(l).asLong();

            Assert
                .assertEquals(String.format("Long hash failed at round: %d, value: %d", i, l), expectHash, actualHash);
        }

        for (int i = 0; i < times; i++) {
            int len = i * 8 + random.nextInt(8);
            byte[] input1 = new byte[len];
            random.nextBytes(input1);

            long expectHash = XxHash64.hash(new ByteArrayInputStream(input1));
            long actualHash = myXxHash64.hashBytes(input1).asLong();

            Assert.assertEquals(String.format("Bytes hash failed at round: %d, length: %d", i, len), expectHash,
                actualHash);
        }
    }

    @Test
    public void testXxHashStream() {
        final int times = 30;
        IStreamingHasher myXxHash64 = new XxHash_64Hasher(0);

        for (int i = 0; i < times; i++) {
            int len = i * 8 + random.nextInt(8);
            byte[] input1 = new byte[len];
            byte[] input2 = new byte[len];
            random.nextBytes(input1);
            random.nextBytes(input2);
            long l = random.nextLong();
            // don't use guava which is based on big-endian
            byte[] longBytes = ByteBuffer.allocate(8).order(ByteUtil.PLATFORM_ENDIAN).putLong(l).array();
            XxHash64 xxHash64 = new XxHash64();
            long expectHash = xxHash64.update(input1).update(longBytes).update(input2).update(longBytes).hash();
            long actualHash = myXxHash64.putBytes(input1).putLong(l).putBytes(input2).putLong(l).hash().asLong();

            Assert.assertEquals(String.format("Failed at round %d, length %d", i, len), expectHash, actualHash);
        }
    }
}
