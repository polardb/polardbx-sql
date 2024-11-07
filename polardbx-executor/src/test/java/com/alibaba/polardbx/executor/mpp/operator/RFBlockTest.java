package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.utils.XxhashUtils;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.ByteBlock;
import com.alibaba.polardbx.executor.chunk.ByteBlockBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlockBuilder;
import com.alibaba.polardbx.executor.chunk.ShortBlock;
import com.alibaba.polardbx.executor.chunk.ShortBlockBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class RFBlockTest {
    static final int CHUNK_LIMIT = 1000;

    @Test
    public void testByteBroadcast() {
        RFBloomFilter bloomFilter = RFBloomFilter.createBlockLongBloomFilter(CHUNK_LIMIT * 4);

        ByteBlockBuilder byteBlockBuilder = new ByteBlockBuilder(1024);
        for (int i = 0; i < CHUNK_LIMIT; i++) {
            byteBlockBuilder.writeByte((byte) ((i % 128) * 2));
        }
        ByteBlock block = (ByteBlock) byteBlockBuilder.build();
        block.addLongToBloomFilter(bloomFilter);

        byteBlockBuilder = new ByteBlockBuilder(1024);
        for (int i = 0; i < CHUNK_LIMIT; i++) {
            byteBlockBuilder.writeByte((byte) (i % 128));
        }
        block = (ByteBlock) byteBlockBuilder.build();

        boolean[] bitmap = new boolean[CHUNK_LIMIT];
        Arrays.fill(bitmap, true);
        block.mightContainsLong(bloomFilter, bitmap, true);

        for (int i = 0; i < bitmap.length; i++) {
            if (i % 2 == 0) {
                Assert.assertTrue(bitmap[i]);
            }
        }
    }

    @Test
    public void testByteLocal() {
        final int partitionCount = 4;

        RFBloomFilter[] bloomFilters = new RFBloomFilter[partitionCount];
        for (int part = 0; part < partitionCount; part++) {
            bloomFilters[part] = RFBloomFilter.createBlockLongBloomFilter(CHUNK_LIMIT * 4);
        }

        ByteBlockBuilder[] byteBlockBuilders = new ByteBlockBuilder[partitionCount];
        for (int part = 0; part < partitionCount; part++) {
            byteBlockBuilders[part] = new ByteBlockBuilder(CHUNK_LIMIT);
        }

        // write values into different partition.
        for (int i = 0; i < 3 * CHUNK_LIMIT; i++) {
            int val = (i % 128) * 2;
            int targetPart = getPartition(val, partitionCount);
            byteBlockBuilders[targetPart].writeByte((byte) val);
        }
        ByteBlock[] byteBlocks = new ByteBlock[partitionCount];
        for (int part = 0; part < partitionCount; part++) {
            byteBlocks[part] = (ByteBlock) byteBlockBuilders[part].build();

            // add to rf.
            byteBlocks[part].addLongToBloomFilter(bloomFilters[part]);
        }

        // build blocks for checking.
        for (int part = 0; part < partitionCount; part++) {
            byteBlockBuilders[part] = new ByteBlockBuilder(CHUNK_LIMIT);
        }
        for (int i = 0; i < 3 * CHUNK_LIMIT; i++) {
            int val = i % 128;
            int targetPart = getPartition(val, partitionCount);
            byteBlockBuilders[targetPart].writeByte((byte) val);
        }
        for (int part = 0; part < partitionCount; part++) {
            byteBlocks[part] = (ByteBlock) byteBlockBuilders[part].build();
        }

        // assert
        for (int part = 0; part < partitionCount; part++) {
            Block block = byteBlocks[part];

            boolean[] bitmap = new boolean[CHUNK_LIMIT];
            Arrays.fill(bitmap, true);
            block.mightContainsLong(bloomFilters[part], bitmap, true);

            System.out.println(Arrays.toString(bitmap));

            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.getLong(i) % 2 == 0) {
                    Assert.assertTrue(bitmap[i]);
                }
            }
        }
    }

    @Test
    public void testShortBroadcast() {
        RFBloomFilter bloomFilter = RFBloomFilter.createBlockLongBloomFilter(CHUNK_LIMIT * 4);

        ShortBlockBuilder shortBlockBuilder = new ShortBlockBuilder(1024);
        for (int i = 0; i < CHUNK_LIMIT; i++) {
            shortBlockBuilder.writeShort((short) ((i % 65536) * 2));
        }
        ShortBlock block = (ShortBlock) shortBlockBuilder.build();
        block.addLongToBloomFilter(bloomFilter);

        shortBlockBuilder = new ShortBlockBuilder(1024);
        for (int i = 0; i < CHUNK_LIMIT; i++) {
            shortBlockBuilder.writeShort((short) (i % 65536));
        }
        block = (ShortBlock) shortBlockBuilder.build();

        boolean[] bitmap = new boolean[CHUNK_LIMIT];
        Arrays.fill(bitmap, true);
        block.mightContainsLong(bloomFilter, bitmap, true);

        for (int i = 0; i < bitmap.length; i++) {
            if (i % 2 == 0) {
                Assert.assertTrue(bitmap[i]);
            }
        }
    }

    @Test
    public void testShortLocal() {
        final int partitionCount = 4;

        RFBloomFilter[] bloomFilters = new RFBloomFilter[partitionCount];
        for (int part = 0; part < partitionCount; part++) {
            bloomFilters[part] = RFBloomFilter.createBlockLongBloomFilter(CHUNK_LIMIT * 4);
        }

        ShortBlockBuilder[] shortBlockBuilders = new ShortBlockBuilder[partitionCount];
        for (int part = 0; part < partitionCount; part++) {
            shortBlockBuilders[part] = new ShortBlockBuilder(CHUNK_LIMIT);
        }

        // write values into different partition.
        for (int i = 0; i < 3 * CHUNK_LIMIT; i++) {
            int val = (i % 65536) * 2;
            int targetPart = getPartition(val, partitionCount);
            shortBlockBuilders[targetPart].writeShort((short) val);
        }
        ShortBlock[] shortBlocks = new ShortBlock[partitionCount];
        for (int part = 0; part < partitionCount; part++) {
            shortBlocks[part] = (ShortBlock) shortBlockBuilders[part].build();

            // add to rf.
            shortBlocks[part].addLongToBloomFilter(bloomFilters[part]);
        }

        // build blocks for checking.
        for (int part = 0; part < partitionCount; part++) {
            shortBlockBuilders[part] = new ShortBlockBuilder(CHUNK_LIMIT);
        }
        for (int i = 0; i < 3 * CHUNK_LIMIT; i++) {
            int val = i % 65536;
            int targetPart = getPartition(val, partitionCount);
            shortBlockBuilders[targetPart].writeShort((short) val);
        }
        for (int part = 0; part < partitionCount; part++) {
            shortBlocks[part] = (ShortBlock) shortBlockBuilders[part].build();
        }

        // assert
        for (int part = 0; part < partitionCount; part++) {
            Block block = shortBlocks[part];

            boolean[] bitmap = new boolean[CHUNK_LIMIT];
            Arrays.fill(bitmap, true);
            block.mightContainsLong(bloomFilters[part], bitmap, true);

            System.out.println(Arrays.toString(bitmap));

            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.getLong(i) % 2 == 0) {
                    Assert.assertTrue(bitmap[i]);
                }
            }
        }
    }

    @Test
    public void testIntBroadcast() {
        RFBloomFilter bloomFilter = RFBloomFilter.createBlockLongBloomFilter(CHUNK_LIMIT * 4);

        IntegerBlockBuilder intBlockBuilder = new IntegerBlockBuilder(1024);
        for (int i = 0; i < CHUNK_LIMIT; i++) {
            intBlockBuilder.writeInt(i * 2);
        }
        IntegerBlock block = (IntegerBlock) intBlockBuilder.build();
        block.addLongToBloomFilter(bloomFilter);

        intBlockBuilder = new IntegerBlockBuilder(1024);
        for (int i = 0; i < CHUNK_LIMIT; i++) {
            intBlockBuilder.writeInt(i);
        }
        block = (IntegerBlock) intBlockBuilder.build();

        boolean[] bitmap = new boolean[CHUNK_LIMIT];
        Arrays.fill(bitmap, true);
        block.mightContainsLong(bloomFilter, bitmap, true);

        for (int i = 0; i < bitmap.length; i++) {
            if (i % 2 == 0) {
                Assert.assertTrue(bitmap[i]);
            }
        }
    }

    @Test
    public void testIntLocal() {
        final int partitionCount = 4;

        RFBloomFilter[] bloomFilters = new RFBloomFilter[partitionCount];
        for (int part = 0; part < partitionCount; part++) {
            bloomFilters[part] = RFBloomFilter.createBlockLongBloomFilter(CHUNK_LIMIT * 4);
        }

        IntegerBlockBuilder[] intBlockBuilders = new IntegerBlockBuilder[partitionCount];
        for (int part = 0; part < partitionCount; part++) {
            intBlockBuilders[part] = new IntegerBlockBuilder(CHUNK_LIMIT);
        }

        // write values into different partition.
        for (int i = 0; i < 3 * CHUNK_LIMIT; i++) {
            int val = i * 2;
            int targetPart = getPartition(val, partitionCount);
            intBlockBuilders[targetPart].writeInt(val);
        }
        IntegerBlock[] intBlocks = new IntegerBlock[partitionCount];
        for (int part = 0; part < partitionCount; part++) {
            intBlocks[part] = (IntegerBlock) intBlockBuilders[part].build();

            // add to rf.
            intBlocks[part].addLongToBloomFilter(bloomFilters[part]);
        }

        // build blocks for checking.
        for (int part = 0; part < partitionCount; part++) {
            intBlockBuilders[part] = new IntegerBlockBuilder(CHUNK_LIMIT);
        }
        for (int i = 0; i < 3 * CHUNK_LIMIT; i++) {
            int val = i % 65536;
            int targetPart = getPartition(val, partitionCount);
            intBlockBuilders[targetPart].writeInt(val);
        }
        for (int part = 0; part < partitionCount; part++) {
            intBlocks[part] = (IntegerBlock) intBlockBuilders[part].build();
        }

        // assert
        for (int part = 0; part < partitionCount; part++) {
            Block block = intBlocks[part];

            boolean[] bitmap = new boolean[CHUNK_LIMIT];
            Arrays.fill(bitmap, true);
            block.mightContainsLong(bloomFilters[part], bitmap, true);

            System.out.println(Arrays.toString(bitmap));

            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.getLong(i) % 2 == 0) {
                    Assert.assertTrue(bitmap[i]);
                }
            }
        }
    }

    private static int getPartition(long longVal, int partitionCount) {

        // Convert the searchVal from field space to hash space
        long hashVal = XxhashUtils.finalShuffle(longVal);
        int partition = (int) ((hashVal & Long.MAX_VALUE) % partitionCount);

        return partition;
    }
}
