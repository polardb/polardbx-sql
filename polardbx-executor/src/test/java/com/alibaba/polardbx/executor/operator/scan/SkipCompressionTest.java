package com.alibaba.polardbx.executor.operator.scan;

import org.apache.orc.CompressionCodec;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.RunLengthIntegerReaderV2;
import org.apache.orc.impl.RunLengthIntegerWriterV2;
import org.apache.orc.impl.ZlibCodec;
import org.apache.orc.impl.writer.StreamOptions;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * In order to optimize performance, we have made some changes to the skip
 * operation in RunLengthReader, so a guarantee test is needed.
 */
public class SkipCompressionTest {
    @Test
    public void testUncompressedSeek() throws Exception {
        runSeekTest(null);
    }

    @Test
    public void testCompressedSeek() throws Exception {
        runSeekTest(new ZlibCodec());
    }

    @Test
    public void testSkips() throws Exception {
        InStreamTest.OutputCollector collect = new InStreamTest.OutputCollector();
        RunLengthIntegerWriterV2 out = new RunLengthIntegerWriterV2(
            new OutStream("test", new StreamOptions(100), collect), true);
        for (int i = 0; i < 2048; ++i) {
            if (i < 1024) {
                out.write(i);
            } else {
                out.write(256 * i);
            }
        }
        out.flush();
        ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
        collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
        inBuf.flip();
        RunLengthIntegerReaderV2 in = new RunLengthIntegerReaderV2(InStream.create("test",
            new BufferChunk(inBuf, 0), 0,
            inBuf.remaining()), true, false);
        for (int i = 0; i < 2048; i += 10) {
            int x = (int) in.next();
            if (i < 1024) {
                assertEquals(i, x);
            } else {
                assertEquals(256 * i, x);
            }
            if (i < 2038) {
                in.skip(9);
            }
            in.skip(0);
        }
    }

    private void runSeekTest(CompressionCodec codec) throws Exception {
        InStreamTest.OutputCollector collect = new InStreamTest.OutputCollector();
        StreamOptions options = new StreamOptions(1000);
        if (codec != null) {
            options.withCodec(codec, codec.getDefaultOptions());
        }
        RunLengthIntegerWriterV2 out = new RunLengthIntegerWriterV2(
            new OutStream("test", options, collect), true);
        InStreamTest.PositionCollector[] positions =
            new InStreamTest.PositionCollector[4096];
        Random random = new Random(99);
        int[] junk = new int[2048];
        for (int i = 0; i < junk.length; ++i) {
            junk[i] = random.nextInt();
        }
        for (int i = 0; i < 4096; ++i) {
            positions[i] = new InStreamTest.PositionCollector();
            out.getPosition(positions[i]);
            // test runs, incrementing runs, non-runs
            if (i < 1024) {
                out.write(i / 4);
            } else if (i < 2048) {
                out.write(2 * i);
            } else {
                out.write(junk[i - 2048]);
            }
        }
        out.flush();
        ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
        collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
        inBuf.flip();
        RunLengthIntegerReaderV2 in =
            new RunLengthIntegerReaderV2(InStream.create("test",
                new BufferChunk(inBuf, 0), 0, inBuf.remaining(),
                InStream.options().withCodec(codec).withBufferSize(1000)), true, false);
        for (int i = 0; i < 2048; ++i) {
            int x = (int) in.next();
            if (i < 1024) {
                assertEquals(i / 4, x);
            } else {
                assertEquals(2 * i, x);
            }
        }
        for (int i = 2047; i >= 0; --i) {
            in.seek(positions[i]);
            int x = (int) in.next();
            if (i < 1024) {
                assertEquals(i / 4, x);
            } else {
                assertEquals(2 * i, x);
            }
        }
    }

}
