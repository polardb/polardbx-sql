/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator.ratelimiter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.impl.FSDataInputStreamRateLimiter;
import org.apache.orc.impl.FSDataOutputStreamRateLimiter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class FSDataStreamRateLimiterTest {
    String FILE_NAME = "/tmp/rateLimiterTest.txt";
    FileSystem FILE_SYSTEM;

    @Before
    public void before() throws IOException {
        FILE_SYSTEM = FileSystem.getLocal(new Configuration());
        Path path = new Path(FILE_NAME);
        if (FILE_SYSTEM.exists(path)) {
            FILE_SYSTEM.delete(path, false);
        }
    }

    @After
    public void after() throws IOException {
        Path path = new Path(FILE_NAME);
        if (FILE_SYSTEM.exists(path)) {
            FILE_SYSTEM.delete(path, false);
        }
    }

    @Test
    public void rateLimiterTest() throws IOException {
        //写
        final Long[] writeLen = {0L};
        FSDataOutputStreamRateLimiter outputStream =
            new FSDataOutputStreamRateLimiter(FILE_SYSTEM.create(new Path(FILE_NAME)),
                (len) -> writeLen[0] += len);
        byte[] a = "abc".getBytes();
        byte[] b = "123".getBytes();
        byte[] b2 = "hello".getBytes();
        byte[] b3 = "world".getBytes();
        int c = 123;
        boolean d = true;
        byte e = (byte) 135;
        String f = "fff";
        char g = 'c';
        String h = "hhh";
        int i = 20;
        int j = 22;
        long k = 30000L;
        float l = 3.14f;
        double m = 3.1415d;
        outputStream.write(a);
        outputStream.write(b, 0, b.length);
        outputStream.write(b2);
        outputStream.write(b3);
        outputStream.write(c);
        outputStream.writeBoolean(d);
        outputStream.writeByte(e);
        outputStream.writeBytes(f);
        outputStream.writeChar(g);
        outputStream.writeChars(h);
        outputStream.writeShort(i);
        outputStream.writeInt(j);
        outputStream.writeLong(k);
        outputStream.writeFloat(l);
        outputStream.writeDouble(m);

        outputStream.flush();
        outputStream.close();

        long fileLen = FILE_SYSTEM.getFileStatus(new Path(FILE_NAME)).getLen();

        System.out.println(FILE_NAME + " fileLen: " + fileLen + " writeLen: " + writeLen[0]);
        Assert.assertEquals(fileLen, (long) writeLen[0]);

        //读
        final Long[] readLen = {0L};
        FSDataInputStreamRateLimiter inputStream =
            new FSDataInputStreamRateLimiter(FILE_SYSTEM.open(new Path(FILE_NAME)),
                (len) -> readLen[0] += len);
        byte[] readA = new byte[a.length];
        byte[] readB = new byte[b.length];
        byte[] readB2 = new byte[b2.length];
        byte[] readB3 = new byte[b3.length];
        int readC;
        boolean readD;
        byte readE;
        String readF;
        char readG;
        String readH;
        int readI;
        int readJ;
        long readK;
        float readL;
        double readM;

        inputStream.read(readA);
        Assert.assertArrayEquals(a, readA);
        inputStream.read(readB, 0, readB.length);
        Assert.assertArrayEquals(b, readB);
        inputStream.readFully(inputStream.getPos(), readB2);
        inputStream.seek(inputStream.getPos() + readB2.length);
        Assert.assertArrayEquals(b2, readB2);
        inputStream.readFully(inputStream.getPos(), readB3, 0, readB3.length);
        inputStream.seek(inputStream.getPos() + readB3.length);
        Assert.assertArrayEquals(b3, readB3);
        readC = inputStream.read();
        Assert.assertEquals(c, readC);
        readD = inputStream.readBoolean();
        Assert.assertEquals(d, readD);
        readE = inputStream.readByte();
        Assert.assertEquals(e, readE);
        byte[] ff = new byte[f.length()];
        inputStream.readFully(ff);
        readF = new String(ff);
        Assert.assertEquals(f, readF);
        readG = inputStream.readChar();
        Assert.assertEquals(g, readG);
        StringBuilder stringBuilder = new StringBuilder();
        for (int len = 0; len < h.length(); len++) {
            stringBuilder.append(inputStream.readChar());
        }
        readH = stringBuilder.toString();
        Assert.assertEquals(h, readH);
        readI = inputStream.readShort();
        Assert.assertEquals(i, readI);
        readJ = inputStream.readInt();
        Assert.assertEquals(j, readJ);
        readK = inputStream.readLong();
        Assert.assertEquals(k, readK);
        readL = inputStream.readFloat();
        Assert.assertEquals(l, readL, 0);
        readM = inputStream.readDouble();
        Assert.assertEquals(m, readM, 0);

        inputStream.close();

        System.out.println(FILE_NAME + " fileLen: " + fileLen + " readLen: " + readLen[0]);
        Assert.assertEquals(fileLen, (long) readLen[0]);
    }
}
