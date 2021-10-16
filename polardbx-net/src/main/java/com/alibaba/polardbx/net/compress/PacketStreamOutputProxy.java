/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.net.compress;

import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream直接就是conn的网络部分 Created by simiao on 15-4-17.
 */
public abstract class PacketStreamOutputProxy extends PacketOutputProxyCommon {

    protected OutputStream out;

    protected ByteArrayOutputStream waitForCompressStream;

    public PacketStreamOutputProxy(OutputStream out) {
        this.out = out;
    }

    @Override
    public void write(byte b) {
        waitForCompressStream.write(b & 0xff);
    }

    @Override
    public void writeUB2(int i) {
        byte[] b = new byte[2];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        try {
            waitForCompressStream.write(b);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void writeUB3(int i) {
        byte[] b = new byte[3];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        b[2] = (byte) (i >>> 16);
        try {
            waitForCompressStream.write(b);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void writeInt(int i) {
        byte[] b = new byte[4];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        b[2] = (byte) (i >>> 16);
        b[3] = (byte) (i >>> 24);
        try {
            waitForCompressStream.write(b);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void writeFloat(float f) {
        writeInt(Float.floatToIntBits(f));
    }

    @Override
    public void writeUB4(long l) {
        byte[] b = new byte[4];
        b[0] = (byte) (l & 0xff);
        b[1] = (byte) (l >>> 8);
        b[2] = (byte) (l >>> 16);
        b[3] = (byte) (l >>> 24);
        try {
            waitForCompressStream.write(b);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void writeLong(long l) {
        byte[] b = new byte[8];
        b[0] = (byte) (l & 0xff);
        b[1] = (byte) (l >>> 8);
        b[2] = (byte) (l >>> 16);
        b[3] = (byte) (l >>> 24);
        b[4] = (byte) (l >>> 32);
        b[5] = (byte) (l >>> 40);
        b[6] = (byte) (l >>> 48);
        b[7] = (byte) (l >>> 56);
        try {
            waitForCompressStream.write(b);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void writeDouble(double d) {
        writeLong(Double.doubleToLongBits(d));
    }

    @Override
    public void writeLength(long length) {
        if (length < 251) {
            waitForCompressStream.write((byte) length);
        } else if (length < 0x10000L) {
            waitForCompressStream.write((byte) 252);
            writeUB2((int) length);
        } else if (length < 0x1000000L) {
            waitForCompressStream.write((byte) 253);
            writeUB3((int) length);
        } else {
            waitForCompressStream.write((byte) 254);
            writeLong(length);
        }
    }

    @Override
    public void writeWithNull(byte[] src) {
        write(src);
        waitForCompressStream.write((byte) 0);
    }

    @Override
    public void writeWithLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            waitForCompressStream.write((byte) length);
        } else if (length < 0x10000L) {
            waitForCompressStream.write((byte) 252);
            writeUB2(length);
        } else if (length < 0x1000000L) {
            waitForCompressStream.write((byte) 253);
            writeUB3(length);
        } else {
            waitForCompressStream.write((byte) 254);
            writeLong(length);
        }
        write(src);
    }

    @Override
    public void writeWithLength(byte[] src, byte nullValue) {
        if (src == null) {
            write(nullValue);
        } else {
            writeWithLength(src);
        }
    }

    @Override
    public boolean avaliable() {
        /**
         * 对于流式输出，总假设可用
         */
        return true;
    }
}
