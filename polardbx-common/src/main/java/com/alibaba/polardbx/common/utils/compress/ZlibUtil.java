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

package com.alibaba.polardbx.common.utils.compress;

import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;


public class ZlibUtil {

    public static byte[] compress(byte[] data) {
        return compress(data, 0, data.length);
    }

    public static byte[] compress(byte[] data, int off, int pieceLen) {
        byte[] output = null;
        Deflater compressor = new Deflater();
        compressor.reset();
        compressor.setInput(data, off, pieceLen);
        compressor.finish();

        ByteArrayOutputStream bos = new ByteArrayOutputStream(pieceLen);
        try {
            byte[] buf = new byte[1024];
            while (!compressor.finished()) {
                int len = compressor.deflate(buf);
                bos.write(buf, 0, len);
            }
            output = bos.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            try {
                bos.close();
            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }
        }

        compressor.end();
        return output;
    }

    public static byte[] decompress(byte[] data) {
        byte[] output;

        Inflater decompressor = new Inflater();
        decompressor.reset();
        decompressor.setInput(data);
        ByteArrayOutputStream o = new ByteArrayOutputStream(data.length);
        try {
            byte[] buf = new byte[1024];
            while (!decompressor.finished()) {
                int len = decompressor.inflate(buf);
                o.write(buf, 0, len);
            }
            output = o.toByteArray();
        } catch (DataFormatException e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            try {
                o.close();
            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }
        }

        decompressor.end();
        return output;
    }

    public static void compress(byte[] data, OutputStream os) {
        compress(data, 0, data.length, os);
    }

    public static void compress(byte[] data, int start, int length, OutputStream os) {
        DeflaterOutputStream dos = new DeflaterOutputStream(os);
        try {
            dos.write(data, start, length);
            dos.finish();
            dos.flush();
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static byte[] decompress(InputStream is) {
        InflaterInputStream iis = new InflaterInputStream(is);
        ByteArrayOutputStream o = new ByteArrayOutputStream(1024);

        try {
            int len = 1024;
            byte[] buf = new byte[len];
            while ((len = iis.read(buf, 0, len)) > 0) {
                o.write(buf, 0, len);
            }
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }

        return o.toByteArray();
    }
}
