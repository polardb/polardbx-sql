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

package com.alibaba.polardbx.rpc;

import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * description:
 * author: ziyang.lb
 **/
public class CdcDirectByteOutput extends ByteOutput {
    private byte[] bytes;

    public CdcDirectByteOutput() {
        super();
    }

    public static byte[] unsafeFetch(ByteString byteString) {
        try {
            CdcDirectByteOutput output = new CdcDirectByteOutput();
            UnsafeByteOperations.unsafeWriteTo(byteString, output);
            return output.getBytes();
        } catch (IOException e) {
            throw new RuntimeException("fetch bytes from byte string error!!");
        }
    }

    private byte[] getBytes() {
        return bytes;
    }

    @Override
    public void write(byte value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] value, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLazy(byte[] value, int offset, int length) throws IOException {
        this.bytes = value;
    }

    @Override
    public void write(ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLazy(ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException();
    }
}
