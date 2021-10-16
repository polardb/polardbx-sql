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

import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by simiao on 15-4-17.
 */
public class RawPacketStreamOutputProxy extends PacketStreamOutputProxy {

    public RawPacketStreamOutputProxy(OutputStream out) {
        super(out);
    }

    @Override
    public void checkWriteCapacity(int capacity) {
        /* 对于网络直接输出，不需要检查是否有空间，自动增长 */
    }

    @Override
    public FrontendConnection getConnection() {
        throw new NotSupportException("CompressPacketStreamOutputProxy not support getConnection");
    }

    @Override
    public void write(byte[] src) {
        try {
            out.write(src);
        } catch (IOException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_NET_SEND, e, e.getMessage());
        }
    }

    @Override
    public void write(byte[] src, int off, int len) {
        try {
            out.write(src, off, len);
        } catch (IOException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_NET_SEND, e, e.getMessage());
        }
    }

    @Override
    public void packetBegin() {

    }

    @Override
    public void packetEnd() {
        /**
         * 对于直接outputstream发送的情形不做特殊处理
         */
        try {
            out.flush();
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
