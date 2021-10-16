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

import com.alibaba.polardbx.common.utils.compress.ZlibUtil;

/**
 * Created by simiao on 15-4-20.
 */
public abstract class PacketOutputProxyCommon implements IPacketOutputProxy {

    public interface CompressSpliter {

        void sendCompressPiece(byte[] content, byte sequenceId, int beforeLen);
    }

    public static boolean isValid(IPacketOutputProxy proxy) {
        return proxy != null && proxy.avaliable();
    }

    private byte sequenceId = 1; /*
     * sequenceId从1开始,
     * 因为支持多次分片压缩发送，所以需要记录sequenceId
     */

    protected void sequenceReset() {
        sequenceId = 1;
    }

    protected byte sequenceGetAndInc() {
        return sequenceId++;
    }

    protected void splitCompressAndSent(byte[] origContent, CompressSpliter spliter) {
        int remaing = origContent.length;
        int offset = 0;

        while (remaing > MAX_ORIG_CONTENT_LENGTH) {
            spliter.sendCompressPiece(ZlibUtil.compress(origContent, offset, MAX_ORIG_CONTENT_LENGTH),
                sequenceId++,
                MAX_ORIG_CONTENT_LENGTH);

            remaing -= MAX_ORIG_CONTENT_LENGTH;
            offset += MAX_ORIG_CONTENT_LENGTH;
        }

        if (remaing > 0) {
            spliter.sendCompressPiece(ZlibUtil.compress(origContent, offset, remaing), sequenceGetAndInc(), remaing);
        }
    }

    @Override
    public int getLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

    @Override
    public int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

    @Override
    public void writeArrayAsPacket(byte[] src) {
        packetBegin();
        write(src);
        packetEnd();
    }
}
