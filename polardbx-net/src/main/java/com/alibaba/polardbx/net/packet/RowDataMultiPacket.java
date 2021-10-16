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

package com.alibaba.polardbx.net.packet;

import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.util.BufferUtil;

import java.util.function.Supplier;

public class RowDataMultiPacket extends RowDataPacket {

    private final Supplier<Byte> packetIdGen;

    public RowDataMultiPacket(int fieldCount, Supplier<Byte> packetIdGen) {
        super(fieldCount);
        this.packetIdGen = packetIdGen;
    }

    @Override
    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        int todo = getPacketLength();
        int packetCapacity = newPacket(proxy, todo, packetIdGen.get());

        FieldValuePacket fv = null;
        for (int i = 0; i < fieldCount; ) {
            if (null == fv) {
                fv = new FieldValuePacket(fieldValues.get(i));
            }
            if (packetCapacity == 0) {
                proxy.packetEnd();

                packetCapacity = newPacket(proxy, todo, packetIdGen.get());
            }

            int sentLen = Math.min(fv.getRemaining(), packetCapacity);

            // Write field header
            final int headerRemaining = fv.getHeaderRemaining();
            if (headerRemaining > sentLen) {
                proxy.checkWriteCapacity(sentLen);
                proxy.write(fv.header, fv.headerOff, sentLen);

                fv.headerOff += sentLen;

                packetCapacity -= sentLen;
                todo -= sentLen;

                continue;
            } else if (headerRemaining > 0) {
                proxy.checkWriteCapacity(headerRemaining);
                proxy.write(fv.header, fv.headerOff, headerRemaining);

                fv.headerOff += headerRemaining;

                packetCapacity -= headerRemaining;
                todo -= headerRemaining;

                sentLen -= headerRemaining;
            }

            // Write field value
            if (sentLen > 0) {
                proxy.checkWriteCapacity(sentLen);
                proxy.write(fv.value, fv.valueOff, sentLen);

                fv.valueOff += sentLen;
            }

            if (fv.getRemaining() <= 0) {
                i++;
                fv = null;
            }

            packetCapacity -= sentLen;
            todo -= sentLen;
        }

        proxy.packetEnd();

        return proxy;
    }

    private static class FieldValuePacket {
        public final boolean isNull;
        public final byte[] header;
        public final byte[] value;
        public final int headerLen;
        public final int valueLen;

        public int headerOff = 0;
        public int valueOff = 0;

        public FieldValuePacket(byte[] fieldValue) {
            this.isNull = null == fieldValue;
            this.value = null == fieldValue ? new byte[] {RowDataPacket.NULL_MARK} : fieldValue;
            this.valueLen = null == fieldValue ? 1 : fieldValue.length;
            this.header = null == fieldValue ? null : buildHeader(fieldValue);
            this.headerLen = null == fieldValue ? 0 : BufferUtil.getLength(fieldValue.length);
        }

        public int getRemaining() {
            return getHeaderRemaining() + getValueRemaining();
        }

        public int getHeaderRemaining() {
            return headerLen - headerOff;
        }

        public int getValueRemaining() {
            return valueLen - valueOff;
        }

        private static byte[] buildHeader(byte[] src) {
            byte[] result = null;

            final long length = src.length;
            if (length < 251) {
                result = new byte[1];
                result[0] = (byte) length;
            } else if (length < 0x10000L) {
                result = new byte[3];
                result[0] = (byte) 252;
                result[1] = (byte) (length & 0xff);
                result[2] = (byte) (length >>> 8);
            } else if (length < 0x1000000L) {
                result = new byte[4];
                result[0] = (byte) 253;
                result[1] = (byte) (length & 0xff);
                result[2] = (byte) (length >>> 8);
                result[3] = (byte) (length >>> 16);
            } else {
                result = new byte[9];
                result[0] = (byte) 254;
                result[1] = (byte) (length & 0xff);
                result[2] = (byte) (length >>> 8);
                result[3] = (byte) (length >>> 16);
                result[4] = (byte) (length >>> 24);
                result[5] = (byte) (length >>> 32);
                result[6] = (byte) (length >>> 40);
                result[7] = (byte) (length >>> 48);
                result[8] = (byte) (length >>> 56);
            }

            return result;
        }
    }

    private int newPacket(IPacketOutputProxy proxy, int todo, byte packetId) {
        final int packetLen = Math.min(todo, MAX_PACKET_PAYLOAD_LENGTH);

        proxy.packetBegin();

        proxy.checkWriteCapacity(proxy.getConnection().getPacketHeaderSize());
        proxy.writeUB3(packetLen);
        proxy.write(packetId);

        return packetLen;
    }
}
