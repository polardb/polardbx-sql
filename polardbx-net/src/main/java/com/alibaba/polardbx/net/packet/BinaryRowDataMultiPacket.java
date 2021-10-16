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

public class BinaryRowDataMultiPacket extends BinaryRowDataPacket {
    public BinaryRowDataMultiPacket(int fieldCount) {
        super(fieldCount);
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        int todo = getPacketLength();
        int packetCapacity = newPacket(proxy, todo, proxy.getConnection().getNewPacketId());

        final int rowHeaderLen = writeRowHeader(proxy);
        packetCapacity -= rowHeaderLen;
        todo -= rowHeaderLen;

        FieldValuePacket fv = null;
        for (int i = 0; i < fieldCount; ) {
            if (null == fv) {
                fv = new FieldValuePacket(fieldValues.get(i));
            }

            if (fv.isNull) {
                i++;
                fv = null;
                continue;
            }

            if (packetCapacity == 0) {
                proxy.packetEnd();

                packetCapacity = newPacket(proxy, todo, proxy.getConnection().getNewPacketId());
            }

            int sentLen = Math.min(fv.getRemaining(), packetCapacity);

            // Write field value
            proxy.checkWriteCapacity(sentLen);
            proxy.write(fv.value, fv.valueOff, sentLen);

            fv.valueOff += sentLen;

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
        public final byte[] value;
        public final int valueLen;
        public int valueOff = 0;

        public FieldValuePacket(byte[] fieldValue) {
            this.isNull = null == fieldValue;
            this.value = fieldValue;
            this.valueLen = null == fieldValue ? 0 : fieldValue.length;
        }

        public int getRemaining() {
            return valueLen - valueOff;
        }
    }

    private int writeRowHeader(IPacketOutputProxy proxy) {
        // packet header of row in a binary resultset
        proxy.checkWriteCapacity(1);
        proxy.write((byte) 0);

        // the first two bit is reserved for execute binary result
        byte[] null_bitmap = new byte[(fieldCount + 7 + 2) / 8];
        int bit = 4;
        int nullMaskPos = 0;
        for (int i = 0; i < fieldCount; i++) {
            byte[] fv = fieldValues.get(i);

            if (fv == null) {
                // fill null map
                null_bitmap[nullMaskPos] |= bit;
            }

            // refer to mysql connector MysqlIO.unpackBinaryResultSetRow
            if (((bit <<= 1) & 255) == 0) {
                bit = 1;
                nullMaskPos++;
            }
        }

        proxy.checkWriteCapacity(null_bitmap.length);
        proxy.write(null_bitmap);

        return 1 + null_bitmap.length;
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
