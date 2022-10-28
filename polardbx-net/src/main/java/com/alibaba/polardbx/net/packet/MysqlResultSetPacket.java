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

/**
 * @author mengshi.sunmengshi 2013-11-20 下午6:13:48
 * @since 5.1.0
 */
public class MysqlResultSetPacket extends MySQLPacket {

    public ResultSetHeaderPacket resultHead;
    public FieldPacket[] fieldPackets;

    @Override
    protected String packetInfo() {
        return "ResultSet Packet";
    }

}
