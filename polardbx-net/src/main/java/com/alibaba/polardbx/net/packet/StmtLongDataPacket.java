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

import com.alibaba.polardbx.net.util.MySQLMessage;
import com.alibaba.polardbx.net.util.MysqlDefsUtil;

/**
 * Created by simiao.zw on 2014/7/30.
 */
public class StmtLongDataPacket extends CommandPacket {

    public int stmt_id;
    public short param_id;
    public Object data;
    public String charset;

    @Override
    protected String packetInfo() {
        return "COM_STMT_SEND_LONG_DATA packet";
    }

    public MySQLMessage readBeforeParamId(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        command = mm.read();
        stmt_id = mm.readInt();
        param_id = (short) mm.readUB2();

        return mm;
    }

    public void readAfterAfterParamId(MySQLMessage mm, short paramType) {
        // no nullmap here
        data = MysqlDefsUtil.readObject(mm, paramType, true, charset);
    }
}
