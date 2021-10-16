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
 * Created by simiao.zw on 2014/7/30. These are data object, can access the
 * public field after parsing
 * http://dev.mysql.com/doc/internals/en/com-stmt-prepare.html
 */
public class StmtPreparePacket extends CommandPacket {

    @Override
    protected String packetInfo() {
        return "MySQL COM_STMT_PREPARE Packet";
    }

}
