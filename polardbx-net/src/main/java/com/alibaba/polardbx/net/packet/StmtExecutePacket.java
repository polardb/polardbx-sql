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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.polardbx.net.util.MySQLMessage;
import com.alibaba.polardbx.net.util.MysqlDefsUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * Created by simiao.zw on 2014/7/30.
 * http://dev.mysql.com/doc/internals/en/com-stmt-execute.html
 */
public class StmtExecutePacket extends CommandPacket {

    private static final Logger logger = LoggerFactory.getLogger(StmtExecutePacket.class);
    public int stmt_id;                                                     // (4)
    public byte flags;                                                       // (1)
    public int iteration_count;                                             // (4)

    public byte[] null_bitmap;                                                 // (num-params+7)/8
    // bytes
    public byte new_params_bound_flag;                                       // (1)
    public String charset = "UTF-8";

    public Map<Integer, Short> paramType = new HashMap<>();                   // num-params

    public List<Object> valuesArr;

    @Override
    protected String packetInfo() {
        return "COM_STMT_EXECUTE Packet";
    }

    /**
     * Since the later half need the num_params, so that we should read stmt_id
     * firstly then map and find params number
     */
    public MySQLMessage readBeforeStmtId(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        command = mm.read();
        stmt_id = mm.readInt();
        return mm;
    }

    /**
     * Read another half by num_params Read execute packet except for the
     * skipParams which comes from send_data_long packet with no indication
     * in execute packet new_params_bound_flag may not always be 1 1: first
     * packet can extract value types 0: continuous packet only have value
     */
    public void readAfterStmtId(MySQLMessage mm, int num_params, Map<Integer, Object> skipParams,
                                Map<Integer, Short> paramTypes) {
        flags = mm.read();
        iteration_count = mm.readInt();

        // 当 num_params 为0时， iteration_count之后null_bitmap不存在
        // modify by chenghui.lch
        if (num_params > 0) {
            int len = (num_params + 7) / 8;
            null_bitmap = new byte[len];
            for (int i = 0; i < len; i++) {
                null_bitmap[i] = mm.read();
            }
            valuesArr = new ArrayList<>(num_params);
        }

        // 按照mysql协议，http://dev.mysql.com/doc/internals/en/com-stmt-execute.html#packet-COM_STMT_EXECUTE,
        // 接下来还可能存在字段 new_params_bound_flag, 但 mysql官方文档对这个字段是否总是存在的描述是有歧义的,
        // 从而导致当num_parmas == 0时，有的client会发送这个字段，有的则不会发，所以这里需要加个字节判断
        if (!mm.hasRemaining()) {
            return;
        }

        new_params_bound_flag = mm.read();

        /**
         * If new_params_bound_flag == 1 extract paramTypes and store to
         * statement
         */
        if (new_params_bound_flag == 1) {
            for (int i = 0; i < num_params; i++) {
                paramType.put(i, (short) mm.readUB2());
                if (logger.isDebugEnabled()) {
                    logger.debug("read mm bound=1 " + i);
                }
            }
            paramTypes.putAll(paramType);
        } else if (new_params_bound_flag == 0) {
            paramType = paramTypes;
        }

        for (int i = 0; i < num_params; i++) {
            /**
             * Refer to mysql connector's ServerPreparedStatement.serverExecute
             */
            if ((null_bitmap[i / 8] & (1 << (i & 7))) == 0) {
                if (skipParams != null && skipParams.containsKey(i)) {
                    valuesArr.add(skipParams.get(i));
                    if (logger.isDebugEnabled()) {
                        StringBuffer info = new StringBuffer();
                        info.append("[readAfterStmtId skip index:").append(i).append("]");
                        logger.debug(info.toString());
                    }
                } else {
                    Object v = MysqlDefsUtil.readObject(mm, paramType.get(i), false, charset);
                    valuesArr.add(v);
                    if (logger.isDebugEnabled()) {
                        StringBuffer info = new StringBuffer();
                        info.append("[readAfterStmtId index:").append(i).append("]");
                        info.append("[type:").append(paramType.get(i)).append(", ");
                        info.append(" value:").append(v).append("]");
                        logger.debug(info.toString());
                    }
                }
            } else {
                valuesArr.add(null);
                if (logger.isDebugEnabled()) {
                    StringBuffer info = new StringBuffer();
                    info.append("[readAfterStmtId set index:").append(i).append(" to null]");
                    logger.debug(info.toString());
                }
            }
        }
    }

}
