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

package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;

/**
 * select @@SESSION.TX_READ_ONLY
 *
 * @author agapple 2016年3月17日 下午9:29:47
 * @author arnkore 2016-12-26
 * @since 5.1.25-SNAPSHOT
 */
public final class SelectTxReadOnly extends SingleColumnSimpleSelect {

    public SelectTxReadOnly(String origColumnName, String aliasColumnName) {
        super(origColumnName, aliasColumnName);
    }

    public SelectTxReadOnly(String origColumnName) {
        super(origColumnName);
    }

    @Override
    protected void initFields(byte packetId) {
        fields[0] = PacketUtil.getField(aliasColumnName, Fields.FIELD_TYPE_LONGLONG);
        fields[0].packetId = packetId;
    }

    @Override
    protected void initRowData(String resultCharset) {
        byte[] rawRowData = LongUtil.toBytes(0);
        row.add(rawRowData);
    }
}
