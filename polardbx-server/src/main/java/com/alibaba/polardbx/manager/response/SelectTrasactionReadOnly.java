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
 * select @@SESSION.TX_ISOLATION
 *
 * @author xiaoying 2010年10月28日 下午6:28:27
 * @since 5.4.2
 */
public final class SelectTrasactionReadOnly extends SingleColumnSimpleSelect {

    public SelectTrasactionReadOnly(String origColumnName, String aliasColumnName) {
        super(origColumnName, aliasColumnName);
    }

    public SelectTrasactionReadOnly(String origColumnName) {
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
