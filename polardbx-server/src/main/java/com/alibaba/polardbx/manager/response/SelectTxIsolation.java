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
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.common.constants.TransactionAttribute;

/**
 * select @@SESSION.TX_ISOLATION
 *
 * @author mengshi.sunmengshi 2014年6月25日 下午4:30:27
 * @author arnkore 2016-12-26
 * @since 5.1.0
 */
public final class SelectTxIsolation extends SingleColumnSimpleSelect {

    public SelectTxIsolation(String origColumnName, String aliasColumnName) {
        super(origColumnName, aliasColumnName);
    }

    public SelectTxIsolation(String origColumnName) {
        super(origColumnName);
    }

    @Override
    protected void initFields(byte packetId) {
        fields[0] = PacketUtil.getField(aliasColumnName, Fields.FIELD_TYPE_VAR_STRING);
        fields[0].packetId = packetId;
    }

    @Override
    protected void initRowData(String resultCharset) {
        byte[] data = TransactionAttribute.DEFAULT_ISOLATION_LEVEL.nameWithHyphen().getBytes();
        row.add(data);
    }
}
