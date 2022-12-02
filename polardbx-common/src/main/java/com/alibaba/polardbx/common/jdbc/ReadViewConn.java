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

package com.alibaba.polardbx.common.jdbc;

import java.sql.SQLException;

public abstract class ReadViewConn implements IConnection {
    /**
     * Xid 缓存
     * 避免重复构造 {@link com.alibaba.polardbx.transaction.utils.XAUtils.XATransInfo}
     */
    private String xid = null;

    private boolean inShareReadView = false;

    @Override
    public String getTrxXid() {
        return xid;
    }

    @Override
    public void setTrxXid(String xid) {
        this.xid = xid;
    }

    @Override
    public void setInShareReadView(boolean inShareReadView) {
        this.inShareReadView = inShareReadView;
    }

    @Override
    public boolean inShareReadView() {
        return this.inShareReadView;
    }

    @Override
    public void close() throws SQLException {
        this.xid = null;
        this.inShareReadView = false;
    }
}
