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

package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.logical.ITPrepareStatement;
import com.alibaba.polardbx.common.logical.ITStatement;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class MppMockConnection implements ITConnection {
    /**
     * 保存 select sql_calc_found_rows 返回的结果
     */
    private long foundRows = 1;

    private String user;
    private long lastInsertId;
    private long returnLastInsertId;

    public MppMockConnection(String user) {
        this.user = user;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public long getLastInsertId() {
        return lastInsertId;
    }

    @Override
    public void setLastInsertId(long id) {
        this.lastInsertId = id;
    }

    @Override
    public long getReturnedLastInsertId() {
        return returnLastInsertId;
    }

    @Override
    public void setReturnedLastInsertId(long id) {
        returnLastInsertId = id;
    }

    @Override
    public List<Long> getGeneratedKeys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setGeneratedKeys(List<Long> ids) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITransactionPolicy getTrxPolicy() {
        return ITransactionPolicy.NO_TRANSACTION;
    }

    @Override
    public void setTrxPolicy(ITransactionPolicy trxPolicy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BatchInsertPolicy getBatchInsertPolicy(
        Map<String, Object> extraCmds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFoundRows() {
        return foundRows;
    }

    @Override
    public void setFoundRows(long foundRows) {
        this.foundRows = foundRows;
    }

    @Override
    public long getAffectedRows() {
        return 0;
    }

    @Override
    public void setAffectedRows(long affectedRows) {
        // do nothing
    }

    @Override
    public void close() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBatchInsertPolicy(BatchInsertPolicy policy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITStatement createStatement() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITPrepareStatement prepareStatement(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMppConnection() {
        return true;
    }
}
