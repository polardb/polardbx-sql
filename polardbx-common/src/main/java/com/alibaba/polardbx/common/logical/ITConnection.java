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

package com.alibaba.polardbx.common.logical;

import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.lock.LockingFunctionHandle;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface ITConnection extends AutoCloseable {

    ITStatement createStatement() throws SQLException;

    ITPrepareStatement prepareStatement(String sql) throws SQLException;

    boolean isClosed();

    @Override
    void close() throws SQLException;

    long getLastInsertId();

    void setLastInsertId(long id);

    long getReturnedLastInsertId();

    void setReturnedLastInsertId(long id);

    String getUser();

    ITransactionPolicy getTrxPolicy();

    void setTrxPolicy(ITransactionPolicy trxPolicy);

    BatchInsertPolicy getBatchInsertPolicy(Map<String, Object> extraCmds);

    void setBatchInsertPolicy(BatchInsertPolicy policy);

    long getFoundRows();

    void setFoundRows(long foundRows);

    long getAffectedRows();

    void setAffectedRows(long affectedRows);

    List<Long> getGeneratedKeys();

    void setGeneratedKeys(List<Long> ids);

    default LockingFunctionHandle getLockHandle(Object ec) {
        return null;
    }

    /**
     * 返回前端connection对应的ID，同show processlist
     */
    default long getId() {
        return 0L;
    }

    boolean isMppConnection();

}
