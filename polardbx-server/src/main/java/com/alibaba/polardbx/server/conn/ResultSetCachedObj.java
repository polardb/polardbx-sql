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

package com.alibaba.polardbx.server.conn;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.optimizer.utils.ITransaction;

import java.sql.ResultSet;

/**
 * @author yaozhili
 */
public class ResultSetCachedObj {
    private final ResultSet resultSet;
    private boolean firstRow;
    private boolean lastRow;
    private ITransaction trx;

    public void setTrx(ITransaction trx) {
        this.trx = trx;
    }

    public ITransaction getTrx() {
        return trx;
    }

    public ResultSetCachedObj(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public boolean isFirstRow() {
        return firstRow;
    }

    public boolean isLastRow() {
        return lastRow;
    }

    public void setFirstRow(boolean firstRow) {
        this.firstRow = firstRow;
    }

    public void setLastRow(boolean lastRow) {
        this.lastRow = lastRow;
    }

    public void close(Logger logger) {
        // Close the result set.
        if (null != resultSet) {
            try {
                resultSet.close();
            } catch (Throwable t) {
                logger.warn(t.getMessage());
            }
        }
        // Commit and close the trx.
        if (null != trx) {
            try {
                trx.commit();
            } catch (Throwable t) {
                logger.warn(t.getMessage());
            }
            try {
                trx.close();
            } catch (Throwable t) {
                logger.warn(t.getMessage());
            }
        }
    }
}
