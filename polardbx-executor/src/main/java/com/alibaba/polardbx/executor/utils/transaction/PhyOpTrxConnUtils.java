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

package com.alibaba.polardbx.executor.utils.transaction;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author chenghui.lch
 */
public class PhyOpTrxConnUtils {

    public static Connection getConnection(
        ITransaction trans,
        String schemaName,
        String groupName,
        TGroupDataSource grpDs,
        ITransaction.RW rw,
        ExecutionContext ec,
        Long grpConnId
    ) throws SQLException
    {
        ITransactionPolicy.TransactionClass tranClass = trans.getTransactionClass();

        if (tranClass == ITransactionPolicy.TransactionClass.XA || tranClass == ITransactionPolicy.TransactionClass.TSO) {
            return trans.getConnection(schemaName,
                groupName,
                grpConnId,
                grpDs,
                rw,
                ec);
        }
        return trans.getConnection(schemaName,
            groupName,
            grpDs,
            rw,
            ec);
    }
}
