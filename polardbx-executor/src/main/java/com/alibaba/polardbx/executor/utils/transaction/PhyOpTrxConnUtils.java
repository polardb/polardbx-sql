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

import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;

import java.sql.Connection;
import java.sql.SQLException;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.SUPPORT_SHARE_READVIEW_TRANSACTION;

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
    ) throws SQLException {
        if (trans.getTransactionClass().isA(SUPPORT_SHARE_READVIEW_TRANSACTION)) {
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
