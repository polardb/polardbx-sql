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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaPolardbxTrx;
import com.alibaba.polardbx.optimizer.view.InformationSchemaPreparedTrxBranch;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author yaozhili
 */
public class InformationSchemaPreparedTrxBranchHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaPreparedTrxBranchHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaPreparedTrxBranch;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        for (String dnId : ExecUtils.getAllDnStorageId()) {
            // Skip metaDB
            if (StringUtils.containsIgnoreCase(dnId, "pxc-xdb-m-")) {
                continue;
            }
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
                Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("XA RECOVER");
                while (rs.next()) {
                    final String formatId = rs.getString("formatID");
                    final String gtridLength = rs.getString("gtrid_length");
                    final String bqualLength = rs.getString("bqual_length");
                    final String data = rs.getString("data");
                    cursor.addRow(new Object[] {dnId, formatId, gtridLength, bqualLength, data});
                }
            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to fetch xa recover information on dn " + dnId, e);
            }
        }

        return cursor;
    }
}
