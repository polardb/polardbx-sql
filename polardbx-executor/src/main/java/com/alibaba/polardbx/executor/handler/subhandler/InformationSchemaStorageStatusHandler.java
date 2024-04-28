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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.view.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static com.alibaba.polardbx.optimizer.view.InformationSchemaStorageStatus.STORAGE_STATUS_ITEM;

public class InformationSchemaStorageStatusHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaStorageStatusHandler.class);

    private static final String sql = generateQuerySQL();

    private static String generateQuerySQL() {
        String[] items = Arrays.copyOfRange(STORAGE_STATUS_ITEM, 2,
            STORAGE_STATUS_ITEM.length); //STORAGE_INST_ID and INST_KIND are not needed here
        String[] quotedItems = Arrays.stream(items).map(s -> "'" + s + "'").toArray(String[]::new);
        String sql =
            ("SHOW GLOBAL STATUS WHERE Variable_name " + "IN (" + Arrays.toString(quotedItems) + ")").replaceAll(
                "\\[|\\]", "");
        return sql;
    }

    public InformationSchemaStorageStatusHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return (virtualView instanceof InformationSchemaStorageStatus);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        HashMap<String, String> storageStatus = new HashMap();

        final int instIdIndex = InformationSchemaStorageStatus.getTableStorageInstIdIndex();
        final int instRoleIndex = InformationSchemaStorageStatus.getTableInstRoleIndex();
        final int instKindIndex = InformationSchemaStorageStatus.getTableInstKindIndex();

        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        // StorageInstIdIndex
        Set<String> indexStorageInstId = virtualView.getEqualsFilterValues(instIdIndex, params);
        // StorageInstIdLike
        String storageInstIdLike = virtualView.getLikeString(instIdIndex, params);
        // InstRoleIndex
        Set<String> indexInstRole = virtualView.getEqualsFilterValues(instRoleIndex, params);
        // InstRoleLike
        String instRoleLike = virtualView.getLikeString(instRoleIndex, params);
        // InstKindIndex
        Set<String> indexInstKind = virtualView.getEqualsFilterValues(instKindIndex, params);
        // InstKindLike
        String instKindLike = virtualView.getLikeString(instKindIndex, params);

        Map<String, StorageInstHaContext> storageStatusMap = StorageHaManager.getInstance().getStorageHaCtxCache();

        TreeSet<StorageInstHaContext> dnInfos =
            new TreeSet<>(new InformationSchemaStorageHandler.StorageInstCtxSorter());
        dnInfos.addAll(storageStatusMap.values());

        for (StorageInstHaContext ctx : dnInfos) {

            String instanceId = ctx.getStorageInstId();
            String masterInstanceId = ctx.getStorageMasterInstId();
            String instanceRole = (instanceId.equals(masterInstanceId) ? "leader" : "learner");
            String instanceKind = StorageInfoRecord.getInstKind(ctx.getStorageKind());

            if ((!indexStorageInstId.isEmpty() && !indexStorageInstId.contains(instanceId.toLowerCase())) ||
                (!indexInstRole.isEmpty() && !indexInstRole.contains(instanceRole.toLowerCase())) ||
                (!indexInstKind.isEmpty() && !indexInstKind.contains(instanceKind.toLowerCase()))) {
                continue;
            }
            if ((storageInstIdLike != null && !new Like().like(instanceId, storageInstIdLike)) ||
                (instRoleLike != null && !new Like().like(instanceRole, instRoleLike)) ||
                (instKindLike != null && !new Like().like(instanceKind, instKindLike))) {
                continue;
            }

            try (Connection metaDbConn = DbTopologyManager.getConnectionForStorage(ctx)) {
                PreparedStatement statement = metaDbConn.prepareStatement(sql);
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    storageStatus.put(rs.getString("Variable_name").toUpperCase(), rs.getString("Value").toUpperCase());
                }
            } catch (SQLException ex) {
                logger.error("get information schema routines failed!", ex);
            }

            ArrayList<Object> row = new ArrayList<>(Arrays.asList(instanceId, instanceRole, instanceKind));
            for (String s : STORAGE_STATUS_ITEM) {
                if (storageStatus.get(s) != null) {
                    row.add(storageStatus.get(s));
                }
            }
            cursor.addRow(row.toArray());
        }

        return cursor;
    }

}
