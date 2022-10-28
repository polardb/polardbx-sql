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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.topology.AffinityInfoAccessor;
import com.alibaba.polardbx.gms.topology.AffinityInfoRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaAffinity;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * CN DN 亲和性表
 */
public class InformationSchemaAffinityHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaAffinityHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaAffinity;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            AffinityInfoAccessor affinityInfoAccessor = new AffinityInfoAccessor();
            affinityInfoAccessor.setConnection(metaDbConn);
            List<AffinityInfoRecord> affinityInfoRecordList = affinityInfoAccessor.getAffinityInfoList();
            if (CollectionUtils.isEmpty(affinityInfoRecordList)) {
                return cursor;
            }

            Map<String, StorageInstHaContext> storageInstHaCtxCache =
                StorageHaManager.getInstance().refreshAndGetStorageInstHaContextCache();
            Multimap<String, Pair<String, Integer>> storageIdAddress =
                getMasterIdAddress(affinityInfoRecordList, storageInstHaCtxCache);

            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            List<GroupDetailInfoExRecord> completedGroupInfos =
                groupDetailInfoAccessor.getCompletedGroupInfosByInstId(InstIdUtil.getInstId());
            Collections.sort(completedGroupInfos);
            for (GroupDetailInfoExRecord groupDetailInfoExRecord : completedGroupInfos) {
                String storageInstId = groupDetailInfoExRecord.storageInstId;
                String dbName = groupDetailInfoExRecord.dbName;
                String groupName = groupDetailInfoExRecord.groupName;
                String phyDbName = groupDetailInfoExRecord.phyDbName;
                String user = null;     // reserved for future use
                String passwd = null;   // reserved for future use
                String storageAddress = storageInstHaCtxCache.get(storageInstId).getCurrAvailableNodeAddr();
                if (storageIdAddress.containsKey(storageInstId)) {
                    for (Pair<String, Integer> pair : storageIdAddress.get(storageInstId)) {
                        cursor.addRow(
                            new Object[] {
                                dbName, phyDbName, groupName, user, passwd, pair.getKey(), pair.getValue(),
                                storageAddress});
                    }
                } else {
                    cursor.addRow(
                        new Object[] {dbName, phyDbName, groupName, user, passwd, null, null, storageAddress});
                }
            }
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }

        return cursor;
    }

    private Multimap<String, Pair<String, Integer>> getMasterIdAddress(
        List<AffinityInfoRecord> affinityInfoRecordList, Map<String, StorageInstHaContext> storageInstHaCtxCache) {

        Multimap<String, Pair<String, Integer>> storageIdAddress = HashMultimap.create();

        for (AffinityInfoRecord affinityInfo : affinityInfoRecordList) {
            StorageInstHaContext storageInstHaContext = storageInstHaCtxCache.get(affinityInfo.storageInstId);
            if (storageInstHaContext != null && storageInstHaContext.isDNMaster() && storageInstHaContext
                .getCurrAvailableNodeAddr().equalsIgnoreCase(affinityInfo.address)) {
                storageIdAddress.put(affinityInfo.storageInstId, new Pair<>(affinityInfo.ip, affinityInfo.port));
            }
        }

        return storageIdAddress;
    }
}
