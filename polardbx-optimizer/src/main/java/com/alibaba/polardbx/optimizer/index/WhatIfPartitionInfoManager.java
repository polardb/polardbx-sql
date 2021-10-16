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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionTableType;
import org.apache.commons.lang.StringUtils;

/**
 * @author dylan
 */
public class WhatIfPartitionInfoManager extends PartitionInfoManager {

    PartitionInfoManager actualPartitionInfoManager;

    public WhatIfPartitionInfoManager(PartitionInfoManager partitionInfoManager) {
        super(partitionInfoManager.getSchemaName(), partitionInfoManager.getAppName());
        actualPartitionInfoManager = partitionInfoManager;
    }

    @Override
    public PartitionInfo getPartitionInfo(String tbName) {
        if (!StringUtils.isEmpty(tbName)) {
            tbName = tbName.toLowerCase();
        }
        PartInfoCtx ctx = partInfoCtxCache.get(tbName);
        if (ctx == null) {
            return actualPartitionInfoManager.getPartitionInfo(tbName);
        }
        return ctx.getPartInfo();
    }

    @Override
    public boolean isNewPartDbTable(String tbName) {
        return partInfoCtxCache.containsKey(tbName) || actualPartitionInfoManager.isNewPartDbTable(tbName);
    }

    @Override
    public boolean isPartitionedTable(String tbName) {
        PartitionInfo partitionInfo = getPartitionInfo(tbName);
        if (partitionInfo == null) {
            return false;
        }
        return partitionInfo.getTableType() == PartitionTableType.PARTITION_TABLE
            || partitionInfo.getTableType() == PartitionTableType.GSI_TABLE;
    }

    @Override
    public boolean isBroadcastTable(String tbName) {
        PartitionInfo partitionInfo = getPartitionInfo(tbName);
        if (partitionInfo == null) {
            return false;
        }
        return partitionInfo.getTableType() == PartitionTableType.BROADCAST_TABLE;
    }

    @Override
    public boolean isSingleTable(String tbName) {
        PartitionInfo partitionInfo = getPartitionInfo(tbName);
        if (partitionInfo == null) {
            return false;
        }
        return partitionInfo.getTableType() == PartitionTableType.SINGLE_TABLE;
    }
}
