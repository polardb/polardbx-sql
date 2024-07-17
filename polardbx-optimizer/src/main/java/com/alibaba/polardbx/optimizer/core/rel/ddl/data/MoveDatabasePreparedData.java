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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveDatabasePreparedData extends DdlPreparedData {

    public MoveDatabasePreparedData(String schemaName, Map<String, List<String>> storageGroups, String sourceSql) {
        super(schemaName, "");
        this.storageGroups = storageGroups;
        this.sourceSql = sourceSql;
        sourceTargetGroupMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        groupAndStorageInstId = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : storageGroups.entrySet()) {
            for (String sourceGroup : entry.getValue()) {
                String targetGroup = GroupInfoUtil.buildScaleOutGroupName(sourceGroup);
                sourceTargetGroupMap.put(sourceGroup, targetGroup);
                final String sourceInstId = DbTopologyManager
                    .getStorageInstIdByGroupName(InstIdUtil.getInstId(), schemaName,
                        sourceGroup);
                if (entry.getKey().equalsIgnoreCase(sourceInstId)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                        "the source storageInst is equal to target storageInst:" + sourceInstId + " for:"
                            + sourceGroup);
                }
                groupAndStorageInstId.put(sourceGroup, Pair.of(sourceInstId, entry.getKey()));
            }
        }
    }

    private String sourceSql;
    /**
     * storageGroups
     * key:targetStorageInst
     * value:sourceGroupNames
     */
    private final Map<String, List<String>> storageGroups;
    private final Map<String, String> sourceTargetGroupMap;
    /**
     * key:sourceGroupName
     * value: key:old storageId, valule: new storageId;
     */
    private final Map<String, Pair<String, String>> groupAndStorageInstId;

    protected boolean usePhysicalBackfill = false;

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public Map<String, List<String>> getStorageGroups() {
        return storageGroups;
    }

    public Map<String, String> getSourceTargetGroupMap() {
        return sourceTargetGroupMap;
    }

    public Map<String, Pair<String, String>> getGroupAndStorageInstId() {
        return groupAndStorageInstId;
    }

    public boolean isUsePhysicalBackfill() {
        return usePhysicalBackfill;
    }

    public void setUsePhysicalBackfill(boolean usePhysicalBackfill) {
        this.usePhysicalBackfill = usePhysicalBackfill;
    }
}
