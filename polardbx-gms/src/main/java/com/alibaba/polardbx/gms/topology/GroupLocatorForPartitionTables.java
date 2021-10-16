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

package com.alibaba.polardbx.gms.topology;

import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class GroupLocatorForPartitionTables extends DefaultGroupLocator {

    final Map<String, String> groupPhyDbMapForPartitionTables;
    final int dbType;

    public GroupLocatorForPartitionTables(Map<String, String> groupPhyDbMap,
                                          List<String> storageInstList,
                                          List<String> singleGroupStorageInstList,
                                          Map<String, String> groupPhyDbMapForPartitionTables,
                                          int dbType) {
        super(groupPhyDbMap, storageInstList, singleGroupStorageInstList);
        this.groupPhyDbMapForPartitionTables = groupPhyDbMapForPartitionTables;
        this.dbType = dbType;
    }

    @Override
    public void buildGroupLocationInfo(Map<String, List<String>> normalGroups,
                                       Map<String, List<String>> singleGroups) {
        super.buildGroupLocationInfo(normalGroups, singleGroups);
        if (dbType == DbInfoRecord.DB_TYPE_NEW_PART_DB) {
            buildGroupLocationInfoInner(groupPhyDbMapForPartitionTables, normalGroups, singleGroups);
        }
    }
}
