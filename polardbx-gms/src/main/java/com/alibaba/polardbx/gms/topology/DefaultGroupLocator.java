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

import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author chenghui.lch
 */
public class DefaultGroupLocator implements GroupLocator {

    protected final int dbType;
    protected final Map<String, String> groupPhyDbMap;
    protected final List<String> storageInstList;
    protected final List<String> singleGroupStorageInstList;

    public DefaultGroupLocator(int dbType,
                               Map<String, String> groupPhyDbMap, List<String> storageInstList,
                               List<String> singleGroupStorageInstList) {
        this.dbType = dbType;
        this.groupPhyDbMap = groupPhyDbMap;
        this.storageInstList = storageInstList;
        this.singleGroupStorageInstList = singleGroupStorageInstList;
    }

    @Override
    public void buildGroupLocationInfo(Map<String, List<String>> normalGroupMap,
                                       Map<String, List<String>> singleGroupMap) {
        buildGroupLocationInfoInner(this.groupPhyDbMap, normalGroupMap, singleGroupMap);
    }

    protected void buildGroupLocationInfoInner(Map<String, String> groupPhyDbMap,
                                               Map<String, List<String>> outputNormalGroupMap,
                                               Map<String, List<String>> outputSingleGroupMap) {

        List<String> groupKeyList = Lists.newArrayList();
        groupKeyList.addAll(groupPhyDbMap.keySet());
        Collections.sort(groupKeyList);
        int instCount = storageInstList.size();
        int grpCnt = groupPhyDbMap.size();
        int curInstIdx = 0;

        int singleInstCnt = singleGroupStorageInstList.size();
        Random rand = new Random();
        for (int i = 0; i < grpCnt; i++) {
            String grpVal = groupKeyList.get(i);

            boolean isSingleGrp = GroupInfoUtil.isSingleGroup(grpVal);
            boolean isFirstPartGroup = i == 0 && this.dbType == DbInfoRecord.DB_TYPE_NEW_PART_DB;
            String storageInstId = null;

            if (isFirstPartGroup) {
                int singeInstIdx = Math.abs(Math.abs(rand.nextInt()) % singleInstCnt);
                storageInstId = singleGroupStorageInstList.get(singeInstIdx);

                // Adjust next normal group location to make sure uniform distribution
                int idx = storageInstList.indexOf(storageInstId);
                if (idx != -1) {
                    curInstIdx = (idx + 1) % instCount;
                }

                outputNormalGroupMap.computeIfAbsent(storageInstId, x -> Lists.newArrayList()).add(grpVal);
            } else if (!isSingleGrp || singleInstCnt == 0) {
                // Get Storage Inst id for non-single group
                storageInstId = storageInstList.get(curInstIdx);
                ++curInstIdx;
                if (curInstIdx >= instCount) {
                    curInstIdx = 0;
                }
                List<String> grpList = outputNormalGroupMap.computeIfAbsent(storageInstId, x -> Lists.newArrayList());
                grpList.add(grpVal);
            } else {
                int singeInstIdx = Math.abs(Math.abs(rand.nextInt()) % singleInstCnt);
                storageInstId = singleGroupStorageInstList.get(singeInstIdx);

                List<String> grpList = outputSingleGroupMap.computeIfAbsent(storageInstId, x -> Lists.newArrayList());
                grpList.add(grpVal);
            }
        }
    }
}
