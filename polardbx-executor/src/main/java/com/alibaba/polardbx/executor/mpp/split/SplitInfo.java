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

package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 *
 **/
public class SplitInfo {

    private final boolean expand;
    private final int sourceId;
    private final QueryConcurrencyPolicy concurrencyPolicy;
    private final Collection<List<Split>> splits;
    private final HashMap<String, String> groups;
    private final int insCount;
    private final int splitCount;
    private final boolean underSort;

    public SplitInfo(int sourceId, boolean expand, QueryConcurrencyPolicy concurrencyPolicy,
                     Collection<List<Split>> splits, HashMap<String, String> groups, int insCount, int splitCount,
                     boolean underSort) {
        this.sourceId = sourceId;
        this.expand = expand;
        this.concurrencyPolicy = concurrencyPolicy;
        this.splits = splits;
        this.groups = groups;
        this.insCount = insCount;
        this.splitCount = splitCount;
        this.underSort = underSort;
    }

    public QueryConcurrencyPolicy getConcurrencyPolicy() {
        return concurrencyPolicy;
    }

    public Collection<List<Split>> getSplits() {
        return splits;
    }

    public int getDbCount() {
        return groups.size();
    }

    public int getInsCount() {
        return insCount;
    }

    public int getSplitCount() {
        return splitCount;
    }

    public boolean isExpand() {
        return expand;
    }

    public Integer getSourceId() {
        return sourceId;
    }

    public boolean isUnderSort() {
        return underSort;
    }

    public HashMap<String, String> getGroups() {
        return groups;
    }
}
