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

import com.alibaba.polardbx.optimizer.config.table.IndexMeta;

import java.util.List;

/**
 * @author dylan
 */
public class Index {

    private final IndexMeta indexMeta;

    private final List<PredicateType> prefixTypeList;

    // with Join Condition and TableScan Condition
    private double totalSelectivity;
    // with just Join Condition
    private double joinSelectivity;

    public Index(IndexMeta indexMeta, List<PredicateType> prefixTypeList, double totalSelectivity,
                 double joinSelectivity) {
        this.indexMeta = indexMeta;
        this.prefixTypeList = prefixTypeList;
        this.totalSelectivity = totalSelectivity;
        this.joinSelectivity = joinSelectivity;
    }

    public IndexMeta getIndexMeta() {
        return indexMeta;
    }

    public int getPrefixLen() {
        return prefixTypeList.size();
    }

    public List<PredicateType> getPrefixTypeList() {
        return prefixTypeList;
    }

    public double getTotalSelectivity() {
        return totalSelectivity;
    }

    public double getJoinSelectivity() {
        return joinSelectivity;
    }

    public enum PredicateType {
        EQUAL, IN, RANGE, AGG, SORT
    }
}
