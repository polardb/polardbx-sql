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

package com.alibaba.polardbx.executor.archive.pruning;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author chenzilin
 * @date 2021/12/3 16:12
 */
public class PruningResult {
    private static final Logger LOGGER = LoggerFactory.getLogger(PruningResult.class);

    public static PruningResult PASS = new PruningResult(CODE.PASS);
    public static PruningResult SKIP = new PruningResult(CODE.SKIP);

    private enum CODE {
        PASS, // all file need to scan
        PART, // part of file need to scan
        SKIP  // can skip all file
    }

    private Map<Long, StripeColumnMeta> stripeMap;

    private CODE code;

    private Set<Long> aggNotUsed;

    private PruningResult(CODE code) {
        this.code = code;
    }

    public PruningResult(List<StripeColumnMeta> stripeList) {
        Preconditions.checkArgument(!stripeList.isEmpty());
        this.code = CODE.PART;

        this.stripeMap = new HashMap<>();
        for (StripeColumnMeta stripeColumnMeta : stripeList) {
            stripeMap.put(stripeColumnMeta.getStripeIndex(),stripeColumnMeta);
        }
    }

    public PruningResult(Map<Long, StripeColumnMeta> stripeMap) {
        this.code = CODE.PART;
        this.stripeMap = new HashMap<>(stripeMap);
    }

    public Map<Long, StripeColumnMeta> getStripeMap() {
        return stripeMap;
    }

    public void initAgg() {
        aggNotUsed = new HashSet<>();
    }

    public RangeSet<Long> getRangeSet() {
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        for (StripeColumnMeta stripeColumnMeta : stripeMap.values()) {
            rangeSet.add(Range.openClosed(stripeColumnMeta.getStripeOffset(), stripeColumnMeta.getStripeOffset() + stripeColumnMeta.getStripeLength()));
        }
        return rangeSet;
    }

    public boolean pass() {
        return code == CODE.PASS;
    }

    public boolean part() {
        return code == CODE.PART;
    }

    public boolean skip() {
        return code == CODE.SKIP;
    }

    public void addNotAgg(Long index) {
        aggNotUsed.add(index);
    }

    public void addNotAgg(Set<Long> indexes) {
        aggNotUsed.addAll(indexes);
    }

    public void log() {
        if (code == CODE.PART) {
            LOGGER.info("agg pruned stripes: " + (stripeMap.size() - aggNotUsed.size())
                + " agg not pruned stripes: " + (aggNotUsed.size()));
        }
    }

    public boolean fullAgg() {
        return aggNotUsed != null && aggNotUsed.size() == 0;
    }

    /**
     * the statistics pruning is enabled
     * @return true if enabled
     */
    public boolean stat() {
        return aggNotUsed != null;
    }

    public boolean stat(Long index) {
        return aggNotUsed != null && !aggNotUsed.contains(index);
    }

    public PruningResult intersect(PruningResult other) {
        if (this.pass()) {
            return other;
        } else if (this.skip()) {
            return this;
        } else if (other.skip()) {
            return other;
        } else if (other.pass()) {
            return this;
        } else if (this.part()) {
            Map<Long, StripeColumnMeta> map = new HashMap<>();
            for (Map.Entry<Long, StripeColumnMeta> entry : getStripeMap().entrySet()) {
                if (other.getStripeMap().containsKey(entry.getKey())) {
                    map.put(entry.getKey(), entry.getValue());
                }
            }
            if (map.isEmpty()) {
                return SKIP;
            }
            return new PruningResult(map);
        } else {
            throw new AssertionError("impossible pruning result");
        }
    }


    public PruningResult union(PruningResult other) {
        if (this.pass()) {
            return this;
        } else if (this.skip()) {
            return other;
        } else if (other.pass()) {
            return this;
        } else if (other.skip()) {
            return this;
        } else if (this.part()) {
            // part
            Map<Long, StripeColumnMeta> map = new HashMap<>(getStripeMap());
            map.putAll(other.getStripeMap());
            return new PruningResult(map);
        } else {
            throw new AssertionError("impossible pruning result");
        }
    }

    @Override
    public String toString() {
        if (pass()) {
            return "PASS";
        } else if (skip()) {
            return "SKIP";
        } else {
            Set<Long> ids;
            if (stat()) {
                ids = new TreeSet<>(aggNotUsed);
            } else {
                ids = new TreeSet<>(stripeMap.keySet());
            }
            // choose the first 10
            StringBuilder sb = new StringBuilder("PART[");
            int cnt = 0;
            for (Long id : ids) {
                cnt++;
                if (cnt > 10) {
                    break;
                }
                if (cnt == 1) {
                    sb.append(id);
                } else {
                    sb.append(", ").append(id);
                }
            }
            if (cnt > 10) {
                sb.append("...");
            }
            sb.append("]");
            // part
            return sb.toString();
        }
    }
}