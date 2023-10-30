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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrcFilePruningResult implements PruningResult {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrcFilePruningResult.class);

    public static PruningResult PASS = new OrcFilePruningResult(CODE.PASS);
    public static PruningResult SKIP = new OrcFilePruningResult(CODE.SKIP);

    enum CODE {
        PASS, // all file need to scan
        PART, // part of file need to scan

        SKIP,  // can skip all file
    }

    protected Map<Long, StripeColumnMeta> stripeMap;

    private CODE code;

    private OrcFilePruningResult(CODE code) {
        this.code = code;
        this.stripeMap = Maps.newHashMap();
    }

    public OrcFilePruningResult(List<StripeColumnMeta> stripeList) {
        Preconditions.checkArgument(!stripeList.isEmpty());
        this.code = CODE.PART;

        this.stripeMap = Maps.newHashMap();
        for (StripeColumnMeta stripeColumnMeta : stripeList) {
            stripeMap.put(stripeColumnMeta.getStripeIndex(), stripeColumnMeta);
        }
    }

    public OrcFilePruningResult(Map<Long, StripeColumnMeta> stripeMap) {
        this.code = CODE.PART;
        this.stripeMap = Maps.newHashMap(stripeMap);
    }

    public RangeSet<Long> getRangeSet() {
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        for (StripeColumnMeta stripeColumnMeta : stripeMap.values()) {
            rangeSet.add(Range.openClosed(stripeColumnMeta.getStripeOffset(),
                stripeColumnMeta.getStripeOffset() + stripeColumnMeta.getStripeLength()));
        }
        return rangeSet;
    }

    public AggPruningResult toStatistics() {
        if (pass()) {
            return AggPruningResult.PASS;
        } else if (skip()) {
            return AggPruningResult.SKIP;
        } else if (part()) {
            return new AggPruningResult(stripeMap);
        }
        throw new AssertionError("unknown type" + code.name());
    }

    public Map<Long, StripeColumnMeta> getStripeMap() {
        return stripeMap;
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

    public void log() {
        if (code == CODE.PART) {
            LOGGER.info("left stripes after pruning: " + (stripeMap.size()));
        }
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
            return new OrcFilePruningResult(map);
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
            return new OrcFilePruningResult(map);
        } else {
            throw new AssertionError("impossible pruning result");
        }
    }

    @Override
    public String toString() {
        switch (code) {
        case PASS:
            return "PASS";
        case SKIP:
            return "SKIP";
        case PART:
            // choose the first 10
            StringBuilder sb = new StringBuilder("PART[");
            int cnt = 0;
            for (Long id : stripeMap.keySet()) {
                cnt++;
                if (cnt > 10) {
                    break;
                }
                if (cnt > 1) {
                    sb.append(", ");
                }
                sb.append(id);
            }
            if (cnt > 10) {
                sb.append("...");
            }
            sb.append("]");
            return sb.toString();
        default:
            throw new AssertionError("unknown type" + code.name());
        }
    }
}
