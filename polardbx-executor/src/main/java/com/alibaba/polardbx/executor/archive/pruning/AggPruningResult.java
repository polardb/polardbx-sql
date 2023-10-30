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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;

import java.util.Map;
import java.util.Set;

public class AggPruningResult implements PruningResult {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrcFilePruningResult.class);

    protected Map<Long, StripeColumnMeta> stripeMap;

    private final CODE code;

    private final Set<Long> aggNotUsed;

    public final static AggPruningResult SKIP = new AggPruningResult(CODE.STATISTICS_SKIP);

    public final static AggPruningResult PASS = new AggPruningResult(CODE.STATISTICS_PASS);

    public final static AggPruningResult NO_SCAN = new AggPruningResult(CODE.STATISTICS_FULL);

    private AggPruningResult(CODE code) {
        this.code = code;
        this.aggNotUsed = null;
    }

    public AggPruningResult(Map<Long, StripeColumnMeta> stripeMap) {
        this.code = CODE.STATISTICS_PART;
        this.stripeMap = Maps.newHashMap(stripeMap);
        this.aggNotUsed = Sets.newHashSet();
    }

    private enum CODE {
        /**
         * the whole file can't use statistics
         */
        STATISTICS_PASS,

        /**
         * stripes of the file can use statistics or not
         */
        STATISTICS_PART,

        /**
         * the whole file can use statistics
         */
        STATISTICS_FULL,

        /**
         * the whole file is pruned
         */
        STATISTICS_SKIP
    }

    @Override
    public Map<Long, StripeColumnMeta> getStripeMap() {
        return stripeMap;
    }

    @Override
    public RangeSet<Long> getRangeSet() {
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        for (StripeColumnMeta stripeColumnMeta : stripeMap.values()) {
            rangeSet.add(Range.openClosed(stripeColumnMeta.getStripeOffset(),
                stripeColumnMeta.getStripeOffset() + stripeColumnMeta.getStripeLength()));
        }
        return rangeSet;
    }

    public boolean pass() {
        return code == CODE.STATISTICS_PASS;
    }

    public boolean part() {
        return code == CODE.STATISTICS_PART;
    }

    public boolean skip() {
        return code == CODE.STATISTICS_SKIP;
    }

    public boolean noscan() {
        return code == CODE.STATISTICS_FULL;
    }

    public void addNotAgg(Long index) {
        aggNotUsed.add(index);
    }

    public void addNotAgg(Set<Long> indexes) {
        aggNotUsed.addAll(indexes);
    }

    /**
     * whether a stripe use statistics or not
     *
     * @param index the index of target stripe
     * @return true if the stripe can use statistics
     */
    public boolean stat(Long index) {
        return aggNotUsed != null && !aggNotUsed.contains(index);
    }

    public int getNonStatisticsStripeSize() {
        return aggNotUsed == null ? 0 : aggNotUsed.size();
    }

    public void log() {
        if (code == CODE.STATISTICS_PART) {
            LOGGER.info("stripes using statistics : " + (stripeMap.size() - getNonStatisticsStripeSize())
                + " stripes can't use statistics: " + getNonStatisticsStripeSize());
        }
    }

    public PruningResult intersect(PruningResult other) {
        throw new UnsupportedOperationException("intersect not supported in AggPruningResult");
    }

    public PruningResult union(PruningResult other) {
        throw new UnsupportedOperationException("union not supported in AggPruningResult");
    }

    @Override
    public String toString() {
        switch (code) {
        case STATISTICS_PASS:
            return "STATISTICS_PASS";
        case STATISTICS_SKIP:
            return "STATISTICS_SKIP";
        case STATISTICS_FULL:
            return "STATISTICS_FULL";
        case STATISTICS_PART:
            // choose the first 10
            StringBuilder sb = new StringBuilder("PART[");
            int cnt = 0;
            for (Long id : aggNotUsed) {
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
            // part
            return sb.toString();
        default:
            throw new AssertionError("unknown type" + code.name());
        }
    }

}
