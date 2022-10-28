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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * check the correctness of the branch bound search algorithm
 * @author shengyu
 */
public class NaiveJoinGraph extends JoinGraph {

    private static final Logger logger = LoggerFactory.getLogger(NaiveJoinGraph.class);

    NaiveJoinGraph() {
        super();
    }

    NaiveJoinGraph(List<DirectedEdges> graph, Map<String, Integer> nameToId,
                   List<String> idToName, boolean[] broadcast, ParamManager paramManager) {
        super(graph, nameToId, idToName, broadcast, false, paramManager);
    }

    boolean run = true;
    long start = 0L;

    @Override
    protected void search() {
        if (!run) {
            return;
        }
        start = System.currentTimeMillis();
        CandidateShardResult candidate = new CandidateShardResult(this);
        // add one table at a time
        search(candidate, 0L);
        PruneResult pr = new PruneResult(paramManager);
        logger.debug(pr.printResult(0, best));
    }

    @Override
    protected void search(CandidateShardResult candidate, long upper) {
        if (candidate.getLevel() >= nodeNum) {
            long weight = candidate.computeWeight(0);
            if (best == null || weight > best.getWeight()) {
                best = candidate.toResult();
                best.setWeight(weight);
            }
            return;
        }
        if (System.currentTimeMillis() - start > 600 * 1000) {
            run = false;
            return;
        }
        if (broadcast[candidate.getLevel()]) {
            candidate.setColumn(AdvisorUtil.BROADCAST);
            search(candidate, 0L);
            candidate.unsetColumn();
        }
        for (Map.Entry<Integer, List<Edge>> entry : getEdges(candidate.getLevel()).entrySet()) {
            if (!run) {
                return;
            }
            // choose a column for sharding
            int col = entry.getKey();
            candidate.setColumn(col);
            search(candidate, 0L);
            //backtrace
            candidate.unsetColumn();
        }
    }
}
