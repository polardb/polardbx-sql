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
package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Callback for a relational expression to dump itself as JSON.
 */
public class DRDSRelJsonWriter extends RelJsonWriter {
    //~ Instance fields ----------------------------------------------------------

    private boolean supportMpp = false;

    private PlannerContext plannerContext = null;

    //~ Constructors -------------------------------------------------------------

    public DRDSRelJsonWriter(boolean supportMpp) {
        relJson = new DRDSRelJson(jsonBuilder, supportMpp);
        this.supportMpp = supportMpp;
    }

    @Override
    protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
        final Map<String, Object> map = jsonBuilder.map();

        map.put("id", null); // ensure that id is the first attribute
        map.put("relOp", relJson.classToTypeName(rel.getClass()));
        for (Pair<String, Object> value : values) {
            if (value.right instanceof RelNode) {
                continue;
            }
            put(map, value.left, value.right);
        }
        if (!(rel instanceof LogicalView)) {
            // omit 'inputs: ["3"]' if "3" is the preceding rel
            final List<Object> list = explainInputs(rel.getInputs());
            if (list.size() != 1 || !list.get(0).equals(previousId)) {
                map.put("inputs", list);
            }
        }

        final String id = Integer.toString(relIdMap.size());
        relIdMap.put(rel, id);
        map.put("id", id);
        if (supportMpp) {
            map.put("relatedId", rel.getRelatedId());
            if (plannerContext == null) {
                this.plannerContext = PlannerContext.getPlannerContext(rel);
            }
            final List<Integer> sources = new ArrayList<>();
            for (RelNode input : rel.getInputs()) {
                sources.add(input.getRelatedId());
            }
            map.put("sources", sources);
        }
        relList.add(map);
        previousId = id;
    }

}

// End RelJsonWriter.java
