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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "OptimizeTablePhyDdlTask")
public class OptimizeTablePhyDdlTask extends BasePhyDdlTask {

    public OptimizeTablePhyDdlTask(String schemaName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
        onExceptionTryRollback();
    }

    public List<OptimizeTablePhyDdlTask> partition(int physicalTableCount){

        List<Map<String, List<List<String>>>> topos = this.physicalPlanData.partitionTableTopology(physicalTableCount);
        List<List<Map<Integer, ParameterContext>>> params = this.physicalPlanData.partitionParamsList(physicalTableCount);

        List<OptimizeTablePhyDdlTask> result = new ArrayList<>();
        for(int i=0;i<topos.size();i++){
            PhysicalPlanData p = this.physicalPlanData.clone();
            Map<String, List<List<String>>> topoMap = topos.get(i);
            p.setTableTopology(topoMap);

            List<Map<Integer, ParameterContext>> paramList = new ArrayList<>();
            for(Map.Entry<String, List<List<String>>> entry: topoMap.entrySet()){
                int size = entry.getValue().size();
                for(int j=0;j<size;j++){
                    Map<Integer, ParameterContext> parameterContextMap = new HashMap<>();
                    parameterContextMap.put(1, new ParameterContext(ParameterMethod.setTableName, new Object[]{
                            1, entry.getValue().get(j).get(0)
                    }));
                    paramList.add(parameterContextMap);
                }
            }
            p.setParamsList(paramList);

            result.add(new OptimizeTablePhyDdlTask(this.schemaName, p));
        }

        if(FailPoint.isAssertEnable()){
            result.forEach(OptimizeTablePhyDdlTask::validatePartitionPlan);
        }

        return result;
    }

    private void validatePartitionPlan(){
        if(FailPoint.isAssertEnable()){
            int index = 0;
            for (Map.Entry<String, List<List<String>>> topology : this.physicalPlanData.getTableTopology().entrySet()) {
                for (List<String> phyTableNames : topology.getValue()) {
                    final String phyTableName = (String) this.physicalPlanData.getParamsList().get(index++).get(1).getValue();
                    if(!StringUtils.equalsIgnoreCase(
                            phyTableNames.get(0).replace("`",""),
                            phyTableName.replace("`",""))){
                        throw new RuntimeException("generate optimize table plan error");
                    }
                }
            }
        }
    }

}
