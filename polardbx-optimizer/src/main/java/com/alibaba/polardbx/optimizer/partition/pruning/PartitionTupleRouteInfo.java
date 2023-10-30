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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionTupleRouteInfo {

    protected String schemaName;
    protected String tableName;
    protected PartitionInfo partInfo;

    /**
     * PartDispatchFuncInfo for each tuple template
     * <p>
     * <pre>
     *     For insert values stmt, it may contain multi different tuple template.
     *     such as
     *      insert into tbl (a,b,c) values (?,?+?,?),(?,?-?,?),(?,?,?+?+?)
     *     , "(?,?+?,?)","(?,?-?,?)" and "(?,?,?+?+?)" are different template
     *     so each tuple template should has its own PartDispatchFuncInfo
     * </pre>
     */
    protected List<PartTupleDispatchInfo> tupleDispatchFuncInfos;

    public PartitionTupleRouteInfo() {
    }

    protected PartPrunedResult routeTuple(int tupleTempleIdx, ExecutionContext ec,
                                          PartPruneStepPruningContext pruningCtx) {
        return tupleDispatchFuncInfos.get(tupleTempleIdx).routeTuple(ec, pruningCtx);
    }

    public List<SearchDatumInfo> calcSearchDatum(int tupleTempleIdx, ExecutionContext ec,
                                                 PartPruneStepPruningContext pruningCtx) {
        return tupleDispatchFuncInfos.get(tupleTempleIdx).calcSearchDatum(ec, pruningCtx);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public List<PartTupleDispatchInfo> getTupleDispatchFuncInfos() {
        return tupleDispatchFuncInfos;
    }

    public void setTupleDispatchFuncInfos(
        List<PartTupleDispatchInfo> tupleDispatchFuncInfos) {
        this.tupleDispatchFuncInfos = tupleDispatchFuncInfos;
    }
}
