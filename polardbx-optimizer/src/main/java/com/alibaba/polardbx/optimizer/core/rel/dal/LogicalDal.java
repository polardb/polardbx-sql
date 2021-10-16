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

package com.alibaba.polardbx.optimizer.core.rel.dal;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.dal.Dal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class LogicalDal extends BaseDalOperation {

    protected CursorMeta cursorMeta;
    protected Dal dal;

    LogicalDal(Dal dal, String dbIndex, String phyTable, String schemaName) {
        super(dal.getCluster(), dal.getTraitSet(), dal.getAst(), dal.getRowType(), dbIndex, phyTable, schemaName);
        this.dal = dal;
        this.dbIndex = dbIndex;
        this.phyTable = phyTable;
        this.cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(rowType, getExplainName()));
    }

    public static LogicalDal create(Dal dal, String dbIndex, String phyTable, String schemaName) {
        return new LogicalDal(dal, dbIndex, phyTable, schemaName);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return cursorMeta;
    }

    public void setCursorMeta(CursorMeta cursorMeta) {
        this.cursorMeta = cursorMeta;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           ExecutionContext executionContext) {
        param = new HashMap<>();
        return Pair.of(dbIndex, param);
    }

    @Override
    protected String getExplainName() {
        return "LogicalDal";
    }

    @Override
    public LogicalDal copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return create(dal.copy(traitSet, inputs), dbIndex, phyTable, schemaName);
    }
}
