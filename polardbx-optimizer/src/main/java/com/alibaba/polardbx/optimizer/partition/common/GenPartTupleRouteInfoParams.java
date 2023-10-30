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

package com.alibaba.polardbx.optimizer.partition.common;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class GenPartTupleRouteInfoParams {
    protected String schemaName;
    protected String logTbName;
    protected PartitionInfo specificPartInfo;
    protected RelDataType valueRowType;
    protected List<List<SqlNode>> astValues;
    protected ExecutionContext ec;

    public GenPartTupleRouteInfoParams() {
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getLogTbName() {
        return logTbName;
    }

    public void setLogTbName(String logTbName) {
        this.logTbName = logTbName;
    }

    public PartitionInfo getSpecificPartInfo() {
        return specificPartInfo;
    }

    public void setSpecificPartInfo(PartitionInfo specificPartInfo) {
        this.specificPartInfo = specificPartInfo;
    }

    public RelDataType getValueRowType() {
        return valueRowType;
    }

    public void setValueRowType(RelDataType valueRowType) {
        this.valueRowType = valueRowType;
    }

    public List<List<SqlNode>> getAstValues() {
        return astValues;
    }

    public void setAstValues(List<List<SqlNode>> astValues) {
        this.astValues = astValues;
    }

    public ExecutionContext getEc() {
        return ec;
    }

    public void setEc(ExecutionContext ec) {
        this.ec = ec;
    }
}
