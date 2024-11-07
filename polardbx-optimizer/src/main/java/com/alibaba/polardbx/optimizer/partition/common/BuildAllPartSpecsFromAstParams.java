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

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenghui.lch
 */
public class BuildAllPartSpecsFromAstParams {

    protected List<ColumnMeta> partColMetaList;
    protected PartKeyLevel partKeyLevel;
    protected PartitionIntFunction partIntFunc;
    protected SearchDatumComparator pruningSpaceComparator;
    protected List<SqlNode> partitions;
    protected SqlNode partitionCntAst;
    protected Map<SqlNode, RexNode> partBoundExprInfo;
    protected PartitionStrategy partStrategy;
    protected PartitionTableType tblType;
    protected ExecutionContext ec;
    protected int allPhyGroupCnt;
    protected boolean buildSubPartBy;
    protected boolean useSubPartTemplate;
    protected boolean buildSubPartSpecTemplate;
    protected boolean containNextLevelPartSpec;
    protected PartitionSpec parentPartSpec;
    protected AtomicInteger phyPartCounter;
    protected boolean ttlTemporary = false;

    public BuildAllPartSpecsFromAstParams() {
    }

    public List<ColumnMeta> getPartColMetaList() {
        return partColMetaList;
    }

    public void setPartColMetaList(List<ColumnMeta> partColMetaList) {
        this.partColMetaList = partColMetaList;
    }

    public PartitionIntFunction getPartIntFunc() {
        return partIntFunc;
    }

    public void setPartIntFunc(PartitionIntFunction partIntFunc) {
        this.partIntFunc = partIntFunc;
    }

    public SearchDatumComparator getPruningSpaceComparator() {
        return pruningSpaceComparator;
    }

    public void setPruningSpaceComparator(
        SearchDatumComparator pruningSpaceComparator) {
        this.pruningSpaceComparator = pruningSpaceComparator;
    }

    public List<SqlNode> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<SqlNode> partitions) {
        this.partitions = partitions;
    }

    public SqlNode getPartitionCntAst() {
        return partitionCntAst;
    }

    public void setPartitionCntAst(SqlNode partitionCntAst) {
        this.partitionCntAst = partitionCntAst;
    }

    public Map<SqlNode, RexNode> getPartBoundExprInfo() {
        return partBoundExprInfo;
    }

    public void setPartBoundExprInfo(
        Map<SqlNode, RexNode> partBoundExprInfo) {
        this.partBoundExprInfo = partBoundExprInfo;
    }

    public PartitionStrategy getPartStrategy() {
        return partStrategy;
    }

    public void setPartStrategy(PartitionStrategy partStrategy) {
        this.partStrategy = partStrategy;
    }

    public PartitionTableType getTblType() {
        return tblType;
    }

    public void setTblType(PartitionTableType tblType) {
        this.tblType = tblType;
    }

    public ExecutionContext getEc() {
        return ec;
    }

    public void setEc(ExecutionContext ec) {
        this.ec = ec;
    }

    public boolean isBuildSubPartBy() {
        return buildSubPartBy;
    }

    public void setBuildSubPartBy(boolean buildSubPartBy) {
        this.buildSubPartBy = buildSubPartBy;
    }

    public int getAllPhyGroupCnt() {
        return allPhyGroupCnt;
    }

    public void setAllPhyGroupCnt(int allPhyGroupCnt) {
        this.allPhyGroupCnt = allPhyGroupCnt;
    }

    public boolean isUseSubPartTemplate() {
        return useSubPartTemplate;
    }

    public void setUseSubPartTemplate(boolean useSubPartTemplate) {
        this.useSubPartTemplate = useSubPartTemplate;
    }

    public boolean isBuildSubPartSpecTemplate() {
        return buildSubPartSpecTemplate;
    }

    public void setBuildSubPartSpecTemplate(boolean buildSubPartSpecTemplate) {
        this.buildSubPartSpecTemplate = buildSubPartSpecTemplate;
    }

    public PartKeyLevel getPartKeyLevel() {
        return partKeyLevel;
    }

    public void setPartKeyLevel(PartKeyLevel partKeyLevel) {
        this.partKeyLevel = partKeyLevel;
    }

    public boolean isContainNextLevelPartSpec() {
        return containNextLevelPartSpec;
    }

    public void setContainNextLevelPartSpec(boolean containNextLevelPartSpec) {
        this.containNextLevelPartSpec = containNextLevelPartSpec;
    }

    public PartitionSpec getParentPartSpec() {
        return parentPartSpec;
    }

    public void setParentPartSpec(PartitionSpec parentPartSpec) {
        this.parentPartSpec = parentPartSpec;
    }

    public AtomicInteger getPhyPartCounter() {
        return phyPartCounter;
    }

    public void setPhyPartCounter(AtomicInteger phyPartCounter) {
        this.phyPartCounter = phyPartCounter;
    }

    public boolean isTtlTemporary() {
        return ttlTemporary;
    }

    public void setTtlTemporary(boolean ttlTemporary) {
        this.ttlTemporary = ttlTemporary;
    }
}
