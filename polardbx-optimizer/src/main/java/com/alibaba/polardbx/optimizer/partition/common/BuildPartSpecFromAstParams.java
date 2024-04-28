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

import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartBoundValBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionValue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenghui.lch
 */
public class BuildPartSpecFromAstParams {

    protected ExecutionContext context;
    protected List<ColumnMeta> partColMetaList;
    protected PartKeyLevel partKeyLevel;
    protected PartitionIntFunction partIntFunc;
    protected SearchDatumComparator pruningComparator;
    protected Map<SqlNode, RexNode> partBoundExprInfo;
    protected PartBoundValBuilder partBoundValBuilder;

    //protected SqlPartition partSpecAst;
    protected boolean autoBuildPart;
    protected SqlNode partNameAst;
    protected SqlPartitionValue partBndValuesAst;
    protected String partComment;
    protected String partLocality;

    protected PartitionStrategy strategy;
    protected long partPosition;
    protected List<Integer> allLevelPrefixPartColCnts;

    protected boolean isLogical;
    protected boolean isSpecTemplate;
    protected boolean isSubPartSpec;
    protected boolean useSpecTemplate;

    protected AtomicInteger phySpecCounter;
    protected PartitionSpec parentPartSpec;
    protected String partEngine = TablePartitionRecord.PARTITION_ENGINE_INNODB;

    public BuildPartSpecFromAstParams() {
    }

    public ExecutionContext getContext() {
        return context;
    }

    public void setContext(ExecutionContext context) {
        this.context = context;
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

    public SearchDatumComparator getPruningComparator() {
        return pruningComparator;
    }

    public void setPruningComparator(SearchDatumComparator pruningComparator) {
        this.pruningComparator = pruningComparator;
    }

    public Map<SqlNode, RexNode> getPartBoundExprInfo() {
        return partBoundExprInfo;
    }

    public void setPartBoundExprInfo(
        Map<SqlNode, RexNode> partBoundExprInfo) {
        this.partBoundExprInfo = partBoundExprInfo;
    }

    public PartBoundValBuilder getPartBoundValBuilder() {
        return partBoundValBuilder;
    }

    public void setPartBoundValBuilder(
        PartBoundValBuilder partBoundValBuilder) {
        this.partBoundValBuilder = partBoundValBuilder;
    }

    public SqlNode getPartNameAst() {
        return partNameAst;
    }

    public void setPartNameAst(SqlNode partNameAst) {
        this.partNameAst = partNameAst;
    }

    public SqlPartitionValue getPartBndValuesAst() {
        return partBndValuesAst;
    }

    public void setPartBndValuesAst(SqlPartitionValue partBndValuesAst) {
        this.partBndValuesAst = partBndValuesAst;
    }

    public String getPartComment() {
        return partComment;
    }

    public void setPartComment(String partComment) {
        this.partComment = partComment;
    }

    public String getPartLocality() {
        return partLocality;
    }

    public void setPartLocality(String partLocality) {
        this.partLocality = partLocality;
    }

    public PartitionStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(PartitionStrategy strategy) {
        this.strategy = strategy;
    }

    public long getPartPosition() {
        return partPosition;
    }

    public void setPartPosition(long partPosition) {
        this.partPosition = partPosition;
    }

    public boolean isAutoBuildPart() {
        return autoBuildPart;
    }

    public void setAutoBuildPart(boolean autoBuildPart) {
        this.autoBuildPart = autoBuildPart;
    }

    public PartKeyLevel getPartKeyLevel() {
        return partKeyLevel;
    }

    public void setPartKeyLevel(PartKeyLevel partKeyLevel) {
        this.partKeyLevel = partKeyLevel;
    }

    public boolean isLogical() {
        return isLogical;
    }

    public void setLogical(boolean logical) {
        isLogical = logical;
    }

    public boolean isSpecTemplate() {
        return isSpecTemplate;
    }

    public void setSpecTemplate(boolean specTemplate) {
        isSpecTemplate = specTemplate;
    }

    public PartitionSpec getParentPartSpec() {
        return parentPartSpec;
    }

    public void setParentPartSpec(PartitionSpec parentPartSpec) {
        this.parentPartSpec = parentPartSpec;
    }

    public boolean isSubPartSpec() {
        return isSubPartSpec;
    }

    public void setSubPartSpec(boolean subPartSpec) {
        isSubPartSpec = subPartSpec;
    }

    public AtomicInteger getPhySpecCounter() {
        return phySpecCounter;
    }

    public void setPhySpecCounter(AtomicInteger phySpecCounter) {
        this.phySpecCounter = phySpecCounter;
    }

    public boolean isUseSpecTemplate() {
        return useSpecTemplate;
    }

    public void setUseSpecTemplate(boolean useSpecTemplate) {
        this.useSpecTemplate = useSpecTemplate;
    }

    public List<Integer> getAllLevelPrefixPartColCnts() {
        return allLevelPrefixPartColCnts;
    }

    public void setAllLevelPrefixPartColCnts(List<Integer> allLevelPrefixPartColCnts) {
        this.allLevelPrefixPartColCnts = allLevelPrefixPartColCnts;
    }

    public String getPartEngine() {
        return partEngine;
    }

    public void setPartEngine(String partEngine) {
        this.partEngine = partEngine;
    }
}
