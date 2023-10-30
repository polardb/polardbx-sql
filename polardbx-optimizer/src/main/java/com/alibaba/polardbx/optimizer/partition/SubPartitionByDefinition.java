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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class SubPartitionByDefinition {

    /**
     * the partition strategy of current partitions
     */
    protected PartitionStrategy strategy;

    /**
     * The raw subpartition expression from create tbl ddl or meta db,
     * or null if has no subpartition
     */
    protected List<SqlNode> subPartitionExprList;

    /**
     * the definition of subpartition template
     */
    protected List<SubPartitionSpecTemplate> subPartitionsTemplate;

    /**
     * list of fields used in subpartitioned expression
     */
    protected List<ColumnMeta> subPartitionFieldList;

    /**
     * list of column name of subpartition fields
     */
    protected List<String> subPartitionColumnNameList;

    public SubPartitionByDefinition() {
        subPartitionsTemplate = new ArrayList<>();
        subPartitionFieldList = new ArrayList<>();
        subPartitionColumnNameList = new ArrayList<>();
    }

    public SubPartitionByDefinition copy() {
        SubPartitionByDefinition subPartDef = new SubPartitionByDefinition();
        subPartDef.setStrategy(this.strategy);

        List<SqlNode> newSubPartitionExprList = new ArrayList<>();
        newSubPartitionExprList.addAll(this.subPartitionExprList);
        subPartDef.setSubPartitionExprList(newSubPartitionExprList);

        List<SubPartitionSpecTemplate> newSubPartitionsTemplate = new ArrayList<>();
        for (int i = 0; i < this.subPartitionsTemplate.size(); i++) {
            newSubPartitionsTemplate.add(this.subPartitionsTemplate.get(i).copy());
        }
        subPartDef.setSubPartitionsTemplate(this.subPartitionsTemplate);

        List<ColumnMeta> newSubPartitionFieldList = new ArrayList<>();
        newSubPartitionFieldList.addAll(this.subPartitionFieldList);
        subPartDef.setSubPartitionFieldList(newSubPartitionFieldList);

        List<String> newSubPartitionColumnNameList = new ArrayList<>();
        newSubPartitionColumnNameList.addAll(this.subPartitionColumnNameList);
        subPartDef.setSubPartitionColumnNameList(newSubPartitionColumnNameList);

        return subPartDef;
    }

    public PartitionStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(PartitionStrategy strategy) {
        this.strategy = strategy;
    }

    public List<SqlNode> getSubPartitionExprList() {
        return subPartitionExprList;
    }

    public void setSubPartitionExprList(List<SqlNode> subPartitionExprList) {
        this.subPartitionExprList = subPartitionExprList;
    }

    public List<SubPartitionSpecTemplate> getSubPartitionsTemplate() {
        return subPartitionsTemplate;
    }

    public void setSubPartitionsTemplate(
        List<SubPartitionSpecTemplate> subPartitionsTemplate) {
        this.subPartitionsTemplate = subPartitionsTemplate;
    }

    public List<ColumnMeta> getSubPartitionFieldList() {
        return subPartitionFieldList;
    }

    public void setSubPartitionFieldList(List<ColumnMeta> subPartitionFieldList) {
        this.subPartitionFieldList = subPartitionFieldList;
    }

    public List<String> getSubPartitionColumnNameList() {
        return subPartitionColumnNameList;
    }

    public void setSubPartitionColumnNameList(List<String> subPartitionColumnNameList) {
        this.subPartitionColumnNameList = subPartitionColumnNameList;
    }
}
