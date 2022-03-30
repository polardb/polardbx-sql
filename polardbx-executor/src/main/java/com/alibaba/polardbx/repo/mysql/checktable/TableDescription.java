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

package com.alibaba.polardbx.repo.mysql.checktable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 表描述
 *
 * @author arnkore 2017-06-19 17:44
 */
public class TableDescription {

    protected String groupName;
    protected String tableName;
    protected Map<String, FieldDescription> fields = new HashMap<String, FieldDescription>();
    protected Map<String, FieldDescription> physicalOrderFields = new LinkedHashMap<String, FieldDescription>();
    /**
     * key: partition name
     * value: partition detail info
     */
    protected List<LocalPartitionDescription> partitions = new ArrayList<>();

    public TableDescription() {
    }

    public TableDescription(final String groupName, final String tableName) {
        this.groupName = groupName;
        this.tableName = tableName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, FieldDescription> getFields() {
        return fields;
    }

    public void setFields(Map<String, FieldDescription> fields) {
        this.fields = fields;
    }

    public Map<String, FieldDescription> getPhysicalOrderFields() {
        return physicalOrderFields;
    }

    public void setPhysicalOrderFields(Map<String, FieldDescription> fields) {
        this.physicalOrderFields = fields;
    }
    public List<LocalPartitionDescription> getPartitions() {
        return this.partitions;
    }

    public void setPartitions(final List<LocalPartitionDescription> partitions) {
        Preconditions.checkNotNull(partitions);
        this.partitions = partitions;
    }

    public String getPartitionDescriptionString(){
        if(CollectionUtils.isEmpty(partitions) || (partitions.size()==1 && partitions.get(0).getPartitionName()==null)){
            return "";
        }
        List<String> partitionDescriptionList =
            partitions.stream()
                .map(LocalPartitionDescription::getPartitionDescription)
                .map(e-> StringUtils.replace(e, "'", ""))
                .collect(Collectors.toList());
        return Joiner.on(",").join(partitionDescriptionList);
    }

    public boolean containsLocalPartition(String name){
        if(StringUtils.isEmpty(name)){
            return false;
        }
        for(LocalPartitionDescription localPartitionDescription: partitions){
            if(StringUtils.equalsIgnoreCase(localPartitionDescription.getPartitionName(), name)){
                return true;
            }
        }
        return false;
    }

    public boolean isEmptyPartition(){
        return fields == null || fields.isEmpty();
    }
}
