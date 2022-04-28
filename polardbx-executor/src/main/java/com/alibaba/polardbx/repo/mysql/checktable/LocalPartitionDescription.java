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

import com.google.common.base.Preconditions;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;

/**
 * information_schema.partitions
 */
@Data
public class LocalPartitionDescription {

    private String  tableCatalog;
    private String  tableSchema;
    private String  tableName;
    private String  partitionName;
    private String  subpartitionName;
    private Long    partitionOrdinalPosition;
    private Long    subpartitionOrdinalPosition;
    private String  partitionMethod;
    private String  subpartitionMethod;
    private String  partitionExpression;
    private String  subpartitionExpression;
    private String  partitionDescription;
    private Long    tableRows;
    private Long    avgRowLength;
    private Long    dataLength;
    private Long    maxDataLength;
    private Long    indexLength;
    private Long    dataFree;
    private Date    createTime;
    private Date    updateTime;
    private Date    checkTime;
    private Long    checksum;
    private String  partitionComment;
    private String  nodegroup;
    private String  tablespaceName;

    public int comparePartitionOrdinalPosition(LocalPartitionDescription d){
        Preconditions.checkNotNull(partitionOrdinalPosition);
        Preconditions.checkNotNull(d);
        Preconditions.checkNotNull(d.getPartitionOrdinalPosition());
        return partitionOrdinalPosition.compareTo(d.getPartitionOrdinalPosition());
    }

    public boolean rangeIdentical(LocalPartitionDescription d){
        Preconditions.checkNotNull(d);
        return StringUtils.equalsIgnoreCase(partitionDescription, d.getPartitionDescription());
    }

}