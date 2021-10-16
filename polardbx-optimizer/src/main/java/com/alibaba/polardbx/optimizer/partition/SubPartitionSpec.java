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


/**
 * The definition of one (sub)partition
 *
 * @author chenghui.lch
 */
public class SubPartitionSpec {

    /**
     * the template definition of one subpartition
     */
    protected SubPartitionSpecTemplate subPartitionSpecInfo;

    /**
     * The unique id for one partition or subpartition
     */
    protected Long id;

    /**
     * The unique id for the parent of one partition or subpartition
     */
    protected Long parentId;

    /**
     * the actual subpartition name
     */
    protected String name;

    /**
     * The (sub)partition status
     */
    protected int status;

    /**
     * The extras config
     */
    protected String extras;


    /**
     * The version of partition
     */
    protected String version;

    /**
     *  The phy location of the partition.
     *  It will be null during create ddl and can be filled by PartitionLocation.
     *
     *  Notice:
     *  If the partitionInfo has subpartitions, only those subpartitions has locations
     *
     */
    protected PartitionLocation location;

    public SubPartitionSpec() {
    }

    public SubPartitionSpec copy() {
        SubPartitionSpec subPartSpec = new SubPartitionSpec();
        subPartSpec.setId(this.id);
        subPartSpec.setParentId(this.parentId);
        subPartSpec.setName(this.name);
        subPartSpec.setStatus(this.status);
        subPartSpec.setExtras(this.extras);
        subPartSpec.setVersion(this.version);
        subPartSpec.setLocation(this.location.copy());
        subPartSpec.setSubPartitionSpecInfo(subPartitionSpecInfo.copy());
        return subPartSpec;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SubPartitionSpecTemplate getSubPartitionSpecInfo() {
        return subPartitionSpecInfo;
    }

    public void setSubPartitionSpecInfo(SubPartitionSpecTemplate subPartitionSpecInfo) {
        this.subPartitionSpecInfo = subPartitionSpecInfo;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public PartitionLocation getLocation() {
        return location;
    }

    public void setLocation(PartitionLocation location) {
        this.location = location;
    }

    public String getExtras() {
        return extras;
    }

    public void setExtras(String extras) {
        this.extras = extras;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
