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

import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.ttl.TtlPartArcState;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;

/**
 * @author chenghui.lch
 */
public abstract class PartSpecBase {

    /**
     * the partition strategy of current partitions
     */
    protected PartitionStrategy strategy;

    /**
     * the definition of one partition bound info
     */
    protected PartitionBoundSpec boundSpec;

    /**
     * The unique id for one partition or subpartition
     */
    protected Long id;

    /**
     * The unique id for the parent of one partition or subpartition
     */
    protected Long parentId;

    /**
     * The position of the parent partition of subpartition,
     * if current part level is NOT sub-part-level, the parentPartPosi will be 0.
     * if current part level is a sub-part-level, the parentPartPosi will be > 0.
     */
    protected Long parentPartPosi = 0L;

    /**
     * The boundVal order position of current (sub)partition among the all (sub)partitions of parent partition
     * Notice: For list partition, the boundVal order is perform as following:
     * sort all values for each partitions by asc, and use the first value of each part to sort all the partitions
     * the default partition are always the last partition
     * <pre>
     *     Case 1: for a 1-level-partition table
     *           the position of the first partition is 1,
     *           the position of the second partition is 2, ......
     *
     *     Case 1: for a 2-level-partition table (using subpartition)
     *      if curr part level is 1-level-partition,then:
     *           the position of the first partition is 1,
     *           the position of the second partition is 2, ......
     *     if curr part level is 2-level-partition,then:
     *          the position of the first subpartition of one partition is 1,
     *          the position of the second subpartition of one partition is 2, ......
     * </pre>
     */
    protected Long position;

    /**
     * The phyPartPosition of current physical partition
     * among the all physical partitions of a logical table., start witth 1,2,3...
     * if the value of phyPartPosition that means curr part is a logical part
     * <p>
     * The variables is only IN-MEMORY, build by loading partInfo from metadb.
     * <pre>
     *     Create table t (...)
     *     partition by key(pk) partitions 4
     *
     *     Case 1: for a 1-level-partition table (all partitions are physical partitions)
     *           the phyPartPosition of the first partition is 1,
     *           the phyPartPosition of the second partition is 2, ......
     *
     *     Create table t (...)
     *     partition by key(pk1)
     *     subpartition by key(pk2)
     *     subpartitions 2
     *     partitions 4
     *
     *     Case 2:
     *     for a 2-level-partition table  (all subpartitions are physical partitions,  all partitions are logical partitions)
     *      if curr part level is 1-level-partition,then:
     *           the phyPartPosition of all global partition is null
     *     if curr part level is 2-level-partition,then:
     *          the phyPartPosition of the 1st subpartition of the 1st partition is 1,
     *          the phyPartPosition of the 1st subpartition of the 3rd partition is (2+2+1)=5,
     *          the phyPartPosition of the 2nd subpartition of the 4th partition is (2+2+2+2)=8,
     *          ......
     * </pre>
     */
    protected Long phyPartPosition = 0L;

    /**
     * The (sub)partition name
     */
    protected String name;

    /**
     * The (sub)partition comment
     */
    protected String comment;

    /**
     * The (sub)partition engine type
     */
    protected String engine = TablePartitionRecord.PARTITION_ENGINE_INNODB;

    /**
     * The partition status
     */
    protected Integer status;

    /**
     * The extras config
     */
    protected ExtraFieldJSON extras;

    /**
     * The flags
     */
    protected Long flags;

    /**
     * The version of partition
     */
    protected Long version;

    /**
     * Only for range/range column partition:
     * flag if contain max-value in boundValue
     */
    protected boolean isMaxValueRange;

    /**
     * The phy location of the partition.
     * It will be null during create ddl and can be filled by PartitionLocation.
     * <p>
     * Notice:
     * If the partitionInfo has subpartitions, only those subpartitions has locations
     */
    protected PartitionLocation location;

    /**
     * Locality of this partition
     */
    protected String locality;

    /**
     * The comparator of bound Value Space
     */
    protected SearchDatumComparator boundSpaceComparator;

    /**
     * the sort key (sorted by partition position) of curr partition in its phy db.
     * <pre>
     * e.g.                 testdb_p00000 have partitions: p1,p4,p8,p11,p23
     *        the orderNum of partitions in testdb_p00000: 0,1,2,3,4,5
     * </pre>
     */
    protected Long intraGroupConnKey;

    /**
     * Only for List/ List Column partition:
     * Flag if is Default Partition
     */
    protected boolean isDefaultPartition;

    /**
     * Label the level of current part spec
     */
    protected PartKeyLevel partLevel;

    /**
     * Label if current partition is a logical partition
     */
    protected boolean isLogical = false;

    /**
     * Label if current part spec is a template definition, only use for subpartition
     */
    protected boolean isSpecTemplate = false;

    /**
     * the name of partition in template, only use for subpartition
     * when isTemplate=true, templateName will always be the same as name
     */
    protected String templateName;

    /**
     * Label if current partSpec is using a subpartition template definition
     */
    protected boolean useSpecTemplate = false;

    /**
     * Label if current partSpec (including its subparts if exists)
     * of ttl-tmp table has been ready for oss data archiving, default is no_use
     */
    protected Integer arcState = TtlPartArcState.ARC_STATE_NO_USE.getArcState();

    public PartitionStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(PartitionStrategy strategy) {
        this.strategy = strategy;
    }

    public PartitionBoundSpec getBoundSpec() {
        return boundSpec;
    }

    public void setBoundSpec(PartitionBoundSpec boundSpec) {
        this.boundSpec = boundSpec;
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

    public Long getPosition() {
        return position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public ExtraFieldJSON getExtras() {
        return extras;
    }

    public void setExtras(ExtraFieldJSON extras) {
        this.extras = extras;
    }

    public Long getFlags() {
        return flags;
    }

    public void setFlags(Long flags) {
        this.flags = flags;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public boolean isMaxValueRange() {
        return isMaxValueRange;
    }

    public void setMaxValueRange(boolean maxValueRange) {
        isMaxValueRange = maxValueRange;
    }

    public PartitionLocation getLocation() {
        return location;
    }

    public void setLocation(PartitionLocation location) {
        this.location = location;
    }

    public String getLocality() {
        return locality;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public SearchDatumComparator getBoundSpaceComparator() {
        return boundSpaceComparator;
    }

    public void setBoundSpaceComparator(
        SearchDatumComparator boundSpaceComparator) {
        this.boundSpaceComparator = boundSpaceComparator;
    }

    public Long getIntraGroupConnKey() {
        return intraGroupConnKey;
    }

    public void setIntraGroupConnKey(Long intraGroupConnKey) {
        this.intraGroupConnKey = intraGroupConnKey;
    }

    public boolean isDefaultPartition() {
        return isDefaultPartition;
    }

    public void setDefaultPartition(boolean defaultPartition) {
        isDefaultPartition = defaultPartition;
    }

    public boolean isSpecTemplate() {
        return isSpecTemplate;
    }

    public void setSpecTemplate(boolean specTemplate) {
        isSpecTemplate = specTemplate;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public PartKeyLevel getPartLevel() {
        return partLevel;
    }

    public void setPartLevel(PartKeyLevel partLevel) {
        this.partLevel = partLevel;
    }

    public boolean isLogical() {
        return isLogical;
    }

    public void setLogical(boolean logical) {
        isLogical = logical;
    }

    public Long getPhyPartPosition() {
        return phyPartPosition;
    }

    public void setPhyPartPosition(Long phyPartPosition) {
        this.phyPartPosition = phyPartPosition;
    }

    public boolean isUseSpecTemplate() {
        return useSpecTemplate;
    }

    public void setUseSpecTemplate(boolean useSpecTemplate) {
        this.useSpecTemplate = useSpecTemplate;
    }

    public Long getParentPartPosi() {
        return parentPartPosi;
    }

    public void setParentPartPosi(Long parentPartPosi) {
        this.parentPartPosi = parentPartPosi;
    }

    public int getArcState() {
        return arcState;
    }

    public void setArcState(Integer arcState) {
        this.arcState = arcState;
    }
}
