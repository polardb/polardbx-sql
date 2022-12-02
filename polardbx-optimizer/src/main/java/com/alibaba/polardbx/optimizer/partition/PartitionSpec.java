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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The definition of one (sub)partition
 *
 * @author chenghui.lch
 */
public class PartitionSpec {

    /**
     * the definition of subpartition template
     */
    protected List<SubPartitionSpec> subPartitions;

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
     * The position of current (sub)partition among all partitions.
     * <p>
     * the first partition is 1, the second partition is 2, ......
     */
    protected Long position;

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
     * */
    protected boolean isDefaultPartition;

    public PartitionSpec() {
        this.subPartitions = new ArrayList<>();
        isDefaultPartition = false;
    }

    public PartitionSpec copy() {

        PartitionSpec spec = new PartitionSpec();
        spec.setStrategy(this.strategy);
        spec.setId(this.id);
        spec.setParentId(this.parentId);
        spec.setPosition(this.position);
        spec.setName(this.name);
        spec.setComment(this.comment);
        spec.setEngine(this.engine);
        spec.setStatus(this.status);
        spec.setExtras(this.extras);
        spec.setFlags(this.flags);
        spec.setVersion(this.version);
        spec.setMaxValueRange(this.isMaxValueRange);
        spec.setBoundSpaceComparator(this.boundSpaceComparator);
        spec.setIsDefaultPartition(this.isDefaultPartition);
        spec.setIntraGroupConnKey(this.intraGroupConnKey);

        if (this.location != null) {
            spec.setLocation(this.location.copy());
        } else {
            spec.setLocation(null);
        }
        spec.setLocality(this.locality);

        spec.setBoundSpec(this.boundSpec.copy());
        List<SubPartitionSpec> newSubPartitions = new ArrayList<>();
        for (int i = 0; i < this.subPartitions.size(); i++) {
            newSubPartitions.add(this.getSubPartitions().get(i).copy());
        }
        spec.setSubPartitions(newSubPartitions);

        return spec;

    }

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

    public void setEngine(String engineType) {
        this.engine = engineType;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
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

    public List<SubPartitionSpec> getSubPartitions() {
        return subPartitions;
    }

    public void setSubPartitions(List<SubPartitionSpec> subPartitions) {
        this.subPartitions = subPartitions;
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

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public String getLocality() {
        if(locality == null){
            return "";
        }
        return locality;
    }

    public void setIsDefaultPartition(boolean isDefaultPartition) { this.isDefaultPartition = isDefaultPartition; }

    public boolean getIsDefaultPartition() { return this.isDefaultPartition; }

    public String normalizePartSpec(boolean usePartGroupNameAsPartName,
                                    String partGrpName,
                                    boolean needSortBoundValues,
                                    int prefixPartColCnt) {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION ");
        if (!usePartGroupNameAsPartName) {
            String partName = SqlIdentifier.surroundWithBacktick(this.name);;
            sb.append(partName);
        } else {
            if (partGrpName != null) {
                sb.append(partGrpName);
            }
        }
        switch (strategy) {
        case HASH:
        case KEY:
        case RANGE:
        case RANGE_COLUMNS:
            sb.append(" VALUES LESS THAN (");
            break;
        case LIST:
        case LIST_COLUMNS:
            sb.append(" VALUES IN (");
            break;
        }
        PartitionBoundSpec tmpBoundSpec = this.boundSpec;
        if (needSortBoundValues) {
            tmpBoundSpec = sortPartitionsAllValues();
        }
        sb.append(tmpBoundSpec.getPartitionBoundDescription(prefixPartColCnt));
        sb.append(")");
        sb.append(" ENGINE = ");
        sb.append(this.engine);
        if (TStringUtil.isNotEmpty(locality)) {
            sb.append(" LOCALITY=").append(TStringUtil.quoteString(locality));
        }
        return sb.toString();
    }

    protected PartitionBoundSpec sortPartitionsAllValues() {
        PartitionSpec newSpec = copy();
        if(this.isDefaultPartition) {
            return newSpec.getBoundSpec();
        } else {
            PartitionBoundSpec newSortedValsBndSpec =
                PartitionByDefinition.sortListPartitionsAllValues(this.boundSpaceComparator, newSpec.getBoundSpec());
            return newSortedValsBndSpec;
        }
    }

    @Override
    public int hashCode() {
        int hashCodeVal = id.intValue();
        hashCodeVal ^= parentId.intValue();
        hashCodeVal ^= position.intValue();
        hashCodeVal ^= name.toLowerCase().hashCode();
        hashCodeVal ^= engine.toLowerCase().hashCode();
        hashCodeVal ^= status.intValue();
        hashCodeVal ^= version.intValue();
        hashCodeVal ^= strategy.hashCode();
        hashCodeVal ^= boundSpec.hashCode();
        if (location != null) {
            hashCodeVal ^= location.hashCode();
        }
        hashCodeVal ^= Objects.hashCode(locality);
        if (!subPartitions.isEmpty()) {
            for (int i = 0; i < subPartitions.size(); i++) {
                hashCodeVal ^= subPartitions.get(i).hashCode();
            }
        }
        return hashCodeVal;

    }

//    @Override
//    public boolean equals(Object obj) {
//        if (this == obj) {
//            return true;
//        }
//        if (obj != null && boundSpec != null && obj.getClass() == this.getClass()) {
//            return strategy == ((PartitionSpec) obj).getStrategy() &&
//                boundSpec.equals(((PartitionSpec) obj).getBoundSpec()) &&
//                Objects.equals(this.locality, ((PartitionSpec) obj).locality) &&
//                name.equalsIgnoreCase(((PartitionSpec) obj).name);
//        }
//        return false;
//    }

    @Override
    public boolean equals(Object obj) {
        return equals(obj, -1);
    }

    public boolean equals(Object obj, int prefixPartColCnt ) {
        if (this == obj) {
            return true;
        }

        if (obj != null && boundSpec != null && obj.getClass() == this.getClass()) {
            if (strategy == ((PartitionSpec) obj).getStrategy()
                && LocalityInfoUtils.equals(this.locality, ((PartitionSpec) obj).locality)
                && name.equalsIgnoreCase(((PartitionSpec) obj).name) ) {
                if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT ) {
                    return boundSpec.equals(((PartitionSpec) obj).getBoundSpec());
                } else {
                    if (strategy == PartitionStrategy.HASH) {
                        /**
                         * Both single-part-columns hash or mulit-part-columns hash, their bound value col has only one,
                         * and not support prefix partition column comparing, so must use full-part-col equal method
                         */
                        return boundSpec.equals(((PartitionSpec) obj).getBoundSpec());
                    } else {
                        return boundSpec.equals(((PartitionSpec) obj).getBoundSpec(), prefixPartColCnt);
                    }

                }
            }
        }
        return false;

    }

    @Override
    public String toString() {
        return normalizePartSpec(false, null, false, PartitionInfoUtil.FULL_PART_COL_COUNT);
    }

    public SearchDatumComparator getBoundSpaceComparator() {
        return boundSpaceComparator;
    }

    public void setBoundSpaceComparator(SearchDatumComparator boundSpaceComparator) {
        this.boundSpaceComparator = boundSpaceComparator;
    }

    public String getDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append(" partitionSpec:[");
        sb.append(this.normalizePartSpec(false, "", false, -1));
        sb.append(",");
        sb.append(this.getLocation().getDigest());
        sb.append("]");
        return sb.toString();
    }

    public Long getIntraGroupConnKey() {
        return intraGroupConnKey;
    }

    public void setIntraGroupConnKey(Long intraGroupConnKey) {
        this.intraGroupConnKey = intraGroupConnKey;
    }
}
