package com.alibaba.polardbx.optimizer.core.rel.util;

import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class TargetTableInfoOneTable {

    protected PartitionInfo partInfo;

    /**
     * The pruned partition count of first-level partition
     */
    protected int prunedFirstLevelPartCount = 0;
    /**
     * Label if all the first-level partitions are sorted,
     * all the partitions sorted with asc order
     */
    protected boolean allPartSorted = false;

    /**
     * The part cols of first-level partitions
     */
    protected List<String> partColList = new ArrayList<>();
    /**
     * Label if using subpart
     */
    protected boolean useSubPart = false;
    /**
     * Label if all the second-level subpartitions are sorted,
     * only for the case of prunedFirstLevelPartCount=1,
     * all the subpartitions sorted with asc order
     */
    protected boolean allSubPartSorted = false;

    /**
     * The subpart cols of first-level partitions
     */
    protected List<String> subpartColList = new ArrayList<>();

    /**
     * Label if all the pruned 1st-level partition contain only one subpartition
     */
    protected boolean allPrunedPartContainOnlyOneSubPart = false;

    public TargetTableInfoOneTable() {
    }

    public int getPrunedFirstLevelPartCount() {
        return prunedFirstLevelPartCount;
    }

    public void setPrunedFirstLevelPartCount(int prunedFirstLevelPartCount) {
        this.prunedFirstLevelPartCount = prunedFirstLevelPartCount;
    }

    public boolean isAllPartSorted() {
        return allPartSorted;
    }

    public void setAllPartSorted(boolean allPartSorted) {
        this.allPartSorted = allPartSorted;
    }

    public List<String> getPartColList() {
        return partColList;
    }

    public void setPartColList(List<String> partColList) {
        this.partColList = partColList;
    }

    public List<String> getSubpartColList() {
        return subpartColList;
    }

    public void setSubpartColList(List<String> subpartColList) {
        this.subpartColList = subpartColList;
    }

    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public boolean isUseSubPart() {
        return useSubPart;
    }

    public void setUseSubPart(boolean useSubPart) {
        this.useSubPart = useSubPart;
    }

    public boolean isAllSubPartSorted() {
        return allSubPartSorted;
    }

    public void setAllSubPartSorted(boolean allSubPartSorted) {
        this.allSubPartSorted = allSubPartSorted;
    }

    public boolean isAllPrunedPartContainOnlyOneSubPart() {
        return allPrunedPartContainOnlyOneSubPart;
    }

    public void setAllPrunedPartContainOnlyOneSubPart(boolean allPrunedPartContainOnlyOneSubPart) {
        this.allPrunedPartContainOnlyOneSubPart = allPrunedPartContainOnlyOneSubPart;
    }
}
