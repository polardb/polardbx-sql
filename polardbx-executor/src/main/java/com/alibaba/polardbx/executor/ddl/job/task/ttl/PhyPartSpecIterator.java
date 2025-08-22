package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.utils.PartitionMetaUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

public class PhyPartSpecIterator implements Iterator<PartitionMetaUtil.PartitionMetaRecord> {

    protected LinkedBlockingQueue<PartitionMetaUtil.PartitionMetaRecord> phyPartSpecMetas = new LinkedBlockingQueue<>();
    protected Iterator<PartitionMetaUtil.PartitionMetaRecord> iterator;
    protected Set<String> targetPhyPartNameSet = null;

    public PhyPartSpecIterator(PartitionInfo partInfo) {
        initIterator(partInfo, null);
    }

    public PhyPartSpecIterator(PartitionInfo partInfo, Set<String> targetPhyPartNameSet) {
        initIterator(partInfo, targetPhyPartNameSet);
        this.targetPhyPartNameSet = targetPhyPartNameSet;

    }

    protected void initIterator(PartitionInfo partInfo, Set<String> targetPhyPartNameSet) {
        String tblName = partInfo.getTableName();
        List<PartitionMetaUtil.PartitionMetaRecord> partitionMetaRecords =
            PartitionMetaUtil.handlePartitionsMeta(partInfo, "", tblName);
        Map<String, LinkedBlockingQueue<PartitionMetaUtil.PartitionMetaRecord>> tmpDnToPhyPartsMappings =
            new TreeMap<>();
        int allPhyPartCnt = partitionMetaRecords.size();

        /**
         * Group all the phy-parts by their dnId
         */
        int finalPhyPartCnt = 0;
        for (int i = 0; i < allPhyPartCnt; i++) {
            PartitionMetaUtil.PartitionMetaRecord record = partitionMetaRecords.get(i);
            String phyPartName = record.getSubPartName();
            if (StringUtils.isEmpty(phyPartName)) {
                phyPartName = record.getPartName();
            }
            if (targetPhyPartNameSet != null) {
                if (!targetPhyPartNameSet.contains(phyPartName)) {
                    continue;
                }
            }
            String dnId = record.rwDnId;
            LinkedBlockingQueue<PartitionMetaUtil.PartitionMetaRecord> recordList = tmpDnToPhyPartsMappings.get(dnId);
            if (recordList == null) {
                recordList = new LinkedBlockingQueue<>();
                tmpDnToPhyPartsMappings.put(dnId, recordList);
            }
            recordList.add(record);
            ++finalPhyPartCnt;
        }

        /**
         * Reorder all the phy-parts by zigzag their dnId
         */
        Set<String> dnIdList = tmpDnToPhyPartsMappings.keySet();
        int currPartIndex = 0;
        LinkedBlockingQueue<PartitionMetaUtil.PartitionMetaRecord> newPhyPartInfos = new LinkedBlockingQueue<>();
        while (currPartIndex < finalPhyPartCnt) {
            Iterator<String> dnIdItor = dnIdList.iterator();
            while (dnIdItor.hasNext()) {
                String tmpDnId = dnIdItor.next();
                LinkedBlockingQueue<PartitionMetaUtil.PartitionMetaRecord> phyPartInfosOfDn =
                    tmpDnToPhyPartsMappings.get(tmpDnId);

                if (phyPartInfosOfDn.isEmpty()) {
                    continue;
                }

                PartitionMetaUtil.PartitionMetaRecord phyPart = phyPartInfosOfDn.poll();
                newPhyPartInfos.add(phyPart);
                currPartIndex++;
            }
        }
        phyPartSpecMetas = newPhyPartInfos;
    }

    @Override
    public boolean hasNext() {
        return !phyPartSpecMetas.isEmpty();
    }

    @Override
    public PartitionMetaUtil.PartitionMetaRecord next() {
        PartitionMetaUtil.PartitionMetaRecord phyPart = phyPartSpecMetas.poll();
        return phyPart;
    }
}
