package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.utils.PartitionMetaUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.BitSet;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class TtlJobPhyPartBitSetUtil {

    public static void markOnePartAsFinished(Set<Integer> phyParSet,
                                             PartitionMetaUtil.PartitionMetaRecord partMetaRec) {
        int nPartPos = partMetaRec.getPartNum().intValue();
        phyParSet.add(nPartPos);
    }

    public static BitSet markOnePartAsFinished(BitSet phyPartBitSet, int nPhyPartPos) {
        phyPartBitSet.set(nPhyPartPos, true);
        return phyPartBitSet;
    }

    public static BitSet markOnePartAsUnFinished(BitSet phyPartBitSet, int nPhyPartPos) {
        phyPartBitSet.set(nPhyPartPos, false);
        return phyPartBitSet;
    }

    public static String bitSetToJsonString(BitSet phyPartBitset) {
        ObjectMapper mapper = new ObjectMapper();
        String bitsetJson = null;
        try {
            bitsetJson = mapper.writeValueAsString(phyPartBitset);
        } catch (Exception ex) {
            throw new TtlJobRuntimeException(ex, ex.getMessage());
        }
        return bitsetJson;
    }

    public static BitSet jsonStringToBitSet(String bitSetJson) {
        ObjectMapper mapper = new ObjectMapper();
        BitSet deserializedBitSet = null;
        try {
            deserializedBitSet = mapper.readValue(bitSetJson, BitSet.class);
        } catch (Exception ex) {
            throw new TtlJobRuntimeException(ex, ex.getMessage());
        }
        return deserializedBitSet;
    }

    public static String createPartMartBitSetStr(PartitionInfo partInfo) {
        BitSet newPhyPartBitSet = PartitionPrunerUtils.buildEmptyPhysicalPartitionsBitSet(partInfo);
        String bitSetJson = bitSetToJsonString(newPhyPartBitSet);
        return bitSetJson;
    }

    public static boolean checkIfFinishCleaningExpiredDataForOnePart(String phyPartBitSetStr,
                                                                     PartitionMetaUtil.PartitionMetaRecord partMetaRec) {
        int nPartPos = partMetaRec.getPartNum().intValue();
        BitSet phyPartBitSet = jsonStringToBitSet(phyPartBitSetStr);
        boolean checkRs = phyPartBitSet.get(nPartPos);
        return checkRs;
    }

    public static BitSet markOnePartAsUnFinished(String phyPartBitSetStr,
                                                 PartitionMetaUtil.PartitionMetaRecord partMetaRec) {
        int nPartPos = partMetaRec.getPartNum().intValue();
        BitSet phyPartBitSet = jsonStringToBitSet(phyPartBitSetStr);
        return markOnePartAsUnFinished(phyPartBitSet, nPartPos);
    }

    public static int fetchPhyPartBitSetPosition(PartitionMetaUtil.PartitionMetaRecord partMetaRec) {
        return partMetaRec.getPartNum().intValue();
    }

}
