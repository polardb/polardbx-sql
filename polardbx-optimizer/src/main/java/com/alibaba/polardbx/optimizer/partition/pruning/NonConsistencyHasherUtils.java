package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;

public class NonConsistencyHasherUtils {
    public static long calcHashCode(SearchDatumInfo searchVal) {
        long hashCode = 0;
        for (int i = 0; i < searchVal.datumInfo.length; i++) {
            PartitionBoundVal bndVal = searchVal.datumInfo[i];
            hashCode = hashCode * 31 + bndVal.getValue().xxHashCode();
        }
        return hashCode;
    }
}
