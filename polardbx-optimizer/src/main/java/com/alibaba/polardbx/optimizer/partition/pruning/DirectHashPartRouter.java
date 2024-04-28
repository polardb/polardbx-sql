package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.google.common.base.Preconditions;

public class DirectHashPartRouter extends PartitionRouter {
    String DIGEST_TEMPLATE = "non consistency hash strategy, partition number: %s";

    protected int partitionCount = 0;

    private boolean isPowerOfTwo;

    protected String routerDigest;

    public DirectHashPartRouter(int partitionCount) {
        Preconditions.checkArgument(partitionCount > 0, "partition count should be greater than zero");
        this.partitionCount = partitionCount;
        // FIXME fix digest
        this.routerDigest = String.format(DIGEST_TEMPLATE, partitionCount);
        this.isPowerOfTwo = MathUtils.isPowerOfTwo(partitionCount);
    }

    @Override
    public RouterResult routePartitions(ExecutionContext ec, ComparisonKind comp, Object searchVal) {
        RouterResult rs = new RouterResult();
        if (comp == ComparisonKind.EQUAL) {
            // Convert the searchVal from field space to hash space
            long hashVal = NonConsistencyHasherUtils.calcHashCode((SearchDatumInfo) searchVal);
            int partition = isPowerOfTwo ? (int) (hashVal & (partitionCount - 1))
                : (int) ((hashVal & Long.MAX_VALUE) % partitionCount);
            rs.partStartPosi = partition + 1;
            rs.pasrEndPosi = partition + 1;
        } else {
            /**
             * Here just use ComparisonKind.NOT_EQUAL to generate full scan RouterResult
             */
            rs.partStartPosi = 1;
            rs.pasrEndPosi = partitionCount;
        }
        rs.strategy = PartitionStrategy.DIRECT_HASH;
        return rs;
    }

    @Override
    public int getPartitionCount() {
        return this.partitionCount;
    }

    @Override
    public String getDigest() {
        return routerDigest;
    }
}
