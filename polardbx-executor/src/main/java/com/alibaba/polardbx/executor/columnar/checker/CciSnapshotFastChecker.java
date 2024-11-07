package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

/**
 * @author yaozhili
 */
public class CciSnapshotFastChecker extends CciFastChecker {
    private static final Logger logger = LoggerFactory.getLogger(CciSnapshotFastChecker.class);

    private static final String CALCULATE_PRIMARY_HASH =
        "select check_sum_v2(*) as checksum from %s as of tso %s force index(primary)";

    private final long primaryTso;
    private final long columnarTso;

    public CciSnapshotFastChecker(String schemaName, String tableName, String indexName,
                                  long primaryTso, long columnarTso) {
        super(schemaName, tableName, indexName);
        this.primaryTso = primaryTso;
        this.columnarTso = columnarTso;
    }

    @Override
    protected Pair<Long, Long> getCheckTso() {
        return new Pair<>(primaryTso, columnarTso);
    }

    @Override
    protected String getPrimarySql(ExecutionContext baseEc, long tso) {
        StringBuilder sb = new StringBuilder();
        ICciChecker.setBasicHint(baseEc, sb);

        sb.append(" TRANSACTION_POLICY=TSO");
        String hint = String.format(PRIMARY_HINT, sb);
        return hint + String.format(CALCULATE_PRIMARY_HASH, tableName, tso);
    }
}
