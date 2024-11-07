package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;

/**
 * @author yuehan.wcf
 */
public class SerializeRangeScanClient extends TableScanClient {

    public SerializeRangeScanClient(ExecutionContext context, CursorMeta meta,
                                    boolean useTransaction) {
        super(context, meta, useTransaction, 1, RangeScanMode.SERIALIZE);
    }
}
