package com.alibaba.polardbx.executor.physicalbackfill;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PhysicalBackfillExecutor {

    public PhysicalBackfillExecutor() {
    }

    public int backfill(String schemaName,
                        String tableName,
                        Map<String, Set<String>> sourcePhyTables,
                        Map<String, Set<String>> targetPhyTables,
                        Map<String, String> sourceTargetGroup,
                        boolean isBroadcast,
                        ExecutionContext ec) {
        final long batchSize = ec.getParamManager().getLong(ConnectionParams.PHYSICAL_BACKFILL_BATCH_SIZE);
        final long minUpdateBatch =
            ec.getParamManager().getLong(ConnectionParams.PHYSICAL_BACKFILL_MIN_SUCCESS_BATCH_UPDATE);
        final long parallelism = ec.getParamManager().getLong(ConnectionParams.PHYSICAL_BACKFILL_PARALLELISM);

        if (null == ec.getServerVariables()) {
            ec.setServerVariables(new HashMap<>());
        }
        PhysicalBackfillExtractor extractor =
            new PhysicalBackfillExtractor(schemaName, tableName, sourcePhyTables, targetPhyTables, sourceTargetGroup,
                isBroadcast,
                batchSize,
                parallelism,
                minUpdateBatch);
        physicalBackfillLoader loader = new physicalBackfillLoader(schemaName, tableName);
        extractor.doExtract(ec, new BatchConsumer() {
            @Override
            public void consume(Pair<String, String> targetDbAndGroup,
                                Pair<String, String> targetFileAndDir,
                                List<Pair<String, Integer>> targetHosts,
                                Pair<String, String> userInfo,
                                PolarxPhysicalBackfill.TransferFileDataOperator transferFileData) {
                loader.applyBatch(targetDbAndGroup, targetFileAndDir, targetHosts, userInfo, transferFileData, ec);
            }
        });
        return 0;
    }

}
