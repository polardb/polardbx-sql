package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.FetchDeadlockInfoSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.DeadlockParser;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.GLOBAL_DEADLOCK;
import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.MDL_DEADLOCK;

/**
 * @author wuzhe
 */
public class LogicalShowGlobalDeadlocksHandler extends HandlerCommon {
    public LogicalShowGlobalDeadlocksHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final ArrayResultCursor result = new ArrayResultCursor("GLOBAL_DEADLOCKS");
        result.addColumn("TYPE", DataTypes.StringType);
        result.addColumn("LOG", DataTypes.StringType);
        result.initMeta();

        List<List<Map<String, Object>>> results;
        final String schemaName = executionContext.getSchemaName();
        if (ExecUtils.hasLeadership(schemaName)) {
            // If I am the leader, just get deadlock information from StorageInfoManager
            final String deadlockInfo = StorageInfoManager.getDeadlockInfo();
            final String mdlDeadlockInfo = StorageInfoManager.getMdlDeadlockInfo();
            results = new LinkedList<>();
            if (null != deadlockInfo && null != mdlDeadlockInfo) {
                results.add(ImmutableList.of(
                    ImmutableMap.of("TYPE", GLOBAL_DEADLOCK, "LOG", deadlockInfo),
                    ImmutableMap.of("TYPE", MDL_DEADLOCK, "LOG", mdlDeadlockInfo)));
            }
        } else {
            // Otherwise, get deadlock information from leader
            results = SyncManagerHelper.sync(new FetchDeadlockInfoSyncAction(schemaName), schemaName);
        }

        if (CollectionUtils.isNotEmpty(results)) {
            for (List<Map<String, Object>> deadlockInfo : results) {
                if (null == deadlockInfo) {
                    continue;
                }
                for (Map<String, Object> row : deadlockInfo) {
                    final String type = (String) row.get("TYPE");
                    String log = (String) row.get("LOG");
                    // Inject logical tables
                    final Map<String, String> physicalToLogical = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    log = DeadlockParser.injectLogicalTables(log, physicalToLogical);
                    // Check privilege
                    log = DeadlockParser.checkPrivilege(log, physicalToLogical.values(), executionContext);
                    result.addRow(new Object[] {type, log});
                }
            }
        }

        return result;
    }
}
