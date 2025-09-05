package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.columnar.FlashbackColumnarManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCheckColumnarSnapshot;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalCheckColumnarSnapshotHandler extends HandlerCommon {

    public LogicalCheckColumnarSnapshotHandler(IRepository repository) {
        super(repository);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalDal dal = (LogicalDal) logicalPlan;
        final SqlCheckColumnarSnapshot checkColumnarSnapshot = (SqlCheckColumnarSnapshot) dal.getNativeSqlNode();

        SqlNode tableNameNode = checkColumnarSnapshot.getTableName();
        String schemaName, tableName;

        if (tableNameNode instanceof SqlIdentifier && ((SqlIdentifier) tableNameNode).names.size() == 2) {
            schemaName = ((SqlIdentifier) tableNameNode).names.get(0);
            tableName = ((SqlIdentifier) tableNameNode).names.get(1);
        } else {
            schemaName = PlannerContext.getPlannerContext(logicalPlan).getSchemaName();
            tableName = tableNameNode.toString();
        }

        ArrayResultCursor result = new ArrayResultCursor("checkColumnarSnapshot");
        result.addColumn("Columnar_Index_Name", DataTypes.StringType);
        result.addColumn("Partition_Name", DataTypes.StringType);
        result.addColumn("Result", DataTypes.StringType);
        result.addColumn("Diff", DataTypes.StringType);
        result.addColumn("Advice", DataTypes.StringType);

        TableMeta tableMeta = Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getLatestSchemaManager()
            .getTable(tableName);

        if (GeneralUtil.isEmpty(tableMeta.getColumnarIndexPublished())) {
            return result;
        }

        Set<String> columnarNames = tableMeta.getColumnarIndexPublished().keySet();

        ColumnarManager cm = ColumnarManager.getInstance();
        long tso = cm.latestTso();
        boolean autoPosition =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_SNAPSHOT_AUTO_POSITION);

        for (String columnarName : columnarNames) {
            FlashbackColumnarManager fcm = new FlashbackColumnarManager(tso, schemaName, columnarName, autoPosition);
            Map<String, Pair<List<String>, List<Pair<String, Long>>>> snapshotInfo = fcm.getSnapshotInfo();

            for (Map.Entry<String, Pair<List<String>, List<Pair<String, Long>>>> entry : snapshotInfo.entrySet()) {
                boolean hasDiff = false;
                StringBuilder diff = new StringBuilder();
                String partName = entry.getKey();
                Pair<List<String>, List<Pair<String, Long>>> expectedSnapshot = entry.getValue();
                Pair<List<String>, List<String>> columnarSnapshot =
                    cm.findFileNames(tso, schemaName, columnarName, partName);

                // compare orc:
                Set<String> expectedOrc = new HashSet<>(expectedSnapshot.getKey());
                Set<String> actualOrc = new HashSet<>(columnarSnapshot.getKey());

                Set<String> expectedButNotActualOrc = new HashSet<>(expectedOrc);
                expectedButNotActualOrc.removeAll(actualOrc);
                if (!expectedButNotActualOrc.isEmpty()) {
                    hasDiff = true;
                    diff.append("expected but not present orc: ").append(expectedButNotActualOrc);
                }
                Set<String> actualButNotExpectedOrc = new HashSet<>(actualOrc);
                actualButNotExpectedOrc.removeAll(expectedOrc);
                if (!actualButNotExpectedOrc.isEmpty()) {
                    if (hasDiff) {
                        diff.append(", ");
                    }
                    hasDiff = true;
                    diff.append("present but not expected orc: ").append(actualButNotExpectedOrc);
                }

                // compare csv:
                Set<String> expectedCsv =
                    expectedSnapshot.getValue().stream().map(Pair::getKey).collect(Collectors.toSet());
                Set<String> actualCsv = new HashSet<>(columnarSnapshot.getValue());

                Set<String> expectedButNotActualCsv = new HashSet<>(expectedCsv);
                expectedButNotActualCsv.removeAll(actualCsv);
                if (!expectedButNotActualCsv.isEmpty()) {
                    if (hasDiff) {
                        diff.append(", ");
                    }
                    hasDiff = true;
                    diff.append("expected but not present csv: ").append(expectedButNotActualCsv);
                }
                Set<String> actualButNotExpectedCsv = new HashSet<>(actualCsv);
                actualButNotExpectedCsv.removeAll(expectedCsv);
                if (!actualButNotExpectedCsv.isEmpty()) {
                    if (hasDiff) {
                        diff.append(", ");
                    }
                    hasDiff = true;
                    diff.append("present but not expected csv: ").append(actualButNotExpectedCsv);
                }
                result.addRow(new Object[] {
                    columnarName, partName, hasDiff ? "Found inconsistent snapshot" : "OK", diff.toString(),
                    hasDiff ? "RELOAD COLUMNARMANAGER SNAPSHOT" : "NONE"});
            }
        }
        return result;
    }
}
