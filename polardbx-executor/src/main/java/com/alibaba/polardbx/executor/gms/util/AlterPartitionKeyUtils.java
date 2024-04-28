package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.config.ConfigDataMode;

import com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator;
import com.alibaba.polardbx.executor.sync.RepartitionSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.AsyncDDLContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;
import static com.alibaba.polardbx.common.ddl.Attribute.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_DDL_JOB_UNSUPPORTED;
import static com.alibaba.polardbx.executor.ddl.job.meta.misc.RepartitionMetaChanger.doCutOver;
import static com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator.getExpectedPrimaryAndShardingKeys;
import static com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableType.BROADCAST;
import static com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableType.SHARDING;
import static com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableType.SINGLE;

/**
 * @author guxu
 */
public class AlterPartitionKeyUtils {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    private static final int CUT_OVER_FLAG = 0x1;
    private static final int FLAG_AUTO_PARTITION = 0x2;

    /**
     * 带GSI的拆分表进行拆分键变更时，GSI可能并不包含主表的变更后的拆分键，所以这部分GSI需要重建。
     * https://yuque.antfin.com/coronadb/design/onmpll
     */
    private static List<Pair<String, SqlIndexDefinition>> initIndexInfoForRebuildingGsi(
        SqlCreateTable primaryTableNode,
        SqlAlterTablePartitionKey ast,
        String primaryTableDefinition) {

        String schemaName = ast.getOriginTableName().getComponent(0).getLastName();
        String sourceTableName = ast.getOriginTableName().getComponent(1).getLastName();

        List<Pair<String, SqlIndexDefinition>> result = new ArrayList<>();

        List<org.apache.calcite.util.Pair<SqlIdentifier, SqlIndexDefinition>> globalKeys =
            primaryTableNode.getGlobalKeys();
        if (CollectionUtils.isEmpty(globalKeys)) {
            return result;
        }

        Map<String, SqlIndexDefinition> gsiMap = new HashMap<>();
        for (org.apache.calcite.util.Pair<SqlIdentifier, SqlIndexDefinition> e : globalKeys) {
            gsiMap.put(e.getKey().getLastName(), e.getValue());
        }
        //拆分变更后，GSI表必须包含的列
        final Set<String> expectedPkSkList = getExpectedPrimaryAndShardingKeys(
            schemaName,
            sourceTableName,
            ast.isSingle(),
            ast.isBroadcast(),
            ast.getDbPartitionBy(),
            ast.getTablePartitionBy()
        );

        for (Map.Entry<String, SqlIndexDefinition> entry : gsiMap.entrySet()) {
            final String gsiName = entry.getKey();
            final SqlIndexDefinition gsiDefinition = entry.getValue();

            final Set<String> gsiAllColumns = new HashSet<>();
            gsiAllColumns.addAll(
                gsiDefinition.getColumns().stream().map(e -> e.getColumnNameStr().toLowerCase())
                    .collect(Collectors.toSet())
            );
            if (gsiDefinition.getCovering() != null) {
                gsiAllColumns.addAll(
                    gsiDefinition.getCovering().stream().map(e -> e.getColumnNameStr().toLowerCase())
                        .collect(Collectors.toSet())
                );
            }

            final List<SqlIndexColumnName> finalCoveringColumns = new ArrayList<>();
            if (gsiDefinition.getCovering() != null) {
                finalCoveringColumns.addAll(gsiDefinition.getCovering());
            }

            //如果发现GSI缺少列，则将该列加入重新后GSI的covering列
            if (!CollectionUtils.isSubCollection(expectedPkSkList, gsiAllColumns)) {
                final Collection<String> missingColumnNames = CollectionUtils.subtract(expectedPkSkList, gsiAllColumns);
                List<SqlIndexColumnName> missingColumns = missingColumnNames.stream()
                    .map(e -> new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(e, SqlParserPos.ZERO), null,
                        null))
                    .collect(Collectors.toList());
                finalCoveringColumns.addAll(missingColumns);
            } else {
                //如果发现GSI没有缺少列，则不重建它
                continue;
            }

            final String randomSuffix =
                RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME).toLowerCase();
            final String rebuildGsiName = gsiName + "_" + randomSuffix;
            SqlIndexDefinition indexDef = SqlIndexDefinition.globalIndex(SqlParserPos.ZERO,
                false,
                null,
                null,
                null,
                new SqlIdentifier(rebuildGsiName, SqlParserPos.ZERO),
                (SqlIdentifier) primaryTableNode.getTargetTable(),
                gsiDefinition.getColumns(),
                finalCoveringColumns,
                gsiDefinition.getDbPartitionBy(),
                gsiDefinition.getTbPartitionBy(),
                gsiDefinition.getTbPartitions(),
                gsiDefinition.getPartitioning(),
                new LinkedList<>(),
                gsiDefinition.getTableGroupName(),
                gsiDefinition.isWithImplicitTableGroup(),
                true);
            indexDef.setPrimaryTableNode(primaryTableNode);
            indexDef.setPrimaryTableDefinition(primaryTableDefinition);
            result.add(Pair.of(gsiName, indexDef));
        }

        return result;
    }

}
