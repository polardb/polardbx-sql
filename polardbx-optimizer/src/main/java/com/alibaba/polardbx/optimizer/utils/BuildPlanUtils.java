/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.jdbc.ZeroTime;
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRoutingContext;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.rule.Partitioner;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BuildPlanUtils {

    protected static final Logger LOGGER = LoggerFactory.getLogger("DDL_META_LOG");

    public static SqlNode buildTargetTable() {
        return new SqlDynamicParam(PlannerUtils.TABLE_NAME_PARAM_INDEX, SqlParserPos.ZERO);
    }

    /**
     * Copy values for physical table of broadcast
     *
     * @return targetDb: { targetTb: [{ rowIndex, [pk1, pk2] }] }
     */
    public static Map<String, Map<String, List<Pair<Integer, List<Object>>>>> buildResultForBroadcastTable(
        String schemaName,
        String logicalTableName,
        List<List<Object>> values,
        Mapping pkMapping,
        ExecutionContext executionContext) {
        PartitionInfoManager partitionInfoManager =
            executionContext.getSchemaManager(schemaName).getTddlRuleManager().getPartitionInfoManager();
        PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(logicalTableName);
        final String physicalTableName;
        if (partitionInfo != null) {
            physicalTableName = partitionInfo.getTopology().values().stream().findFirst().get().iterator().next();
        } else {
            OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
            TddlRuleManager or = optimizerContext.getRuleManager();
            TableRule tr = or.getTableRule(logicalTableName);
            physicalTableName = tr.getTbNamePattern();
        }

        List<String> groupNames = HintUtil.allGroup(schemaName);
        // May use jingwei to sync broadcast table
        Map<String, Object> extraCmd = executionContext.getExtraCmds();
        boolean enableBroadcast = extraCmd == null
            || GeneralUtil.getPropertyBoolean(extraCmd, ConnectionProperties.CHOOSE_BROADCAST_WRITE,
            true);
        if (!enableBroadcast) {
            groupNames = groupNames.subList(0, 1);
        }

        List<Pair<Integer, List<Object>>> rowList = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            final List<Object> row = values.get(i);
            final List<Object> primaryKeyValues = Mappings.permute(row, pkMapping);
            rowList.add(Pair.of(i, primaryKeyValues));
        }

        // targetDb: { targetTb: [[pk1], [pk2]] }
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults = new HashMap<>();
        Map<String, List<Pair<Integer, List<Object>>>> tbMap = new HashMap<>();
        tbMap.put(physicalTableName, rowList);
        for (String dbIndex : groupNames) {
            shardResults.put(dbIndex, tbMap);
        }
        return shardResults;
    }

    /**
     * Separate values for physical table
     *
     * @return targetDb: { targetTb: [{ rowIndex, [pk1, pk2] }] }
     */
    public static Map<String, Map<String, List<Pair<Integer, List<Object>>>>> buildResultForShardingTable(
        String schemaName,
        String logicalTableName,
        List<List<Object>> values,
        List<ColumnMeta> skMetas,
        Mapping skMapping,
        Mapping pkMapping,
        ExecutionContext ec,
        boolean isGetShardResultForReplicationTable) {
        // targetDb: { targetTb: [{ rowIndex, [pk1, pk2] }] }
        final Map<String, Map<String, List<org.apache.calcite.util.Pair<Integer, List<Object>>>>> result =
            new HashMap<>();

        // Foreach row to be updated or deleted
        final List<String> skNames = skMetas.stream().map(ColumnMeta::getName).collect(Collectors.toList());
        for (int i = 0; i < values.size(); i++) {
            List<Object> row = values.get(i);
            // Shard
            List<Object> skValues = Mappings.permute(row, skMapping).stream().map(value -> {
                if (value instanceof ZeroDate || value instanceof ZeroTime || value instanceof ZeroTimestamp) {
                    // For date like "0000-00-00" partition result is different for ZeroDate and String.
                    // INSERT and SELECT use String data type, so UPDATE/DELETE here should keep same.
                    return value.toString();
                }
                return value;
            }).collect(Collectors.toList());

            Pair<String, String> dbAndTable = BuildPlanUtils
                .shardSingleRow(skValues, skMetas, skNames, logicalTableName, schemaName, ec,
                    isGetShardResultForReplicationTable, ec.getSchemaManager(schemaName).getTable(logicalTableName));

            // Add primary keys to the map
            List<Object> primaryKeyValues = Mappings.permute(row, pkMapping);
            result.computeIfAbsent(dbAndTable.getKey(), b -> new HashMap<>())
                .computeIfAbsent(dbAndTable.getValue(), b -> new ArrayList<>())
                .add(org.apache.calcite.util.Pair.of(i, primaryKeyValues));
        }

        return result;
    }

    public static Map<String, Map<String, List<Pair<Integer, List<Object>>>>> buildResultForShardingTable(
        String schemaName,
        String logicalTableName,
        List<List<Object>> values,
        List<String> shardingKeyNames,
        List<Integer> shardingKeyIndexes,
        List<ColumnMeta> shardingKeyMetas,
        List<Integer> primaryKeyIndexes,
        ExecutionContext ec) {

        boolean isPartTable = OptimizerContext.getContext(schemaName).getRuleManager().getPartitionInfoManager()
            .isNewPartDbTable(logicalTableName);
        if (isPartTable) {

            TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(logicalTableName);
            PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
            PartitionTupleRoutingContext routingContext = PartitionTupleRoutingContext
                .buildPartitionTupleRoutingContext(schemaName, logicalTableName, partitionInfo, shardingKeyMetas);

            SqlCall rowsAst = routingContext.createPartColDynamicParamAst();
            PartitionTupleRouteInfo tupleRouteInfo = buildPartitionTupleRouteInfo(routingContext, rowsAst, true, ec);

            // fast path
            return routeMultiValueRowWithResult(tupleRouteInfo, routingContext, ec, shardingKeyIndexes,
                primaryKeyIndexes, values, true);
        } else {
            // slow path
            return shardMultiRowsSlowPath(schemaName, logicalTableName, values, shardingKeyNames, shardingKeyIndexes,
                shardingKeyMetas, primaryKeyIndexes, ec);
        }

    }

    /**
     * Route rows one by one, as a slow path
     */
    private static Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardMultiRowsSlowPath(
        String schemaName,
        String logicalTableName,
        List<List<Object>> values,
        List<String> shardingKeyNames,
        List<Integer> shardingKeyIndexes,
        List<ColumnMeta> shardingKeyMetas,
        List<Integer> primaryKeyIndexes,
        ExecutionContext ec) {
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults = new HashMap<>();
        // for each row to be updated or deleted
        for (int i = 0; i < values.size(); i++) {
            List<Object> row = values.get(i);
            // shard
            List<Object> shardingKeyValues = shardingKeyIndexes.stream().map(idx -> {
                final Object value = row.get(idx);
                if (value instanceof ZeroDate || value instanceof ZeroTime || value instanceof ZeroTimestamp) {
                    // For date like "0000-00-00" partition result is different for ZeroDate and String.
                    // INSERT and SELECT use String data type, so UPDATE/DELETE here should keep same.
                    return value.toString();
                }
                return value;
            }).collect(Collectors.toList());
            Pair<String, String> dbAndTable = BuildPlanUtils.shardSingleRow(shardingKeyValues,
                shardingKeyMetas,
                shardingKeyNames,
                logicalTableName,
                schemaName,
                ec,
                false,
                ec.getSchemaManager(schemaName).getTable(logicalTableName));

            // add primary keys to the map
            List<Object> primaryKeyValues = primaryKeyIndexes.stream().map(row::get).collect(Collectors.toList());
            shardResults.computeIfAbsent(dbAndTable.getKey(), b -> new HashMap<>())
                .computeIfAbsent(dbAndTable.getValue(), b -> new ArrayList<>())
                .add(Pair.of(i, primaryKeyValues));
        }

        return shardResults;
    }

    public static Pair<String, String> shardSingleRow(List<Object> shardingKeyValues,
                                                      List<ColumnMeta> shardingKeyMetas,
                                                      List<String> shardingKeyNames,
                                                      String logicalTableName,
                                                      String schemaName,
                                                      ExecutionContext executionContext,
                                                      PartitionInfo partInfo) {

        boolean isPartTable =
            OptimizerContext.getContext(schemaName).getRuleManager().getPartitionInfoManager()
                .isNewPartDbTable(logicalTableName);
        if (!isPartTable) {
            // Do sharding for sharded table
            return doShardSingleRow(shardingKeyValues, shardingKeyMetas, shardingKeyNames, logicalTableName, schemaName,
                executionContext);
        } else {
            // Do routeing for partitioned table
            return routeSingleRow(shardingKeyValues, shardingKeyMetas, logicalTableName, schemaName, executionContext,
                partInfo);
        }
    }

    /**
     * Route a single tuple row by its values of partKeys
     * <p>
     * <pre>
     *     e.g. Assume (pk1,pk2) is a partitioned column of a target table (c1,pk1,c2,pk2,c3),
     *     now we want to do the tuple routing of (val1, val2, val3, val4, val5 ),
     *     then
     *
     *          The partKeyValues is (val2, val4);
     *          The partKeyMetas is (columnMeta of pk1, columnMeta of pk2).
     *
     * </pre>
     *
     * @param logTbName the logical table name
     * @param schemaName the schema name
     * @param context the execution context
     * @param partInfo the target partInfo, if it is null, the partInfo of schemaName.logTbName will be used as default
     */
    private static Pair<String, String> routeSingleRow(List<Object> partKeyValues,
                                                       List<ColumnMeta> partKeyMetas,
                                                       String logTbName,
                                                       String schemaName,
                                                       ExecutionContext context,
                                                       PartitionInfo partInfo) {
        PartitionTupleRoutingContext routingContext = PartitionTupleRoutingContext
            .buildPartitionTupleRoutingContext(schemaName, logTbName, partInfo, partKeyMetas);
        SqlCall rowsAst = routingContext.createPartColDynamicParamAst();
        PartitionTupleRouteInfo tupleRouteInfo = buildPartitionTupleRouteInfo(routingContext, rowsAst, true, context);
        Parameters valuesParams = routingContext.createPartColValueParameters(partKeyValues);
        return routeSingleValueRow(tupleRouteInfo, context, true, 0, valuesParams);
    }

    public static Pair<String, String> shardSingleRow(List<Object> shardingKeyValues,
                                                      List<ColumnMeta> shardingKeyMetas,
                                                      List<String> shardingKeyNames,
                                                      String logicalTableName,
                                                      String schemaName,
                                                      ExecutionContext executionContext,
                                                      boolean isGetShardResultForReplicationTable,
                                                      TableMeta tableMeta) {
        PartitionInfo partitionInfo;
        if (isGetShardResultForReplicationTable) {
            partitionInfo = tableMeta.getNewPartitionInfo();
            if (partitionInfo == null) {
                LOGGER.info("unexpected empty PartitionInfo");
                LOGGER.info(tableMeta.getComplexTaskTableMetaBean().getDigest());
                LOGGER.info(tableMeta.getPartitionInfo().getDigest(tableMeta.getVersion()));
            }
        } else {
            partitionInfo = tableMeta.getPartitionInfo();
        }
        return shardSingleRow(shardingKeyValues, shardingKeyMetas, shardingKeyNames,
            logicalTableName,
            schemaName, executionContext, partitionInfo);

    }

    private static Pair<String, String> doShardSingleRow(List<Object> shardingKeyValues,
                                                         List<ColumnMeta> shardingKeyMetas,
                                                         List<String> shardingKeyNames,
                                                         String logicalTableName,
                                                         String schemaName,
                                                         ExecutionContext executionContext) {

        // Column name comes from RelDataTypeField, but dataType comes from ColumnMeta
        Map<String, Comparative> comparatives = Partitioner.getComparatives(shardingKeyMetas,
            shardingKeyValues,
            shardingKeyNames);
        Partitioner partitioner = OptimizerContext.getContext(schemaName).getPartitioner();
        Map<String, Comparative> fullComparative = partitioner.getInsertFullComparative(comparatives);

        // construct calcParams
        Map<String, Object> calcParams = Maps.newHashMap();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        Map<String, Map<String, Comparative>> stringMapMap = Maps.newHashMap();
        stringMapMap.put(logicalTableName, fullComparative);
        calcParams.put(CalcParamsAttribute.COM_DB_TB, stringMapMap);
        calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, executionContext.getTimeZone());
        calcParams.put(CalcParamsAttribute.EXECUTION_CONTEXT, executionContext);
        List<TargetDB> targetDBS =
            executionContext.getSchemaManager(schemaName).getTddlRuleManager()
                .shard(logicalTableName, true, false, comparatives, Maps.newHashMap(), calcParams, executionContext);

        // get the dbIndex and tableName
        assert targetDBS.size() == 1;
        TargetDB targetDB = targetDBS.get(0);
        String dbIndex = targetDB.getDbIndex();
        assert targetDB.getTableNames().size() == 1;
        String tableName = (String) targetDB.getTableNames().toArray()[0];

        return Pair.of(dbIndex, tableName);
    }

    /**
     * Do sharding depending on the logical insert, and returned sharded
     * results.
     *
     * @param sqlInsert new logical insert
     * @param executionContext old logical parameters
     * @return { targetDb : { targetTb : [valueIndex1, valueIndex2] } }
     */
    public static Map<String, Map<String, List<Integer>>> shardValues(SqlInsert sqlInsert,
                                                                      TableMeta indexMeta,
                                                                      ExecutionContext executionContext,
                                                                      String schemaName,
                                                                      PartitionInfo newPartitionInfo) {

        String logTbName = indexMeta.getTableName();
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        boolean isPartitionedTable = partitionInfoManager.isNewPartDbTable(logTbName);
        if (!isPartitionedTable) {
            return shardValuesForShardedTable(sqlInsert, indexMeta, executionContext, schemaName);
        } else {
            return shardValuesForPartitionedTable(sqlInsert, indexMeta, newPartitionInfo, executionContext, schemaName);
        }
    }

    public static Map<String, Map<String, List<Integer>>> shardValuesByPartName(SqlInsert sqlInsert,
                                                                                TableMeta indexMeta,
                                                                                ExecutionContext executionContext,
                                                                                String schemaName,
                                                                                PartitionInfo newPartitionInfo, String partName) {
        String logTbName = indexMeta.getTableName();
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        boolean isPartitionedTable = partitionInfoManager.isNewPartDbTable(logTbName);
        if (!isPartitionedTable) {
            throw GeneralUtil.nestedException("Unsupported to shard key");
        } else {
            return shardValuesForPartitionedTableByPartName(
                sqlInsert, indexMeta, newPartitionInfo, executionContext, schemaName, partName);
        }
    }
    protected static Map<String, Map<String, List<Integer>>> shardValuesForPartitionedTableByPartName(SqlInsert sqlInsert,
                                                                                                      TableMeta tableMeta,
                                                                                                      PartitionInfo partInfo,
                                                                                                      ExecutionContext executionContext,
                                                                                                      String schemaName, String partName) {
        String logTbName = tableMeta.getTableName();
        List<ColumnMeta> rowColMeta = new ArrayList<>();
        SqlNodeList rowColsAst = sqlInsert.getTargetColumnList();
        for (int i = 0; i < rowColsAst.size(); i++) {
            SqlNode rowCol = rowColsAst.get(i);
            if (rowCol instanceof SqlIdentifier) {
                String colName = ((SqlIdentifier) rowCol).getLastName();
                ColumnMeta cm = tableMeta.getColumn(colName);
                rowColMeta.add(cm);
            }
        }
        List<Integer> partColIndexList;
        Parameters parameters = executionContext.getParams();
        if (partInfo == null) {
            partInfo = tableMeta.getPartitionInfo();
        }
        List<String> partitionNameList = ImmutableList.of(partName);
        PhysicalPartitionInfo loadTablePhysicalPartitionInfo =
            partInfo.getPhysicalPartitionTopology(partitionNameList)
                .values()
                .stream()
                .findFirst().get().get(0);
        final Pair<String, String> dbAndTable = Pair
            .of(loadTablePhysicalPartitionInfo.getGroupKey(), loadTablePhysicalPartitionInfo.getPhyTable());
        /**
         * isBatch:
         *   isBatch=false,
         *     e.g. convert "values (?,?,?)" to one tupleRouteFn
         *   isBatch=true
         *     e.g. convert "values (?,?+?,?)(?-?,?,?),..." to multi tupleRouteFn
         */
        PartitionTupleRoutingContext routingContext =
            PartitionTupleRoutingContext.buildPartitionTupleRoutingContext(schemaName, logTbName, partInfo, rowColMeta);
        partColIndexList = routingContext.getPartColIndexMappings();
        // Pick part columns Values from insert Values ast of SqlInsert，
        // each column of each value of multiValues is tha same as part column definitions
        List<List<Object>> multiValues = valuesForInsert(sqlInsert, partColIndexList, parameters, true);
        // Do Tuple Routing
        Map<String, Map<String, List<Integer>>> shardResults = new HashMap<>();
        for (int i = 0; i < multiValues.size(); i++) {
            shardResults.computeIfAbsent(dbAndTable.getKey(), b -> new HashMap<>())
                .computeIfAbsent(dbAndTable.getValue(), b -> new ArrayList<>())
                .add(i);
        }
        return shardResults;
    }

    /**
     * Do sharding depending on the logical insert, and returned sharded
     * results.
     *
     * @param sqlInsert new logical insert
     * @param executionContext old logical parameters
     * @return { targetDb : { targetTb : [valueIndex1, valueIndex2] } }
     */
    protected static Map<String, Map<String, List<Integer>>> shardValuesForShardedTable(SqlInsert sqlInsert,
                                                                                        TableMeta indexMeta,
                                                                                        ExecutionContext executionContext,
                                                                                        String schemaName) {
        Parameters parameters = executionContext.getParams();
        List<String> shardingKeyNames = GsiUtils.getShardingKeys(indexMeta, schemaName);
        List<ColumnMeta> shardingKeyMetas = shardingKeyNames.stream()
            .map(indexMeta::getColumnIgnoreCase)
            .collect(Collectors.toList());
        List<String> relShardingKeyNames = BuildPlanUtils.getRelColumnNames(indexMeta, shardingKeyMetas);

        Map<String, Map<String, List<Integer>>> shardResults = new HashMap<>();
        List<List<Object>> shardingKeyValueList =
            pickValuesFromInsert(sqlInsert, shardingKeyNames, parameters, true, new ArrayList<>());
        for (int i = 0; i < shardingKeyValueList.size(); i++) {
            Pair<String, String> dbAndTable = shardSingleRow(shardingKeyValueList.get(i),
                shardingKeyMetas,
                relShardingKeyNames,
                indexMeta.getTableName(),
                schemaName,
                executionContext,
                false,
                indexMeta);
            shardResults.computeIfAbsent(dbAndTable.getKey(), b -> new HashMap<>())
                .computeIfAbsent(dbAndTable.getValue(), b -> new ArrayList<>())
                .add(i);
        }
        return shardResults;
    }

    /**
     * Build the tuple routing info by one or multi tuple templates
     * <p>
     * <pre>
     *
     * For isBatch=true, the rowValuesAst will has only one tuple template, so
     * this method will generate one tupleRouteFn for one template,
     *      e.g. convert "values (?,?,?)" to one tupleRouteFn
     * .
     *
     * Fort isBatch=false, the rowValuesAst will has multi tuple templates, so
     * this method will generate multi tupleRouteFn for each templates,
     *      e.g. convert "values (?,?+?,?)(?-?,?,?),..." to one (tupleRouteFn1, tupleRouteFn2)
     * , then if use the PartitionTupleRouteInfo return to do routing by calling routeSingleValueRow,
     * it need specify the tuple template idx of tupleTemplateIdx
     *
     * </pre>
     *
     * @param routingContext the context of tupling
     * @param rowValuesAst the tuple templates
     * @param isBatch flag that label if rowValuesAst is a batch sql
     */
    public static PartitionTupleRouteInfo buildPartitionTupleRouteInfo(
        PartitionTupleRoutingContext routingContext,
        SqlNode rowValuesAst,
        boolean isBatch,
        ExecutionContext ec) {

        String schemaName = routingContext.getSchemaName();
        String logTbName = routingContext.getTableName();
        PartitionInfo partInfo = routingContext.getPartInfo();
        if (partInfo == null) {
            TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(logTbName);
            partInfo = tableMeta.getPartitionInfo();
        }

        RelDataType valueRowType = routingContext.getPartColRelRowType();
        List<Integer> pickedColumnIdxList = routingContext.getPartColIndexMappings();

        // Fetch the tuple template from SqlInsert Values according to the value of parameters.isBatch()
        List<SqlNode> rows = ((SqlCall) rowValuesAst).getOperandList();
        List<List<SqlNode>> tupleTemplates = new ArrayList<>();
        if (isBatch) {
            /**
             * <pre>
             * When isBatch=true, all ast values has only one tupleTumlates,
             * such as :
             *
             * assume (b,c) are part_col, and
             * insert into tbl (a,b,c) values (?,?+?,?)
             *
             * Tuple Tumplates of partition cols:
             *  (?+?,?)
             *
             * Params:
             *  (3+1,5)
             *  (7+1,5)
             *  (1+2,4)
             *  ...
             * ,
             *  so all row are the same tuple template.
             *
             * </pre>
             */
            List<SqlNode> nodesInRow = ((SqlCall) rows.get(0)).getOperandList();
            List<SqlNode> nodesOfPickedColumn = new ArrayList<>();
            for (int i = 0; i < pickedColumnIdxList.size(); i++) {
                nodesOfPickedColumn.add(nodesInRow.get(pickedColumnIdxList.get(i)));
            }
            tupleTemplates.add(nodesOfPickedColumn);
        } else {
            /**
             * <pre>
             * When isBatch=false, all ast values may contain multi tuple templates, such as:
             *
             * assume (b,c) are part_col, and
             * insert into tbl (a,b,c)
             *  values (2,3,4),(6,7+1,8),(6+1+1,7-4,8+5)
             *
             * Tuple Tumplates of partition columns:
             * 0:(?,?)
             * 1:(?+?,?)
             * 2:(?-?,?+？)
             * ,
             * so each row is a new tuple template
             * </pre>
             */
            for (SqlNode row : rows) {
                List<SqlNode> nodesInRow = ((SqlCall) row).getOperandList();
                List<SqlNode> nodesOfPickedColumn = new ArrayList<>();
                for (int i = 0; i < pickedColumnIdxList.size(); i++) {
                    nodesOfPickedColumn.add(nodesInRow.get(pickedColumnIdxList.get(i)));
                }
                tupleTemplates.add(nodesOfPickedColumn);
            }
        }

        PartitionTupleRouteInfo tupleRouteInfo = PartitionPruner
            .generatePartitionTupleRoutingInfo(schemaName, logTbName, partInfo, valueRowType, tupleTemplates, ec);

        return tupleRouteInfo;
    }

    /**
     * Route multi values, and return row index and input values
     *
     * @return return topology with input values
     */
    protected static Map<String, Map<String, List<Pair<Integer, List<Object>>>>> routeMultiValueRowWithResult(
        PartitionTupleRouteInfo tupleRouteInfo,
        PartitionTupleRoutingContext routingContext,
        ExecutionContext executionContext,
        List<Integer> shardingKeyIndexes,
        List<Integer> primaryKeyIndices,
        List<List<Object>> multiValues,
        boolean isBatch) {

        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults = new HashMap<>();

        // Ust tmp ExecutionContext to dynamic update the info of params for part pruning
        ExecutionContext tmpEc = executionContext.copy();

        int tupleTemplateIdx = 0;
        for (int i = 0; i < multiValues.size(); i++) {
            if (!isBatch) {
                tupleTemplateIdx = i;
            }

            List<Object> singleValue = multiValues.get(i);
            List<Object> shardingKeyValues = shardingKeyIndexes.stream().map(idx -> {
                final Object value = singleValue.get(idx);
                if (value instanceof ZeroDate || value instanceof ZeroTime || value instanceof ZeroTimestamp) {
                    // For date like "0000-00-00" partition result is different for ZeroDate and String.
                    // INSERT and SELECT use String data type, so UPDATE/DELETE here should keep same.
                    return value.toString();
                }
                return value;
            }).collect(Collectors.toList());
            Parameters singleValParams = routingContext.createPartColValueParameters(shardingKeyValues);

            Pair<String, String> dbAndTable =
                routeSingleValueRow(tupleRouteInfo, tmpEc, false, tupleTemplateIdx, singleValParams);

            // add primary keys to the map
            List<Object> primaryKeyValues =
                primaryKeyIndices.stream().map(singleValue::get).collect(Collectors.toList());
            shardResults.computeIfAbsent(dbAndTable.getKey(), b -> new HashMap<>())
                .computeIfAbsent(dbAndTable.getValue(), b -> new ArrayList<>())
                .add(Pair.of(i, primaryKeyValues));
        }
        return shardResults;
    }

    /**
     * route a single row value by tupleRouteInfo and singleValueParams
     */
    protected static Pair<String, String> routeSingleValueRow(
        PartitionTupleRouteInfo tupleRouteInfo,
        ExecutionContext ec,
        boolean useTmpCtx,
        int tupleTemplateIdx,
        Parameters singleValueParams) {

        List<PhysicalPartitionInfo> prunedParts =
            resetParamsAndDoPruning(tupleRouteInfo, ec, useTmpCtx, tupleTemplateIdx, singleValueParams);

        assert prunedParts.size() == 1;
        String grpKey = prunedParts.get(0).getGroupKey();
        String phyTbl = prunedParts.get(0).getPhyTable();
        Pair<String, String> dbAndTable = new Pair<>(grpKey, phyTbl);
        return dbAndTable;
    }

    public static List<PhysicalPartitionInfo> resetParamsAndDoPruning(PartitionTupleRouteInfo tupleRouteInfo,
                                                                      ExecutionContext ec,
                                                                      boolean useTmpCtx,
                                                                      int tupleTemplateIdx,
                                                                      Parameters singleValueParams) {
        ExecutionContext tmpEc;
        if (useTmpCtx) {
            tmpEc = ec.copy();
        } else {
            tmpEc = ec;
        }
        tmpEc.setParams(singleValueParams);

        // Execute tuple routing and fetch route result.
        PartPrunedResult routeResult =
            PartitionPruner.doPruningByTupleRouteInfo(tupleRouteInfo, tupleTemplateIdx, tmpEc);
        if (routeResult.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NO_FOUND,
                "GSI table or temp table has no partition corresponding the value from primary table");
        }
        List<PhysicalPartitionInfo> prunedParts = routeResult.getPrunedPartitions();
        return prunedParts;
    }

    public static SearchDatumInfo resetParamsAndDoCalcSearchDatum(PartitionTupleRouteInfo tupleRouteInfo,
                                                                  ExecutionContext ec,
                                                                  boolean useTmpCtx,
                                                                  int tupleTemplateIdx,
                                                                  Parameters singleValueParams) {
        ExecutionContext tmpEc;
        if (useTmpCtx) {
            tmpEc = ec.copy();
        } else {
            tmpEc = ec;
        }
        tmpEc.setParams(singleValueParams);
        // calc hash code for a tuple
        return PartitionPruner.doCalcSearchDatumByTupleRouteInfo(tupleRouteInfo, tupleTemplateIdx, tmpEc);
    }

    /**
     * Do sharding depending on the logical insert ast for partitioned table, and returned sharded
     * results.
     *
     * @param sqlInsert the insertAst (its values maybe SqlDynamicParams or SqlLiteral) to be insert
     * @param tableMeta the meta of the insertAst, provided logTableName and the RelRowType of SqlInsert
     * @param executionContext the exec context, which will contains all the dynamic params
     * @param schemaName the target schema Name
     * @return { targetDb : { targetTb : [valueIndex1, valueIndex2] } }
     */
    protected static Map<String, Map<String, List<Integer>>> shardValuesForPartitionedTable(SqlInsert sqlInsert,
                                                                                            TableMeta tableMeta,
                                                                                            PartitionInfo partInfo,
                                                                                            ExecutionContext executionContext,
                                                                                            String schemaName) {

        String logTbName = tableMeta.getTableName();
        SqlNode rowValuesAst = sqlInsert.getSource();

        List<ColumnMeta> rowColMeta = new ArrayList<>();
        SqlNodeList rowColsAst = sqlInsert.getTargetColumnList();
        for (int i = 0; i < rowColsAst.size(); i++) {
            SqlNode rowCol = rowColsAst.get(i);
            if (rowCol instanceof SqlIdentifier) {
                String colName = ((SqlIdentifier) rowCol).getLastName();
                ColumnMeta cm = tableMeta.getColumn(colName);
                rowColMeta.add(cm);
            }
        }

        boolean isBatch = executionContext.getParams().isBatch();
        List<Integer> partColIndexList;
        Parameters parameters = executionContext.getParams();

        if (partInfo == null) {
            partInfo = tableMeta.getPartitionInfo();
        }

        /**
         * isBatch:
         *   isBatch=false,
         *     e.g. convert "values (?,?,?)" to one tupleRouteFn
         *   isBatch=true
         *     e.g. convert "values (?,?+?,?)(?-?,?,?),..." to multi tupleRouteFn
         */
        PartitionTupleRoutingContext routingContext =
            PartitionTupleRoutingContext.buildPartitionTupleRoutingContext(schemaName, logTbName, partInfo, rowColMeta);
        partColIndexList = routingContext.getPartColIndexMappings();
        PartitionTupleRouteInfo tupleRouteInfo =
            buildPartitionTupleRouteInfo(routingContext, rowValuesAst, isBatch, executionContext);

        // Pick part columns Values from insert Values ast of SqlInsert，
        // each column of each value of multiValues is tha same as part column definitions
        List<List<Object>> multiValues = valuesForInsert(sqlInsert, partColIndexList, parameters, true);

        // Do Tuple Routing
        Map<String, Map<String, List<Integer>>> shardResults = new HashMap<>();
        // Ust tmp ExecutionContext to dynamic update the info of params for part pruning
        ExecutionContext tmpEc = executionContext.copy();
        int tupleTemplateIdx = 0;
        for (int i = 0; i < multiValues.size(); i++) {
            if (!isBatch) {
                tupleTemplateIdx = i;
            }
            List<Object> singleValue = multiValues.get(i);
            // multiValues has been the order of part column definitions, so need not do partColIndexMapping
            Parameters singleValueParams = routingContext.createPartColValueParameters(singleValue, false);
            Pair<String, String> dbAndTable =
                routeSingleValueRow(tupleRouteInfo, tmpEc, false, tupleTemplateIdx, singleValueParams);
            shardResults.computeIfAbsent(dbAndTable.getKey(), b -> new HashMap<>())
                .computeIfAbsent(dbAndTable.getValue(), b -> new ArrayList<>())
                .add(i);
        }
        return shardResults;
    }

    /**
     * Get column names from RelDataTypeField. Names of ColumnMeta are different
     * with RelDataTypeField
     */
    public static List<String> getRelColumnNames(TableMeta tableMeta, List<ColumnMeta> columnMetas) {
        List<String> columnNames = new ArrayList<>(columnMetas.size());
        RelDataTypeFactory typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        for (ColumnMeta columnMeta : columnMetas) {
            RelDataTypeField relDataTypeField = tableMeta.getRowTypeIgnoreCase(columnMeta.getName(), typeFactory);
            columnNames.add(relDataTypeField.getName());
        }
        return columnNames;
    }

    public static Map<String, Map<String, List<Pair<Integer, List<Object>>>>> buildResultForSingleTable(
        String schemaName,
        String logicalTableName,
        List<List<Object>> values,
        List<Integer> primaryKeyIndexes) {
        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        String physicalTableName;
        String dbIndex = "";
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            PartitionInfo partitionInfo = optimizerContext.getPartitionInfoManager().getPartitionInfo(logicalTableName);
            physicalTableName = partitionInfo.getPrefixTableName();

            Map<String, Set<String>> topology = partitionInfo.getTopology();
            assert (topology.size() == 1);
            for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
                dbIndex = entry.getKey();
            }
        } else {
            TddlRuleManager or = optimizerContext.getRuleManager();
            TableRule tableRule = or.getTableRule(logicalTableName);
            physicalTableName = tableRule.getTbNamePattern();

            if (tableRule != null) {
                dbIndex = tableRule.getDbNamePattern();
            } else {
                dbIndex = or.getDefaultDbIndex(logicalTableName);
            }
        }

        List<Pair<Integer, List<Object>>> rowList = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            List<Object> row = values.get(i);
            List<Object> primaryKeyValues = primaryKeyIndexes.stream().map(row::get).collect(Collectors.toList());
            rowList.add(Pair.of(i, primaryKeyValues));
        }

        // targetDb: { targetTb: [[pk1], [pk2]] }
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults = new HashMap<>();
        shardResults.computeIfAbsent(dbIndex, b -> new HashMap<>()).put(physicalTableName, rowList);
        return shardResults;
    }

    /**
     * Pick values from INSERT.
     *
     * @param sqlInsert logical or physical insert
     * @param pickColumnNames the columns that need to pick
     * @param parameters corresponding parameters
     * @param isLogical is logical insert or physical insert
     * @param pickedColumnIndexes is output of picked column indexes
     * @return values list
     */
    public static List<List<Object>> pickValuesFromInsert(SqlInsert sqlInsert, List<String> pickColumnNames,
                                                          Parameters parameters, boolean isLogical,
                                                          List<Integer> pickedColumnIndexes) {
        // get column indexes of columnNames
        List<Integer> tmpPickedColumnIndexes = getColumnIndexesInInsert(sqlInsert, pickColumnNames);
        pickedColumnIndexes.addAll(tmpPickedColumnIndexes);
        return valuesForInsert(sqlInsert, pickedColumnIndexes, parameters, isLogical);
    }

    public static List<List<Object>> valuesForInsert(SqlInsert sqlInsert, List<Integer> pickedColumnIndexes,
                                                     Parameters parameters, boolean isLogical) {
        List<SqlNode> rows = ((SqlCall) sqlInsert.getSource()).getOperandList();
        List<List<Object>> values;
        if (parameters.isBatch()) {
            List<Map<Integer, ParameterContext>> batchParams = parameters.getBatchParameters();
            values = new ArrayList<>(batchParams.size());
            List<SqlNode> nodesInRow = ((SqlCall) rows.get(0)).getOperandList();
            for (Map<Integer, ParameterContext> batchParam : batchParams) {
                List<Object> pickedValues = pickValuesFromRow(batchParam, nodesInRow, pickedColumnIndexes, isLogical);
                values.add(pickedValues);
            }
        } else {
            values = new ArrayList<>(rows.size());
            Map<Integer, ParameterContext> params = parameters.getCurrentParameter();
            for (SqlNode row : rows) {
                List<SqlNode> nodesInRow = ((SqlCall) row).getOperandList();
                List<Object> pickedValues = pickValuesFromRow(params, nodesInRow, pickedColumnIndexes, isLogical);
                values.add(pickedValues);
            }
        }

        return values;
    }

    /**
     * Pick interested columns from one row. The column may be sharding key or
     * unique key. For logical insert, dynamicParam.index + 1 =
     * ParameterContext.index. For physical insert, dynamicParam.index + 2 =
     * ParameterContext.index.
     *
     * @param params params for the physical insert
     * @param columns all columns in the row
     * @param keyIndexes indexes of interested columns
     * @param isLogical is it a logical insert
     * @return picked values
     */
    public static List<Object> pickValuesFromRow(Map<Integer, ParameterContext> params, List<SqlNode> columns,
                                                 List<Integer> keyIndexes, boolean isLogical) {
        List<Object> keyValues = new ArrayList<>(keyIndexes.size());

        if (GeneralUtil.isEmpty(keyIndexes)) {
            return keyValues;
        }

        int indexDiff = isLogical ? 1 : 2;
        for (int index : keyIndexes) {
            SqlNode column = columns.get(index);
            if (column instanceof SqlDynamicParam) {
                int dynamicParamIndex = ((SqlDynamicParam) column).getIndex();
                keyValues.add(params.get(dynamicParamIndex + indexDiff).getValue());
            } else if (column instanceof SqlLiteral) {
                keyValues.add(getValueForParameter((SqlLiteral) column));
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED,
                    "sharding keys or unique keys must be literal in inserting global secondary index");
            }
        }

        return keyValues;
    }

    public static Object getValueForParameter(SqlLiteral literal) {
        Object value;
        switch (literal.getTypeName()) {
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case DATE:
            value = literal.getValueAs(Calendar.class);
            break;
        case CHAR:
            // If time is evaluated from now(), it'll be NlsString
            value = literal.getValueAs(String.class);
            break;
        default:
            value = literal.getValue();
            break;
        }
        return value;
    }

    /**
     * Get the index of each given column in the insert target column list.
     *
     * @param sqlInsert the insert to be searched
     * @param pickColumnNames the columns to be searched for
     * @return indexes
     */
    public static List<Integer> getColumnIndexesInInsert(SqlInsert sqlInsert, List<String> pickColumnNames) {
        List<Integer> pickedColumnIndexes = new ArrayList<>(pickColumnNames.size());
        SqlNodeList targetColumnList = sqlInsert.getTargetColumnList();
        for (String keyName : pickColumnNames) {
            int index = -1;
            for (int i = 0; i < targetColumnList.size(); i++) {
                if (((SqlIdentifier) targetColumnList.get(i)).getLastName().equalsIgnoreCase(keyName)) {
                    index = i;
                    break;
                }
            }
            // if it's absent, it's using default value
            if (index < 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_KEY_DEFAULT, keyName);
            }
            pickedColumnIndexes.add(index);
        }
        return pickedColumnIndexes;
    }

    /**
     * Classify columns by whether it is included in table
     *
     * @param columns list[ C0, ... , Cn-1 ], Column names
     * @param tableIndexes list[ T0, ... , Tn-1 ], Table Index for each column
     * @param tableGsiMap map[ primary, list[ GSI0, ..., GSIm-1 ]], might be more than one primary table
     * @param outPrimaryColumnMappings map[ primary, list[ index of columns ]], Classified columns by primary table
     * @param outGsiColumnMappings map[ primary, list[ list[ index of columns ]0, ..., list[ index of columns ]m-1], Classified columns by primary and gsi table
     */
    public static void buildColumnMappings(List<String> columns,
                                           List<Integer> tableIndexes,
                                           Map<Integer, List<TableMeta>> tableGsiMap,
                                           Map<Integer, List<Integer>> outPrimaryColumnMappings,
                                           Map<Integer, List<List<Integer>>> outGsiColumnMappings) {
        IntStream.range(0, columns.size()).forEach(i -> {
            final String column = columns.get(i);
            final Integer primaryIndex = tableIndexes.get(i);
            final List<TableMeta> gsiMetas = tableGsiMap.get(primaryIndex);

            final List<Integer> primaryUpdateColumnMapping = outPrimaryColumnMappings.computeIfAbsent(primaryIndex,
                (t) -> new ArrayList<>());

            primaryUpdateColumnMapping.add(i);

            if (GeneralUtil.isNotEmpty(gsiMetas)) {
                final List<List<Integer>> gsiUpdateColumnMapping = outGsiColumnMappings.computeIfAbsent(primaryIndex,
                    (t) -> new ArrayList<>());
                Ord.zip(gsiMetas).forEach(o -> {
                    final TableMeta gsi = o.e;
                    final int gsiIndex = o.i;
                    if (gsiUpdateColumnMapping.size() <= gsiIndex) {
                        gsiUpdateColumnMapping.add(new ArrayList<>());
                    }

                    if (null != gsi.getColumn(column)) {
                        gsiUpdateColumnMapping.get(gsiIndex).add(i);
                    }
                });
            }
        });
    }

    public static List<String> buildUpdateColumnList(LogicalInsert upsert) {
        final List<String> result = new ArrayList<>();

        if (!upsert.withDuplicateKeyUpdate()) {
            return result;
        }

        final List<String> targetTableColumnNames = upsert.getTable().getRowType().getFieldNames();

        upsert.getDuplicateKeyUpdateList().stream()
            .map(rex -> ((RexInputRef) ((RexCall) rex).getOperands().get(0)).getIndex())
            .map(targetTableColumnNames::get).forEach(result::add);

        return result;
    }

    public static List<String> buildUpdateColumnList(SqlNodeList duplicateKeyUpdateList,
                                                     Consumer<SqlNode> unsupported) {
        final List<String> result = new ArrayList<>();

        if (null == duplicateKeyUpdateList || duplicateKeyUpdateList.size() == 0) {
            return result;
        }

        for (SqlNode sqlNode : duplicateKeyUpdateList) {
            if (!(sqlNode instanceof SqlBasicCall)) {
                unsupported.accept(sqlNode);
                continue;
            }

            final SqlNode columnName = ((SqlBasicCall) sqlNode).getOperandList().get(0);
            if (!(columnName instanceof SqlIdentifier)) {
                unsupported.accept(columnName);
                continue;
            }

            result.add(RelUtils.lastStringValue(columnName));
        }

        return result;
    }

    public static List<Map<Integer, ParameterContext>> buildInsertBatchParam(List<List<Object>> values) {
        final int batchSize = values.size();
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>(batchSize);

        // Build parameters for insert
        for (List<Object> rowValue : values) {
            final Map<Integer, ParameterContext> rowParams = new HashMap<>(rowValue.size());

            for (int columnIndex = 0; columnIndex < rowValue.size(); columnIndex++) {
                final Object value = rowValue.get(columnIndex);
                rowParams.put(columnIndex + 1,
                    new ParameterContext(ParameterMethod.setObject1, new Object[] {
                        columnIndex + 1, value instanceof EnumValue ? ((EnumValue) value).getValue() : value}));
            }
            batchParams.add(rowParams);
        }
        return batchParams;
    }
}
