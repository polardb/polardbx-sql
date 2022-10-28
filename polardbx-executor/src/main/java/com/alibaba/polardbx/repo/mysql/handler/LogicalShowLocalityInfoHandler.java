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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.privilege.DbInfo;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.locality.LocalityInfo;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowLocalityInfo;
import org.apache.calcite.sql.SqlShowTableInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author jinkun.taojinkun
 */
public class LogicalShowLocalityInfoHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowDbStatusHandler.class);

    public LogicalShowLocalityInfoHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowLocalityInfo showLocalityInfo = (SqlShowLocalityInfo) show.getNativeSqlNode();

        String schemaName = showLocalityInfo.getSchema();
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return handleNewPartitionTable(executionContext, schemaName);
        }else{
            return getLocalityInfoResultCursor();
        }
    }

    private ArrayResultCursor getLocalityInfoResultCursor() {
        ArrayResultCursor result = new ArrayResultCursor("LOCALITY_INFO");
        result.addColumn("OBJECT_ID", DataTypes.LongType);
        result.addColumn("OBJECT_NAME", DataTypes.StringType);
        result.addColumn("OBJECT_TYPE", DataTypes.StringType);
        result.addColumn("LOCALITY", DataTypes.StringType);
        result.addColumn("OBJECT_GROUP_ELEMENT", DataTypes.StringType);
        result.initMeta();
        return result;
    }

    private Cursor handleNewPartitionTable(ExecutionContext executionContext, String schemaName) {
        ArrayResultCursor result = getLocalityInfoResultCursor();
        TableGroupInfoManager tableGroupInfoManager = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        LocalityManager localityManager = LocalityManager.getInstance();

        try (Connection connection = MetaDbUtil.getConnection()){
            List<TableGroupConfig> tableGroupConfigList = tableGroupInfoManager.getTableGroupConfigInfoCache().values().stream().collect(Collectors.toList());
            DbInfoManager dbInfoManager = DbInfoManager.getInstance();
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);
            List<TablesRecord> tableInfoList = tableInfoManager.queryTables(schemaName);
            List<PartitionGroupRecord> partitionGroupRecordList = new ArrayList<>();

            //database
            Long objectId;
            String objectName, locality;
            objectId = dbInfoManager.getDbInfo(schemaName).id;
            objectName = schemaName;
            locality = localityManager.getLocalityOfDb(objectId).getLocality();
            result.addRow(new Object[]{objectId, objectName, "database", locality, ""});

            for(TablesRecord tableInfo:tableInfoList){
               objectId = tableInfo.id;
               objectName = tableInfo.tableName;
               locality = partitionInfoManager.getPartitionInfo(objectName).getLocality();
               result.addRow(new Object[]{objectId, objectName, "table", locality, ""});
           }

           for(TableGroupConfig tableGroupConfig:tableGroupConfigList){
               objectId = tableGroupConfig.getTableGroupRecord().getId();
               partitionGroupRecordList = tableGroupConfig.getPartitionGroupRecords();
               objectName = tableGroupConfig.getTableGroupRecord().getTg_name();
               locality = tableGroupConfig.getLocalityDesc().toString();
               List<String> tableList = tableGroupConfig.getAllTables().stream().map(o->o.getTableName()).collect(Collectors.toList());
               String tableListString = String.join(",", tableList);
               result.addRow(new Object[]{objectId, objectName, "tablegroup", locality, tableListString});
               for(PartitionGroupRecord partitionGroupRecord:partitionGroupRecordList){
                   Long partitionGroupId = partitionGroupRecord.id;
                   String partitionGroupName = partitionGroupRecord.partition_name;
                   locality = partitionGroupRecord.getLocality();
                   if(!StringUtils.isEmpty(locality)) {
                       result.addRow(new Object[]{partitionGroupId, objectName + "." + partitionGroupName, "partitiongroup", locality, ""});
                   }
               }
            }
        } catch (SQLException e){
            logger.error(String.format(
                    "error occurs while show locality: %s", schemaName), e);
            throw GeneralUtil.nestedException(e);
        }finally{
            return result;
        }
    }
}
