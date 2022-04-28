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

package com.alibaba.polardbx.executor.partitionmanagement;

import com.alibaba.polardbx.common.utils.time.calculator.MySQLInterval;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.repo.mysql.checktable.CheckTableUtil;
import com.alibaba.polardbx.repo.mysql.checktable.LocalPartitionDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.MAXVALUE;
import static com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo.before;

public class LocalPartitionManager {

    /**
     * @param schemaName
     * @param tableName
     * @return
     */
    public static List<TableDescription> getLocalPartitionInfoList(MyRepository repo,
                                                                   String schemaName,
                                                                   String tableName,
                                                                   boolean limitOne){
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        Map<String, List<List<String>>> tableTopology =
            PartitionInfoUtil.buildTargetTablesFromPartitionInfo(tableMeta.getPartitionInfo());

        List<TableDescription> phyTablePartitions = new ArrayList<>();
        for (Map.Entry<String, List<List<String>>> topo : tableTopology.entrySet()) {
            String groupName = topo.getKey();
            List<List<String>> phyTableList = topo.getValue();
            if(CollectionUtils.isEmpty(phyTableList)){
                continue;
            }
            for(List<String> list: phyTableList){
                if(CollectionUtils.isEmpty(list)){
                    continue;
                }
                for(String phyTableName: list){
                    List<LocalPartitionDescription> localPartitionList =
                        CheckTableUtil.getLocalPartitionDescription(repo, groupName, phyTableName);
                    localPartitionList.sort((o1, o2) -> o1.comparePartitionOrdinalPosition(o2));
                    TableDescription tableDescription = new TableDescription(groupName, phyTableName);
                    tableDescription.setPartitions(localPartitionList);
                    phyTablePartitions.add(tableDescription);
                    if(limitOne){
                        return phyTablePartitions;
                    }
                }
            }
        }
        return phyTablePartitions;
    }

    /**
     * 获取一个tableDescription所有过期的分区
     * @param definitionInfo 逻辑表的local partition元数据
     * @param tableDescription 逻辑表的某个物理分片的描述信息
     * @param pivotDate 基准时间
     * @return
     */
    public static List<LocalPartitionDescription> getExpiredLocalPartitionDescriptionList(LocalPartitionDefinitionInfo definitionInfo,
                                                                                          TableDescription tableDescription,
                                                                                          MysqlDateTime pivotDate){
        List<LocalPartitionDescription> result = new ArrayList<>();
        if(!definitionInfo.getExpirationInterval().isPresent()){
            return result;
        }
        List<LocalPartitionDescription> partitionList = tableDescription.getPartitions();
        if(CollectionUtils.isEmpty(partitionList)){
            return result;
        }
        MySQLInterval expireInterval = definitionInfo.getExpirationInterval().get();
        List<LocalPartitionDescription> candidates = new ArrayList<>();
        candidates.addAll(partitionList);
        candidates.sort((o1, o2) -> o1.comparePartitionOrdinalPosition(o2));
        for(int i=0;i<candidates.size();i++){
            LocalPartitionDescription localPartitionInfo = candidates.get(i);
            if(StringUtils.equalsIgnoreCase(localPartitionInfo.getPartitionDescription(), MAXVALUE)){
                continue;
            }
            MysqlDateTime partitionDate = parsePartitionDate(localPartitionInfo.getPartitionDescription());
            MysqlDateTime expirationDate = MySQLTimeCalculator.addInterval(partitionDate, definitionInfo.getIntervalType(), expireInterval);
            if(before(expirationDate, pivotDate)){
                result.add(localPartitionInfo);
            }
        }
        return result;
    }

    public static MysqlDateTime parsePartitionDate(String originDateStr){
        String dateStr = originDateStr.replace("'", "");
        MysqlDateTime partitionDate = StringTimeParser.parseDatetime(dateStr.getBytes());
        return partitionDate;
    }

    /**
     * 获取最新的分区
     * @param tableDescription 逻辑表的某个物理分片的描述信息
     * @return
     */
    public static MysqlDateTime getNewestPartitionDate(TableDescription tableDescription){
        List<LocalPartitionDescription> partitionList = tableDescription.getPartitions();
        if(CollectionUtils.isEmpty(partitionList)){
            return null;
        }
        List<LocalPartitionDescription> candidates = new ArrayList<>();
        candidates.addAll(partitionList);
        candidates.sort((o1, o2) -> o1.comparePartitionOrdinalPosition(o2));
        for(int i=candidates.size()-1;i>=0;i--){
            LocalPartitionDescription partitionDescription = candidates.get(i);
            String dateStr = partitionDescription.getPartitionDescription().replace("'", "");
            if(StringUtils.equalsIgnoreCase(dateStr, MAXVALUE)){
                continue;
            }
            return StringTimeParser.parseDatetime(dateStr.getBytes());
        }
        return null;
    }

    public static boolean checkLocalPartitionConsistency(TableDescription expect,
                                                         List<TableDescription> phyTablePartitions){
        if(CollectionUtils.isEmpty(phyTablePartitions)){
            return false;
        }

        final int expectPartitionSize = expect.getPartitions().size();
        for(int i=0;i<phyTablePartitions.size();i++){
            if(expectPartitionSize != phyTablePartitions.get(i).getPartitions().size()){
                //不同物理表的partition数量不同
                return false;
            }
        }

        for(int partitionIndex=0;partitionIndex<expectPartitionSize;partitionIndex++){
            for(int tableIndex=0; tableIndex<phyTablePartitions.size();tableIndex++){
                TableDescription currentTable = phyTablePartitions.get(tableIndex);
                LocalPartitionDescription sourcePartitionDesc = expect.getPartitions().get(partitionIndex);
                LocalPartitionDescription targetPartitionDesc = currentTable.getPartitions().get(partitionIndex);
                if(!sourcePartitionDesc.rangeIdentical(targetPartitionDesc)){
                    return false;
                }
            }
        }
        return true;
    }

}