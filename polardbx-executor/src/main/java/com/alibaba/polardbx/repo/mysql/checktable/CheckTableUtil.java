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

package com.alibaba.polardbx.repo.mysql.checktable;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.MysqlErrorNumbers;
import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author arnkore 2017-06-19 17:47
 */
public class CheckTableUtil {

    private static final Logger logger = LoggerFactory.getLogger(CheckTableUtil.class);

    public static TAtomDataSource findMasterAtomForGroup(TGroupDataSource groupDs) {
        TAtomDataSource targetAtom = null;
        Weight targetAtomWeight = null;
        boolean isFindMaster = false;
        List<TAtomDataSource> atomList = groupDs.getAtomDataSources();
        Map<TAtomDataSource, Weight> atomDsWeightMaps = groupDs.getAtomDataSourceWeights();
        for (Map.Entry<TAtomDataSource, Weight> atomWeightItem : atomDsWeightMaps.entrySet()) {
            targetAtom = atomWeightItem.getKey();
            targetAtomWeight = atomWeightItem.getValue();
            if (targetAtomWeight.w > 0) {
                isFindMaster = true;
                break;
            }
        }

        if (isFindMaster) {
            return targetAtom;
        } else {
            targetAtom = atomList.get(0);
        }

        return targetAtom;
    }

    public static TableDescription getTableDescription(MyRepository myRepository, String groupName, String tableName,
                                                       boolean isShadow, String schemaName) {
        TableDescription tableDescription = new TableDescription();
        TGroupDataSource groupDs = (TGroupDataSource) myRepository.getDataSource(groupName);
        TAtomDataSource masterAtom = findMasterAtomForGroup(groupDs);

        tableDescription.setGroupName(groupName);
        tableDescription.setTableName(tableName);

        Connection conn = null;
        ResultSet rs = null;
        Throwable ex = null;
        try {
            StringBuilder targetSql = new StringBuilder("describe ");
            targetSql.append("`" + tableName + "`");
            String sql = targetSql.toString();
            if (isShadow) {
                sql = "select * from information_schema.columns where table_name='" + tableName + "' and TABLE_SCHEMA='"
                    + schemaName + "'";
            }

            conn = (Connection) masterAtom.getConnection();

            rs = conn.createStatement().executeQuery(sql);

            Map<String, FieldDescription> fieldDescMaps = new HashMap<String, FieldDescription>();
            Map<String, FieldDescription> physicalOrderFieldMaps = new LinkedHashMap<String, FieldDescription>();
            while (rs.next()) {
                FieldDescription fd = new FieldDescription();
                if (isShadow) {
                    fd.setFieldName(rs.getString("COLUMN_NAME"));
                    fd.setFieldType(rs.getString("COLUMN_TYPE"));
                    fd.setFieldNull(rs.getString("IS_NULLABLE"));
                    fd.setFieldKey(rs.getString("COLUMN_KEY"));
                    fd.setFieldDefault(rs.getString("COLUMN_DEFAULT"));
                    fd.setFieldExtra(rs.getString("EXTRA"));
                    fd.setShadowTable(true);
                } else {
                    fd.setFieldName(rs.getString(1));
                    fd.setFieldType(rs.getString(2));
                    fd.setFieldNull(rs.getString(3));
                    fd.setFieldKey(rs.getString(4));
                    fd.setFieldDefault(rs.getString(5));
                    fd.setFieldExtra(rs.getString(6));
                }
                fieldDescMaps.put(fd.getFieldName(), fd);
                physicalOrderFieldMaps.put(fd.getFieldName(), fd);
            }
            tableDescription.setFields(fieldDescMaps);
            tableDescription.setPhysicalOrderFields(physicalOrderFieldMaps);

        } catch (Exception e) {
            // 打好相关的日志
            if (e instanceof SQLException) {
                if (((SQLException) e).getErrorCode() == MysqlErrorNumbers.ER_NO_SUCH_TABLE) {
                    // mysql 报没有这个表，则表示直接将这个表置为null
                    tableDescription.setFields(null);
                }
                ex = e;
            } else {
                // 注意打好日志
                logger.error(e);
                ex = e;
            }
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }

                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException e) {
                // 打好相关的日志
                logger.error(e);
            }

            if (ex != null && !ConfigDataMode.isFastMock()) {
                throw GeneralUtil.nestedException(ex);
            }
        }

        return tableDescription;
    }

    public static List<LocalPartitionDescription> getLocalPartitionDescription(MyRepository myRepository,
                                                                               String groupName,
                                                                               String tableName) {
        final List<LocalPartitionDescription> localPartitionMap = new ArrayList<>();

        TGroupDataSource groupDs = (TGroupDataSource) myRepository.getDataSource(groupName);
        TAtomDataSource masterAtom = findMasterAtomForGroup(groupDs);
        final String physicalSchemaName = masterAtom.getDsConfHandle().getRunTimeConf().getDbName();

        Connection conn = null;
        ResultSet rs = null;
        Throwable ex = null;
        try {
            String sql = "select * from information_schema.partitions where table_name=? and TABLE_SCHEMA=?";
            conn = masterAtom.getConnection();
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString(1, tableName);
            preparedStatement.setString(2, physicalSchemaName);

            rs = preparedStatement.executeQuery();

            while (rs.next()) {
                LocalPartitionDescription fd = new LocalPartitionDescription();
                fd.setTableCatalog(rs.getString("TABLE_CATALOG"));
                fd.setTableSchema(rs.getString("TABLE_SCHEMA"));
                fd.setTableName(rs.getString("TABLE_NAME"));
                fd.setPartitionName(rs.getString("PARTITION_NAME"));
                fd.setSubpartitionName(rs.getString("SUBPARTITION_NAME"));
                fd.setPartitionOrdinalPosition(rs.getLong("PARTITION_ORDINAL_POSITION"));
                fd.setSubpartitionOrdinalPosition(rs.getLong("SUBPARTITION_ORDINAL_POSITION"));
                fd.setPartitionMethod(rs.getString("PARTITION_METHOD"));
                fd.setSubpartitionMethod(rs.getString("SUBPARTITION_METHOD"));
                fd.setPartitionExpression(rs.getString("PARTITION_EXPRESSION"));
                fd.setSubpartitionExpression(rs.getString("SUBPARTITION_EXPRESSION"));
                fd.setPartitionDescription(rs.getString("PARTITION_DESCRIPTION"));
                fd.setTableRows(rs.getLong("TABLE_ROWS"));
                fd.setAvgRowLength(rs.getLong("AVG_ROW_LENGTH"));
                fd.setDataLength(rs.getLong("DATA_LENGTH"));
                fd.setMaxDataLength(rs.getLong("MAX_DATA_LENGTH"));
                fd.setIndexLength(rs.getLong("INDEX_LENGTH"));
                fd.setDataFree(rs.getLong("DATA_FREE"));
//                fd.setCreateTime(rs.getDate("CREATE_TIME"));
//                fd.setUpdateTime(rs.getDate("UPDATE_TIME"));
//                fd.setCheckTime(rs.getDate("CHECK_TIME"));
                fd.setChecksum(rs.getLong("CHECKSUM"));
                fd.setPartitionComment(rs.getString("PARTITION_COMMENT"));
                fd.setNodegroup(rs.getString("NODEGROUP"));
                fd.setTablespaceName(rs.getString("TABLESPACE_NAME"));
                localPartitionMap.add(fd);
            }

        } catch (Exception e) {
            // 打好相关的日志
            if (e instanceof SQLException) {
                if (((SQLException) e).getErrorCode() == MysqlErrorNumbers.ER_NO_SUCH_TABLE) {
                    // mysql 报没有这个表，则表示直接将这个表置为null
                }
                ex = e;
            } else {
                // 注意打好日志
                logger.error(e);
                ex = e;
            }
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                // 打好相关的日志
                logger.error(e);
            }
            if (ex != null) {
                throw GeneralUtil.nestedException(ex);
            }
        }
        return localPartitionMap;
    }

    public static TableCheckResult verifyTableAndGsiMeta(TableDescription tableDesc, TableDescription gsiDesc) {
        TableCheckResult tableCheckResult = new TableCheckResult();
        tableCheckResult.setTableDesc(gsiDesc);
        tableCheckResult.setExist(true);

        Map<String, FieldDescription> gsiFieldDescMap = gsiDesc.getFields();
        Map<String, FieldDescription> tableFieldDescMap = tableDesc.getFields();

        for (Map.Entry<String, FieldDescription> gsiFieldDesc : gsiFieldDescMap.entrySet()) {
            String fieldName = gsiFieldDesc.getKey();
            FieldDescription fieldDesc = gsiFieldDesc.getValue();

            FieldDescription tableFieldDesc = tableFieldDescMap.get(fieldName);
            if (tableFieldDesc != null) {
                if (!fieldDesc.equalsTableAndGsi(tableFieldDesc)) {
                    tableCheckResult.setFieldDescTheSame(false);
                    tableCheckResult.addIncorrectFieldDescMaps(fieldDesc);
                }
            } else {
                tableCheckResult.addUnexpectedFieldDesc(fieldDesc);
            }
        }
        return tableCheckResult;
    }

    public static TableCheckResult verifylogicalAndPhysicalMeta(TableDescription physicalTableDesc,
                                                                List<FieldDescription> logicalMetaDescs) {
        TableCheckResult tableCheckResult = new TableCheckResult();
        tableCheckResult.setTableDesc(physicalTableDesc);
        tableCheckResult.setExist(true);

        Map<String, FieldDescription> physicalDescs = physicalTableDesc.getPhysicalOrderFields();
//        List<FieldDescription> physicalDescs = Arrays.asList(logicalTablePhysicalDesc.values().toArray(new FieldDescription[0]));
        Set<String> fieldNameSet = new HashSet<>();
        for (FieldDescription logicalMetaDesc : logicalMetaDescs) {
            String fieldName = logicalMetaDesc.fieldName;
            fieldNameSet.add(fieldName);
            FieldDescription physicalDesc = physicalDescs.get(fieldName);
            if (physicalDesc != null) {
                if (!physicalDesc.equalsLogicalAndPhysicalMeta(logicalMetaDesc)) {
                    tableCheckResult.setFieldDescTheSame(false);
                    tableCheckResult.addIncorrectFieldDescMaps(physicalDesc);
                }
            } else {
                tableCheckResult.addMissingFieldDesc(logicalMetaDesc);
            }
        }
        Set<String> unexpectedFieldNames = physicalDescs.keySet();
        unexpectedFieldNames.removeAll(fieldNameSet);
        for (String unexpectedFieldName : unexpectedFieldNames) {
            tableCheckResult.addUnexpectedFieldDesc(physicalDescs.get(unexpectedFieldName));
        }
        return tableCheckResult;
    }

    public static TableCheckResult verifyTableMeta(TableDescription referTableDesc, TableDescription targetTableDesc) {
        TableCheckResult tableCheckResult = new TableCheckResult();
        tableCheckResult.setTableDesc(targetTableDesc);
        tableCheckResult.setExist(true);

        Map<String, FieldDescription> referTableFieldDescMap = referTableDesc.getFields();
        Map<String, FieldDescription> targetTableFieldDescMap = targetTableDesc.getFields();

        // 对于参考表的每个列的定义，是否目标表都有并且列定义相同
        // 这里可以检查出定义不同的列或目标表缺某个列
        for (Map.Entry<String, FieldDescription> referTableFieldDesc : referTableFieldDescMap.entrySet()) {
            String fieldName = referTableFieldDesc.getKey();
            FieldDescription referFieldDescription = referTableFieldDesc.getValue();

            FieldDescription targetFieldDescription = targetTableFieldDescMap.get(fieldName);
            if (targetFieldDescription != null) {
                if (!targetFieldDescription.equals(referFieldDescription)) {
                    tableCheckResult.setFieldDescTheSame(false);
                    tableCheckResult.addIncorrectFieldDescMaps(targetFieldDescription);
                }
            } else {
                tableCheckResult.addMissingFieldDesc(referFieldDescription);
            }
        }

        // 对于目标表的每个列的定义，是否参考表都存在
        // 这里可以检查出目标表是否比参考表多了几个列
        for (Map.Entry<String, FieldDescription> targetTableFieldDesc : targetTableFieldDescMap.entrySet()) {
            String fieldName = targetTableFieldDesc.getKey();
            FieldDescription fieldDescription = targetTableFieldDesc.getValue();

            FieldDescription referFieldDescription = referTableFieldDescMap.get(fieldName);
            if (referFieldDescription == null) {
                tableCheckResult.setFieldDescTheSame(false);
                tableCheckResult.addUnexpectedFieldDesc(fieldDescription);
            }
        }

        return tableCheckResult;
    }
}
