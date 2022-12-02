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

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.oss.OSSFileType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OSSOrcFileManager implements FileManager {
    public OSSOrcFileManager() {
    }

    @Override
    public List<FileMeta> getFiles(String physicalSchema, String physicalTable, String logicalTableName) {
        List<FileMeta> fileMetaSet = new ArrayList<>();
        try (Connection connection = MetaDbUtil.getConnection()) {
            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(connection);
            // query meta db && filter table files.
            List<FilesRecord> filesRecords = filesAccessor
                .query(physicalSchema, physicalTable, logicalTableName)
                .stream()
                .filter(filesRecord -> OSSFileType.of(filesRecord.fileType) == OSSFileType.TABLE_FILE)
                .collect(Collectors.toList());
            filesRecords.forEach(
                filesRecord -> {
                    FileMeta fileMeta = FileMeta.parseFrom(filesRecord);
                    fileMetaSet.add(fileMeta);
                }
            );
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
        return fileMetaSet;
    }

    @Override
    public Map<String, Map<String, List<FileMeta>>> getFiles(TableMeta tableMeta) {
        Map<String, Set<String>> topology = tableMeta.getPartitionInfo().getTopology(true);
        Map<String, Map<String, List<FileMeta>>> fileMetaMap = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
            String groupKey = entry.getKey();
            Map<String, List<FileMeta>> phySchemaMap = new HashMap<>();
            Set<String> physicalTableNameSet = entry.getValue();
            for (String physicalTable : physicalTableNameSet) {
                List<FileMeta> phyTableList = new ArrayList<>();
                List<FileMeta> phyTableFileMetas = getFiles(groupKey, physicalTable, tableMeta.getTableName());
                // fill with column metas
                phyTableFileMetas.forEach(fileMeta -> fileMeta.initColumnMetas(tableMeta));
                phyTableList.addAll(phyTableFileMetas);
                phySchemaMap.put(physicalTable, phyTableList);
            }
            fileMetaMap.put(groupKey, phySchemaMap);
        }
        return fileMetaMap;
    }
}