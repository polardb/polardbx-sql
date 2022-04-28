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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetaAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcTail;

import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OSSOrcFileMeta extends FileMeta {
    protected TypeDescription typeDescription;
    protected Map<String, ColumnStatistics> statisticsMap;
    protected OrcTail orcTail;
    // <column name - <stripe index - meta>>
    protected volatile Map<String, Map<Long, StripeColumnMeta>> stripeColumnMetaMap;
    protected String createTime;
    protected String updateTime;
    protected Engine engine;
    protected Long commitTs;
    protected Long removeTs;
    private Map<String, Integer> columnNameToIdx;
    private Map<Integer, String> idxToColumnName;
    private final static Configuration configuration = new Configuration();

    public OSSOrcFileMeta(String logicalSchemaName, String logicalTableName, String physicalTableSchema,
                          String physicalTableName, String fileName, long fileSize,
                          long tableRows, ByteBuffer tailBuffer, String createTime, String updateTime, Engine engine,
                          Long commitTs, Long removeTs) {
        super(logicalSchemaName, logicalTableName, physicalTableSchema, physicalTableName, fileName, fileSize,
            tableRows);
        this.orcTail = OrcMetaUtils.extractFileTail(tailBuffer);
        this.typeDescription = this.orcTail.getSchema();
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.engine = engine;
        this.commitTs = commitTs;
        this.removeTs = removeTs;
        // invoke id assignment.
        this.typeDescription.getId();
        this.statisticsMap = new HashMap<>();
        this.stripeColumnMetaMap = null;
        this.columnNameToIdx = new HashMap<>();
        this.idxToColumnName = new HashMap<>();
        List<OrcProto.ColumnStatistics> fileStats = this.orcTail.getFooter().getStatisticsList();
        ColumnStatistics[] columnStatisticsArray = OrcMetaUtils.deserializeStats(this.typeDescription, fileStats);
        for (String fieldName : this.typeDescription.getFieldNames()) {
            TypeDescription subSchema = this.typeDescription.findSubtype(fieldName);
            int idx = -1;
            for (int i = 0; i < columnStatisticsArray.length; i++) {
                if (this.typeDescription.findSubtype(i) == subSchema) {
                    idx = i;
                    break;
                }
            }
            columnNameToIdx.put(fieldName, idx);
            idxToColumnName.put(idx, fieldName);
            statisticsMap.put(fieldName, columnStatisticsArray[idx]);
        }
    }

    @Override
    public void initColumnMetas(TableMeta tableMeta) {
        PolarDBXOrcSchema orcSchema = OrcMetaUtils.buildPolarDBXOrcSchema(tableMeta);
        this.columnMetas = new ArrayList<>();
        this.columnMetas.addAll(orcSchema.getColumnMetas());
        this.columnMetas.addAll(orcSchema.getRedundantColumnMetas());
        for (ColumnMeta columnMeta : columnMetas) {
            this.columnMetaMap.put(columnMeta.getName(), columnMeta);
        }
    }

    public TypeDescription getTypeDescription() {
        return typeDescription;
    }

    public Map<String, ColumnStatistics> getStatisticsMap() {
        return statisticsMap;
    }

    public OrcTail getOrcTail() {
        return orcTail;
    }

    /**
     * Load stripe column meta from oss file meta.
     */
    public Map<Long, StripeColumnMeta> getStripeColumnMetas(String columnName) {
        if (stripeColumnMetaMap == null) {
            synchronized (this) {
                if (stripeColumnMetaMap == null) {
                    Map<String, Map<Long, StripeColumnMeta>> stripeColumnMetaMapTmp = TreeMaps.caseInsensitiveMap();
                    long stamp = FileSystemManager.readLockWithTimeOut(engine);
                    // stripStatistic
                    List<StripeStatistics> stripeStatistics = null;
                    List<StripeInformation> stripeInformations = null;
                    try {
                        FileSystem fileSystem = FileSystemManager.getFileSystemGroup(this.engine).getMaster();
                        String orcPath = FileSystemUtils.buildUri(fileSystem, this.fileName);
                        URI ossFileUri = URI.create(orcPath);
                        // fetch file footer
                        try (Reader reader = OrcFile.createReader(
                            new Path(ossFileUri),
                            OrcFile.readerOptions(configuration).filesystem(fileSystem).orcTail(getOrcTail()))) {
                            stripeStatistics = reader.getStripeStatistics();
                            stripeInformations = reader.getStripes();
                        }
                    } catch (Throwable t) {
                        throw GeneralUtil.nestedException(t);
                    } finally {
                        FileSystemManager.unlockRead(engine, stamp);
                    }
                    if (stripeStatistics != null && stripeInformations != null) {
                        List<Map<String, ColumnStatistics>> list = stripeStatistics.stream().map(x -> {
                            ColumnStatistics[] columnStatistics = x.getColumnStatistics();
                            Map<String, ColumnStatistics> m = TreeMaps.caseInsensitiveMap();
                            for (int idx = 0; idx < columnStatistics.length; idx++) {
                                if (!idxToColumnName.containsKey(idx)) {
                                    continue;
                                }
                                m.put(idxToColumnName.get(idx), columnStatistics[idx]);
                            }
                            return m;
                        }).collect(Collectors.toList());
                        Map<Integer, StripeInfo> stripeInfoMap = new HashMap<>();
                        for (int stripeIndex = 0; stripeIndex < stripeInformations.size(); stripeIndex++) {
                            StripeInformation stripeInformation = stripeInformations.get(stripeIndex);
                            stripeInfoMap.put(stripeIndex, new StripeInfo(stripeIndex, stripeInformation.getOffset(),
                                stripeInformation.getLength()));
                        }
                        for (ColumnMeta columnMeta : columnMetas) {
                            Map<Long, StripeColumnMeta> map = new HashMap<>();
                            for (int i = 0; i < list.size(); i++) {
                                StripeColumnMeta stripeColumnMeta = new StripeColumnMeta();
                                stripeColumnMeta.setStripeInfo(stripeInfoMap.get(i));
                                stripeColumnMeta.setColumnStatistics(list.get(i).get(columnMeta.getName()));
                                map.put((long) i, stripeColumnMeta);
                            }
                            stripeColumnMetaMapTmp.put(columnMeta.getName(), map);
                        }
                        try (Connection connection = MetaDbUtil.getConnection()) {
                            for (ColumnMeta columnMeta : columnMetas) {
                                Map<Long, StripeColumnMeta> map = stripeColumnMetaMapTmp.get(columnMeta.getName());
                                ColumnMetaAccessor accessor = new ColumnMetaAccessor();
                                accessor.setConnection(connection);
                                List<ColumnMetasRecord> records = accessor.query(fileName, columnMeta.getName());
                                for (ColumnMetasRecord record : records) {
                                    StripeColumnMeta stripeColumnMeta = map.get(record.stripeIndex);
                                    OrcBloomFilter bloomFilter = StripeColumnMeta.parseBloomFilter(record);
                                    stripeColumnMeta.setBloomFilter(bloomFilter);
                                }
                            }
                        } catch (SQLException e) {
                            throw GeneralUtil.nestedException(e);
                        }
                    }
                    // make it visible
                    this.stripeColumnMetaMap = stripeColumnMetaMapTmp;
                }
            }
        }
        return stripeColumnMetaMap.get(columnName);
    }

    public String getCreateTime() {
        return createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public Long getCommitTs() {
        return commitTs;
    }

    public Long getRemoveTs() {
        return removeTs;
    }

    @Override
    public String toString() {
        return "OSSOrcFileMeta{" +
            "typeDescription=" + typeDescription +
            ", statisticsMap=" + statisticsMap +
            ", orcTail=" + orcTail +
            ", logicalTableSchema='" + logicalTableSchema + '\'' +
            ", logicalTableName='" + logicalTableName + '\'' +
            ", physicalTableSchema='" + physicalTableSchema + '\'' +
            ", physicalTableName='" + physicalTableName + '\'' +
            ", fileName='" + fileName + '\'' +
            ", fileSize=" + fileSize +
            ", tableRows=" + tableRows +
            '}';
    }
}