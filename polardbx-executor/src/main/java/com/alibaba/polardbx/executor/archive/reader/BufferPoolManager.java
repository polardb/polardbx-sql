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

package com.alibaba.polardbx.executor.archive.reader;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BufferPoolManager {
    private static volatile BufferPoolManager instance;

    private static int BUFFER_POOL_SIZE = 200000;

    private static int BUFFER_POOL_EXPIRE_HOURS = 24;

    public static BufferPoolManager getInstance() {
        if (instance == null) {
            synchronized (BufferPoolManager.class) {
                if (instance == null) {
                    instance = new BufferPoolManager();
                }
            }
        }
        return instance;
    }

    private Cache<Key, List<Block>> cache;

    private BufferPoolManager() {
        cache = CacheBuilder.newBuilder()
                .maximumSize(BUFFER_POOL_SIZE)
                .expireAfterWrite(BUFFER_POOL_EXPIRE_HOURS, TimeUnit.HOURS)
                .softValues()
                .build();
    }

    public void clear() {
        cache.invalidateAll();
    }

    public void invalidate(String schemaName, String logicalTableName) {
        for (Key key : cache.asMap().keySet()) {
            if (key.logicalSchemaName.equalsIgnoreCase(schemaName) && key.logicalTableName.equalsIgnoreCase(logicalTableName)) {
                cache.invalidate(key);
            }
        }
    }

    public static class Key {
        public String logicalSchemaName;
        public String logicalTableName;
        public String fileName;
        public String column;

        public Key(String logicalSchemaName, String logicalTableName, String fileName, String column) {
            this.logicalSchemaName = logicalSchemaName;
            this.logicalTableName = logicalTableName;
            this.fileName = fileName;
            this.column = column;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return logicalSchemaName.equals(key.logicalSchemaName)
                    && logicalTableName.equals(key.logicalTableName)
                    && fileName.equals(key.fileName)
                    && column.equals(key.column);
        }

        @Override
        public int hashCode() {
            return Objects.hash(logicalSchemaName, logicalTableName, fileName, column);
        }
    }

    public List<Block> getImpl(OSSOrcFileMeta fileMeta, String column, OSSReadOption ossReadOption,
                               ExecutionContext executionContext) throws ExecutionException {
        return cache.get(new Key(fileMeta.getLogicalTableSchema(), fileMeta.getLogicalTableName(), fileMeta.getFileName(), column), () -> {
            long stamp = FileSystemManager.readLockWithTimeOut(ossReadOption.getEngine());
            try {
                FileSystem fileSystem = FileSystemManager.getFileSystemGroup(ossReadOption.getEngine()).getMaster();

                String orcPath = FileSystemUtils.buildUri(fileSystem, fileMeta.getFileName());

                Configuration configuration = new Configuration(false);
                configuration.setLong(OrcConf.MAX_MERGE_DISTANCE.getAttribute(), ossReadOption.getMaxMergeDistance());

                Reader reader = OrcFile.createReader(new Path(URI.create(orcPath)),
                        OrcFile.readerOptions(configuration).filesystem(fileSystem).orcTail(fileMeta.getOrcTail()));

                ColumnMeta columnMeta = ossReadOption.getColumnMetas().stream()
                        .filter(x -> x.getName().equals(column)).findFirst().get();

                TypeDescription schema = TypeDescription.createStruct();

                schema.addField(
                        fileMeta.getTypeDescription().getFieldNames().get(fileMeta.getColumnMetas().indexOf(columnMeta)),
                        fileMeta.getTypeDescription().getChildren().get(fileMeta.getColumnMetas().indexOf(columnMeta)).clone());

                // reader filter options
                Reader.Options readerOptions = new Reader.Options(configuration)
                        .schema(schema);

                RecordReader recordReader = reader.rows(readerOptions);

                ColumnProvider columnProvider = ColumnProviders.getProvider(columnMeta);

                SessionProperties sessionProperties = SessionProperties.fromExecutionContext(executionContext);

                VectorizedRowBatch buffer = schema.createRowBatch(1000);

                List<Block> result = new ArrayList<>();

                while (recordReader.nextBatch(buffer)) {
                    if (buffer.size == 0) {
                        continue;
                    }
                    BlockBuilder blockBuilder = BlockBuilders.create(columnMeta.getDataType(), executionContext);
                    columnProvider.transform(buffer.cols[0], blockBuilder, 0, buffer.size, sessionProperties);
                    result.add(blockBuilder.build());
                }

                return result;
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            } finally {
                FileSystemManager.unlockRead(ossReadOption.getEngine(), stamp);
            }
        });
    }

    public List<Chunk> get(OSSOrcFileMeta fileMeta, String[] columns, OSSReadOption ossReadOption,
                           ExecutionContext executionContext) {
        try {
            List<Block>[] blockLists = new List[columns.length];
            for (int i = 0; i < columns.length; i++) {
                blockLists[i] = getImpl(fileMeta, columns[i], ossReadOption, executionContext);
            }
            List<Chunk> result = new ArrayList<>();
            for (int i = 0; i < blockLists[0].size(); i++) {
                Block[] blocks = new Block[columns.length];
                for (int j = 0; j < columns.length; j++) {
                    blocks[j] = blockLists[j].get(i);
                }
                result.add(new Chunk(blocks));
            }
            return result;
        } catch (ExecutionException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }
}
