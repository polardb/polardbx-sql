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

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.util.Iterator;

public class ORCReadResult {
    private VectorizedRowBatch rowBatch;
    private DataType[] dataTypeList;

    private Chunk chunk;
    private long resultRows;
    private FileSystem fileSystem;
    private long time;

    // todo
    private long currentRowPosition;
    private long restRows;

    public ORCReadResult(String orcKey, long time, long resultRows, FileSystem fileSystem) {
        this.orcKey = orcKey;
        this.time = time;
        this.resultRows = resultRows;
        this.fileSystem = fileSystem;
        this.chunk = null;
    }

    public ORCReadResult(String orcKey, long time, long resultRows, FileSystem fileSystem, Chunk chunk) {
        this(orcKey, time, resultRows, fileSystem);
        this.chunk = chunk;
    }

    private String orcKey;

    public long getTime() {
        return time;
    }

    public long getResultRows() {
        return resultRows;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public VectorizedRowBatch getRowBatch() {
        return rowBatch;
    }

    public DataType[] getDataTypeList() {
        return dataTypeList;
    }

    public String getOrcKey() {
        return orcKey;
    }

    public Chunk getChunk() {
        return chunk;
    }

    public boolean isStatistics() {
        return chunk != null;
    }

    @Override
    public String toString() {
        return String.format(
            "file = %s, rows = %s, time = %s(ms)",
            orcKey,
            resultRows,
            time
        );
    }
}

