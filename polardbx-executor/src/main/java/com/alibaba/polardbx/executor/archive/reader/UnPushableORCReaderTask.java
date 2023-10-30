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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.statistics.ExecuteSQLOperation;
import com.google.common.collect.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class UnPushableORCReaderTask {

    protected OSSReadOption ossReadOption;
    protected String tableFileName;

    protected RecordReader recordReader;

    protected FileSystem fileSystem;
    protected Reader reader;
    protected URI ossFileUri;

    protected OSSOrcFileMeta fileMeta;

    protected PruningResult pruningResult;

    protected ListIterator<Range<Long>> listIterator;

    protected Configuration configuration;

    protected final AtomicBoolean closed;

    protected volatile long stamp;

    protected ExecutionContext context;

    protected long startTime;

    protected long count;

    public UnPushableORCReaderTask(OSSReadOption ossReadOption, String tableFileName, FileMeta fileMeta,
                                   PruningResult pruningResult, ExecutionContext context) {
        this.ossReadOption = ossReadOption;
        this.tableFileName = tableFileName;
        this.closed = new AtomicBoolean(false);
        this.stamp = FileSystemManager.readLockWithTimeOut(ossReadOption.getEngine());
        this.fileSystem = FileSystemManager.getFileSystemGroup(ossReadOption.getEngine()).getMaster();
        String orcPath = FileSystemUtils.buildUri(this.fileSystem, tableFileName);
        this.ossFileUri = URI.create(orcPath);
        this.fileMeta = (OSSOrcFileMeta) fileMeta;
        this.pruningResult = pruningResult;
        this.configuration = new Configuration(false);
        this.configuration.setLong(OrcConf.MAX_MERGE_DISTANCE.getAttribute(), ossReadOption.getMaxMergeDistance());
        this.count = 0;
        this.context = context;
    }

    public void init() {
        try {
            startTime = System.nanoTime() / 1000_000;
            // fetch file footer
            this.reader = OrcFile.createReader(new Path(ossFileUri),
                OrcFile.readerOptions(configuration).filesystem(fileSystem).orcTail(fileMeta.getOrcTail()));

            // reader filter options
            Reader.Options readerOptions = createOption();
            if (pruningResult.pass()) {
                this.recordReader = reader.rows(readerOptions);
            }

            if (pruningResult.part()) {
                Iterator<Range<Long>> descendingIterator =
                    pruningResult.getRangeSet().asDescendingSetOfRanges().iterator();
                List<Range<Long>> rangeList = new ArrayList<>();
                while (descendingIterator.hasNext()) {
                    rangeList.add(descendingIterator.next());
                }

                // sequential access file
                listIterator = rangeList.listIterator(rangeList.size());
                Range<Long> range = listIterator.previous();
                readerOptions =
                    readerOptions.range(range.lowerEndpoint(), range.upperEndpoint() - range.lowerEndpoint());
                this.recordReader = reader.rows(readerOptions);
            }

        } catch (Throwable t) {
            close();
            throw GeneralUtil.nestedException(t);
        }
    }

    public ORCReadResult next(VectorizedRowBatch buffer, SessionProperties sessionProperties) {
        try {
            buffer.size = 0;
            long s = System.currentTimeMillis();
            long resultRows = 0;

            // Data in whole file or only one stripe
            if (this.recordReader.nextBatch(buffer)) {
                resultRows += buffer.size;
            }

            // If no data in this stripe, fetch the next.
            if (resultRows == 0 && pruningResult.part() && nextStripe()) {
                return next(buffer, sessionProperties);
            }

            count += resultRows;
            ORCReadResult readResult = new ORCReadResult(
                tableFileName,
                System.currentTimeMillis() - s,
                resultRows,
                fileSystem
            );
            return readResult;
        } catch (Throwable t) {
            close();
            throw GeneralUtil.nestedException(t);
        }
    }

    @NotNull
    private Block[] blocksFrom(BlockBuilder[] blockBuilders) {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        return blocks;
    }

    /**
     * get the next stripe in the file
     *
     * @return true if there is more stripe to read
     */
    protected boolean nextStripe() {
        // close
        closeRecordReader();
        if (listIterator.hasPrevious()) {
            Range<Long> range = listIterator.previous();
            // reader filter options
            Reader.Options readerOptions = createOption()
                .range(range.lowerEndpoint(), range.upperEndpoint() - range.lowerEndpoint());
            try {
                this.recordReader = reader.rows(readerOptions);
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }
            return true;
        }
        return false;
    }

    protected void closeRecordReader() {
        try {
            if (this.recordReader != null) {
                this.recordReader.close();
                this.recordReader = null;
            }
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    protected Reader.Options createOption() {
        return new Reader.Options(configuration)
            .schema(this.ossReadOption.getReadSchema())
            .searchArgument(
                ossReadOption.getSearchArgument(),
                ossReadOption.getColumns()
            );
    }

    public OSSReadOption getOssReadOption() {
        return ossReadOption;
    }

    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            if (context.isEnableTrace()) {
                long now = System.nanoTime() / 1000_000;
                ExecuteSQLOperation op = new ExecuteSQLOperation(
                    fileMeta.getPhysicalTableSchema(),
                    tableFileName,
                    "pruning result:" + pruningResult.toString() + " predicate :" + ossReadOption.getSearchArgument()
                        .toString(),
                    now);
                // Update trace time.
                op.setThreadName(Thread.currentThread().getName());
                op.setTimeCost(now - startTime);
                op.setGetConnectionTimeCost(0);
                op.setRowsCount(count);
                op.setTotalTimeCost(now - op.getTimestamp());
                op.setPhysicalCloseCost(0);
                context.getTracer().trace(op);
            }

            if (this.recordReader != null) {
                this.recordReader.close();
                this.recordReader = null;
            }
            if (this.reader != null) {
                this.reader.close();
                this.reader = null;
            }
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            FileSystemManager.unlockRead(ossReadOption.getEngine(), stamp);
        }
    }
}
