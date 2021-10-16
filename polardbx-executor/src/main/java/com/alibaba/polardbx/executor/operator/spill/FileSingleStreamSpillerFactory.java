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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.operator.spill;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeFactory;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.spill.LocalSpillMonitor;
import org.apache.calcite.sql.OutFileParams;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_OUT_OF_SPILL_SPACE;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.getFileStore;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class FileSingleStreamSpillerFactory implements SingleStreamSpillerFactory {
    private static final Logger log = LoggerFactory.getLogger(FileSingleStreamSpillerFactory.class);

    private final ListeningExecutorService executor;
    private final FileCleaner fileCleaner;
    private final PagesSerdeFactory serdeFactory;
    private final List<Path> spillPaths;
    private final double maxUsedSpaceThreshold;
    private int roundRobinIndex;

    public FileSingleStreamSpillerFactory(
        FileCleaner fileCleaner) {
        this(
            listeningDecorator(newFixedThreadPool(
                MppConfig.getInstance().getMaxSpillThreads(),
                Threads.daemonThreadsNamed("binary-spiller"))),
            fileCleaner,
            MppConfig.getInstance().getSpillPaths(),
            MppConfig.getInstance().getAvaliableSpillSpaceThreshold());
    }

    @VisibleForTesting
    public FileSingleStreamSpillerFactory(
        ListeningExecutorService executor,
        List<Path> spillPaths,
        double maxUsedSpaceThreshold) {
        this(
            executor,
            new SyncFileCleaner(),
            spillPaths,
            maxUsedSpaceThreshold);
    }

    public FileSingleStreamSpillerFactory(
        ListeningExecutorService executor,
        FileCleaner fileCleaner,
        List<Path> spillPaths,
        double maxUsedSpaceThreshold) {
        log.info("load FileSingleStreamSpillerFactory as SingleStreamSpillerFactory");
        this.serdeFactory = new PagesSerdeFactory(false);
        this.executor = requireNonNull(executor, "executor is null");
        this.fileCleaner = requireNonNull(fileCleaner, "fileCleaner is null");
        requireNonNull(spillPaths, "spillPaths is null");
        Preconditions.checkArgument(spillPaths.size() > 0, "spillPaths is empty");
        this.spillPaths = ImmutableList.copyOf(spillPaths);
        spillPaths.forEach(path -> {
            try {
                createDirectories(path);
            } catch (IOException e) {
                throw new IllegalArgumentException(
                    format(
                        "could not create spill path %s; adjust spill.spiller-spill-path config property or filesystem permissions",
                        path), e);
            }
            if (!path.toFile().canWrite()) {
                throw new IllegalArgumentException(
                    format(
                        "spill path %s is not writable; adjust spill.spiller-spill-path config property or filesystem permissions",
                        path));
            }
        });
        this.maxUsedSpaceThreshold = requireNonNull(maxUsedSpaceThreshold, "maxUsedSpaceThreshold can not be null");
        this.roundRobinIndex = 0;
    }

    @PostConstruct
    public SingleStreamSpillerFactory cleanupOldSpillFiles() {
        spillPaths.forEach(FileSingleStreamSpillerFactory::cleanupOldSpillFiles);
        return this;
    }

    private static void cleanupOldSpillFiles(Path path) {
        try (DirectoryStream<Path> stream = newDirectoryStream(path, SPILL_FILE_GLOB)) {
            stream.forEach(spillFile -> {
                try {
                    log.info("Deleting old spill file: " + spillFile);
                    // FIXME, do it async
                    delete(spillFile);
                } catch (Exception e) {
                    log.warn("Could not cleanup old spill file: " + spillFile);
                }
            });
        } catch (IOException e) {
            log.warn("Error cleaning spill files", e);
        }
    }

    @Override
    public SingleStreamSpiller create(String filePrefix, List<DataType> types, LocalSpillMonitor spillMonitor,
                                      OutFileParams params) {
        if (params != null) {
            throw new UnsupportedOperationException();
        }
        return new FileSingleStreamSpiller(serdeFactory.createPagesSerde(types), executor, fileCleaner,
            getNextSpillPath(), spillMonitor);
    }

    private synchronized Path getNextSpillPath() {
        int spillPathsCount = spillPaths.size();
        for (int i = 0; i < spillPathsCount; ++i) {
            int pathIndex = (roundRobinIndex + i) % spillPathsCount;
            Path path = spillPaths.get(pathIndex);
            if (hasEnoughDiskSpace(path)) {
                roundRobinIndex = (roundRobinIndex + i + 1) % spillPathsCount;
                return path;
            }
        }
        throw new TddlRuntimeException(ERR_OUT_OF_SPILL_SPACE, "No free space available for spill");
    }

    private boolean hasEnoughDiskSpace(Path path) {
        try {
            FileStore fileStore = getFileStore(path);
            return fileStore.getUsableSpace() > fileStore.getTotalSpace() * (1.0 - maxUsedSpaceThreshold);
        } catch (IOException e) {
            throw new TddlRuntimeException(ERR_OUT_OF_SPILL_SPACE, e, "Cannot determine free space for spill");
        }
    }
}
