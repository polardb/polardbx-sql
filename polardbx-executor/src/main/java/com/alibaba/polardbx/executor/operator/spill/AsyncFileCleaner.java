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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

public class AsyncFileCleaner implements FileCleaner {

    private static final Logger log = LoggerFactory.getLogger(AsyncFileCleaner.class);

    private BlockingQueue<FileHolder> cleanQueue = new LinkedBlockingDeque<>();

    @Inject
    public AsyncFileCleaner(@ForAsyncSpill ExecutorService asyncSpillCleanerExecutor) {
        asyncSpillCleanerExecutor.submit(this);
    }

    @Override
    public void run() {
        Thread.currentThread().setName("MPP-AsyncFileCleaner");
        log.info("start MPP-AsyncFileCleaner Thread");
        FileHolder toClean = null;
        while (true) {
            try {
                toClean = cleanQueue.take();
                toClean.doClean();
            } catch (InterruptedException e) {
                log.warn("FileCleaner got interrupted", e);
            } catch (Throwable e) {
                if (toClean != null) {
                    log.error("fail to clean file " + toClean.getFilePath(), e);
                } else {
                    log.error(e);
                }
            }
        }
    }

    @Override
    public void recycleFile(FileHolder toClean) {
        cleanQueue.add(toClean);
    }
}
