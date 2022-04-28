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

package com.alibaba.polardbx.common.oss.filesystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by {@link OSSFileSystem} as an task that submitted
 * to the thread pool to accelerate the copy progress.
 * Each OSSCopyFileTask copies one file from src path to dst path
 */
public class OSSCopyFileTask implements Runnable {
    public static final Logger LOG =
        LoggerFactory.getLogger(OSSCopyFileTask.class);

    private OSSFileSystemStore store;
    private String srcKey;
    private long srcLen;
    private String dstKey;
    private OSSCopyFileContext copyFileContext;

    public OSSCopyFileTask(OSSFileSystemStore store,
                           String srcKey, long srcLen,
                           String dstKey, OSSCopyFileContext copyFileContext) {
        this.store = store;
        this.srcKey = srcKey;
        this.srcLen = srcLen;
        this.dstKey = dstKey;
        this.copyFileContext = copyFileContext;
    }

    @Override
    public void run() {
        boolean fail = false;
        try {
            fail = !store.copyFile(srcKey, srcLen, dstKey);
        } catch (Exception e) {
            LOG.warn("Exception thrown when copy from "
                + srcKey + " to " + dstKey +  ", exception: " + e);
            fail = true;
        } finally {
            copyFileContext.lock();
            if (fail) {
                copyFileContext.setCopyFailure(fail);
            }
            copyFileContext.incCopiesFinish();
            copyFileContext.signalAll();
            copyFileContext.unlock();
        }
    }
}

