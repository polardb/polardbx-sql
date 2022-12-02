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

package com.alibaba.polardbx.executor.archive.writer;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * check whether the backfill task should be paused
 */
public class OSSBackFillTimer {

    /**
     * used to the test break point
     */
    private final Optional<AtomicLong> counter;

    /**
     * true if any file is flushed to oss and committed
     */
    private final Optional<AtomicBoolean> flushed;

    private String sourcePhySchema;

    private String sourcePhyTable;

    OSSBackFillTimer(String sourcePhySchema, String sourcePhyTable, ExecutionContext ec) {
        this.sourcePhySchema = sourcePhySchema;
        this.sourcePhyTable = sourcePhyTable;
        if (ec.getParamManager().getBoolean(ConnectionParams.ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE)) {
            counter = Optional.of(new AtomicLong(0));
            flushed = Optional.of(new AtomicBoolean(false));
        } else {
            counter = Optional.empty();
            flushed = Optional.empty();
        }
    }

    public void setFlushed() {
        counter.ifPresent(AtomicLong::incrementAndGet);
        flushed.ifPresent(a -> a.set(true));
    }

    /**
     * simulate pause ddl use hint ENABLE_EXPIRE_FILE_STORE_TEST_PAUSE=true
     *
     * @param ec the context need to pause ddl
     */
    public void checkTime(ExecutionContext ec) {
        // some batch have been flushed to oss
        if (counter.filter(a -> a.incrementAndGet() > 3).isPresent()) {
            // don't pause until any file is committed
            if (flushed.filter(AtomicBoolean::get).isPresent()) {
                pauseDDL(ec);
                throw new TddlRuntimeException(ErrorCode.ERR_BACK_FILL_TIMEOUT);
            }
        }
    }

    public static void pauseDDL(ExecutionContext ec) {
        final DdlEngineSchedulerManager schedulerManager = new DdlEngineSchedulerManager();
        if (schedulerManager.tryPauseDdl(
            ec.getDdlJobId(),
            DdlState.RUNNING,
            DdlState.PAUSED)) {
            ec.getDdlContext().setInterruptedAsTrue();
        }
    }
}
