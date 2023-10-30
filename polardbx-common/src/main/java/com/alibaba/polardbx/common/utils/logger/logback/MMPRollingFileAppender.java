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

package com.alibaba.polardbx.common.utils.logger.logback;

import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.util.FileUtil;

import java.io.File;
import java.io.IOException;

public class MMPRollingFileAppender<E> extends RollingFileAppender<E> {

    @Override
    public void openFile(String file_name) throws IOException {
        this.lock.lock();

        try {
            File file = new File(file_name);

            boolean result = FileUtil.createMissingParentDirectories(file);
            if (!result) {
                this.addError("Failed to create parent directories for [" + file.getAbsolutePath() + "]");
            }

            ResilientMMPFileOutputStream resilientFos = new ResilientMMPFileOutputStream(file, true);
            resilientFos.setContext(this.context);
            this.setOutputStream(resilientFos);
        } finally {
            this.lock.unlock();
        }

    }

    @Override
    protected void subAppend(E event) {
        super.subAppend(event);
    }

}

