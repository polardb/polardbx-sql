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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;

import java.io.File;
import java.util.List;

/**
 * delete file from local disk
 */
public class DeleteOssFileSyncAction implements ISyncAction {
    List<String> ossFiles;

    public DeleteOssFileSyncAction(List<String> ossFiles) {
        this.ossFiles = ossFiles;
    }

    public List<String> getOssFiles() {
        return ossFiles;
    }

    public void setOssFiles(List<String> ossFiles) {
        this.ossFiles = ossFiles;
    }

    @Override
    public ResultCursor sync() {
        for (String path : ossFiles) {
            File tmpFile = new File(path);
            if (tmpFile.exists()) {
                if (!tmpFile.delete()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_BACK_FILL_FAIL,
                        "can't delete file " + path);
                }
            }
        }
        return null;
    }
}
