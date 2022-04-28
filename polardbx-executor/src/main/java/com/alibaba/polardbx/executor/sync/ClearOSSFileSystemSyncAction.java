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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.executor.archive.reader.BufferPoolManager;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import org.apache.hadoop.fs.FileSystem;

/**
 * @author chenzilin
 * @date 2022/2/21 11:29
 */
public class ClearOSSFileSystemSyncAction implements ISyncAction {

    public ClearOSSFileSystemSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(Engine.OSS);
        if (fileSystemGroup != null) {
            ((FileMergeCachingFileSystem) fileSystemGroup.getMaster()).getCacheManager().clear();
            for (FileSystem slave : fileSystemGroup.getSlaves()) {
                ((FileMergeCachingFileSystem) slave).getCacheManager().clear();
            }
        }
        return null;
    }
}

