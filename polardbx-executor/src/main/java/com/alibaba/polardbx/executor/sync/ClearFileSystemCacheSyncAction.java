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
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheConfig;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import org.apache.hadoop.fs.FileSystem;
import org.jetbrains.annotations.Nullable;

/**
 * @author chenzilin
 */
public class ClearFileSystemCacheSyncAction implements ISyncAction {

    private boolean all;
    public Engine engine;

    public ClearFileSystemCacheSyncAction() {
    }

    public ClearFileSystemCacheSyncAction(@Nullable Engine engine, boolean all) {
        this.engine = engine;
        this.all = all;
    }

    @Override
    public ResultCursor sync() {

        if (all) {
            clearAllCache();
            // also clear in memory block cache.
            BlockCacheManager.getInstance().clear();
        } else {
            clearCache(FileSystemManager.getFileSystemGroup(engine));
        }
        return null;
    }

    private void clearAllCache() {
        for (Engine engine : Engine.values()) {
            if (Engine.hasCache(engine)) {
                // If the engine does not exist, just skip
                clearCache(FileSystemManager.getFileSystemGroup(engine, false));
            }
        }
    }

    private void clearCache(FileSystemGroup fileSystemGroup) {
        if (fileSystemGroup != null) {
            FileMergeCacheConfig fileMergeCacheConfig = FileConfig.getInstance().getMergeCacheConfig();
            // rebuild cache by new configs.
            ((FileMergeCachingFileSystem) fileSystemGroup.getMaster()).getCacheManager()
                .rebuildCache(fileMergeCacheConfig);
            for (FileSystem slave : fileSystemGroup.getSlaves()) {
                ((FileMergeCachingFileSystem) slave).getCacheManager().rebuildCache(fileMergeCacheConfig);
            }
        }
    }

    public boolean isAll() {
        return all;
    }

    public void setAll(boolean all) {
        this.all = all;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }
}

