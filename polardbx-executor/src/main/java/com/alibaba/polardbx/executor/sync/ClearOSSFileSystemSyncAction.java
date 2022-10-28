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
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.reader.BufferPoolManager;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import com.alibaba.polardbx.gms.topology.InstConfigRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.hadoop.fs.FileSystem;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

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
            Map<String, Long> configs = fetchConfig();

            if (configs == null || configs.isEmpty()) {
                // just clear
                ((FileMergeCachingFileSystem) fileSystemGroup.getMaster()).getCacheManager().clear();
                for (FileSystem slave : fileSystemGroup.getSlaves()) {
                    ((FileMergeCachingFileSystem) slave).getCacheManager().clear();
                }
            } else {
                // rebuild cache by new configs.
                ((FileMergeCachingFileSystem) fileSystemGroup.getMaster()).getCacheManager().rebuildCache(configs);
                for (FileSystem slave : fileSystemGroup.getSlaves()) {
                    ((FileMergeCachingFileSystem) slave).getCacheManager().rebuildCache(configs);
                }
            }
        }
        return null;
    }

    private Map<String, Long> fetchConfig() {
        Map<String, Long> results = new HashMap<>();
        try (Connection connection = MetaDbUtil.getConnection()) {
            InstConfigAccessor accessor = new InstConfigAccessor();
            accessor.setConnection(connection);

            doFetch(results, ConnectionProperties.OSS_FS_CACHE_TTL, accessor);
            doFetch(results, ConnectionProperties.OSS_FS_MAX_CACHED_ENTRIES, accessor);
        } catch (SQLException e) {
            GeneralUtil.nestedException(e);
        }
        return results;
    }

    private void doFetch(Map<String, Long> results, String connProp, InstConfigAccessor accessor) {
        List<InstConfigRecord> records = accessor.queryByParamKey(InstIdUtil.getInstId(), connProp);
        Long result;
        if (records != null
            && !records.isEmpty()
            && (result = DataTypes.LongType.convertFrom(records.get(0).paramVal)) != null) {
            results.put(connProp, result);
        }
    }
}

