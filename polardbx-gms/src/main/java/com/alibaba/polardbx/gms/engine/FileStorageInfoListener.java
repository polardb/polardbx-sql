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

package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.gms.listener.ConfigListener;

import java.util.HashMap;
import java.util.Map;

public class FileStorageInfoListener implements ConfigListener {
    @Override
    public void onHandleConfig(String dataId, long newOpVersion) {
        Map<Engine, FileSystemGroup> fsGroupMap = new HashMap<>();
        for (Engine engine : Engine.values()) {
            fsGroupMap.put(engine, FileSystemManager.getFileSystemGroup(engine, false));
        }
        FileSystemManager.invalidFileSystem();
        for (Engine engine : Engine.values()) {
            FileSystemGroup newFsGroup = FileSystemManager.getFileSystemGroup(engine, false);
            // drop file storage
            if (fsGroupMap.get(engine) != null && newFsGroup == null) {
                EventLogger.log(EventType.CLOSE_OSS, String.format("engine %s is close.", engine));
            }
            // create file storage
            if (fsGroupMap.get(engine) == null && newFsGroup != null) {
                EventLogger.log(EventType.INIT_OSS, String.format("engine %s is initialized.", engine));
            }
        }
    }
}

