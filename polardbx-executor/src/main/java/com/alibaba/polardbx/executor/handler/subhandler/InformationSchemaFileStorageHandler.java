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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaFileStorage;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.hadoop.fs.FileSystem;

public class InformationSchemaFileStorageHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaFileStorageHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaFileStorage;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        for (Engine engine : Engine.values()) {
            if (!Engine.isFileStore(engine)) {
                continue;
            }
            FileSystemGroup group = FileSystemManager.getFileSystemGroup(engine, false);
            if (group != null) {
                cursor.addRow(new Object[] {
                    group.getMaster().getWorkingDirectory(),
                    engine,
                    "MASTER",
                    FileSystemManager.getReadLockCount(engine),
                    FileSystemManager.isWriteLocked(engine) ? 1 : 0
                });
                for (FileSystem slave : group.getSlaves()) {
                    cursor.addRow(new Object[] {
                        slave.getWorkingDirectory(),
                        engine,
                        "SLAVE",
                        FileSystemManager.getReadLockCount(engine),
                        FileSystemManager.isWriteLocked(engine) ? 1 : 0
                    });
                }
            }
        }
        return cursor;
    }
}
