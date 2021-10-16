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

package com.alibaba.polardbx.executor.ddl.newengine.serializable;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.DdlTaskNameRegistry;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * for serialize/deserialize
 * register an unique name for subclass of DdlTask.
 */
public class SerializableClassMapper {

    protected static final Map<String, Class<? extends DdlTask>> name2Clazz = new ConcurrentHashMap<>();
    protected static final Map<Class<? extends DdlTask>, String> clazz2Name = new ConcurrentHashMap<>();

    static {
        try {
            DdlTaskNameRegistry.registerTaskNames();
        } catch (Exception e) {
            GeneralUtil.nestedException("Failed to register taskNames", e);
        }
    }

    public static synchronized void register(String uniqueName, Class<? extends DdlTask> taskClass) {
        if (name2Clazz.containsKey(uniqueName) || clazz2Name.containsKey(taskClass)) {
            throw new TddlNestableRuntimeException("duplicate taskName:" + uniqueName);
        }
        name2Clazz.put(uniqueName, taskClass);
        clazz2Name.put(taskClass, uniqueName);
    }

    public static Class<? extends DdlTask> getTaskClassByName(String uniqueTaskName) {
        return name2Clazz.get(uniqueTaskName);
    }

    public static String getNameByTaskClass(Class<? extends DdlTask> clazz) {
        return clazz2Name.get(clazz);
    }

    public static boolean containsClass(Class<? extends DdlTask> clazz) {
        return clazz2Name.containsKey(clazz);
    }

}