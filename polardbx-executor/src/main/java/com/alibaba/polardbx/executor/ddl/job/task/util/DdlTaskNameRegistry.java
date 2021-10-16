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

package com.alibaba.polardbx.executor.ddl.job.task.util;

import com.alibaba.polardbx.common.utils.ClassFinder;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.SerializableClassMapper;
import com.google.common.collect.Lists;

import java.lang.reflect.Modifier;
import java.util.List;

public class DdlTaskNameRegistry {

    public static void registerTaskNames() {
        List<Class> classes = Lists.newArrayList();

        ClassFinder.ClassFilter filter = new ClassFinder.ClassFilter() {
            @Override
            public boolean filter(Class klass) {
                int mod = klass.getModifiers();
                return !Modifier.isAbstract(mod)
                    && !Modifier.isInterface(mod)
                    && DdlTask.class.isAssignableFrom(klass)
                    && klass.getAnnotation(TaskName.class) != null;
            }

            @Override
            public boolean preFilter(String classFulName) {
                return true;
            }
        };

        classes.addAll(ClassFinder.findClassesInPackage("com.alibaba.polardbx.executor.ddl", filter));
        classes.addAll(ClassFinder.findClassesInPackage("com.alibaba.polardbx.executor.balancer.action", filter));

        for (@SuppressWarnings("unchecked") Class<? extends DdlTask> klass : classes) {
            TaskName taskName = klass.getAnnotation(TaskName.class);
            SerializableClassMapper.register(taskName.name(), klass);
        }
    }

}
