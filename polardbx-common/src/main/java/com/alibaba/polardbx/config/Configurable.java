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

package com.alibaba.polardbx.config;

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public interface Configurable {

    default void config(Properties props) {
        Map<String, MethodHandle> handlers = getConfigHandlers();
        for (String key : handlers.keySet()) {
            if (props.containsKey(key)) {
                try {
                    handlers.get(key).invoke(this, props.getProperty(key));
                } catch (Throwable throwable) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SERVER, "Failed to update configuration " + key);
                }
            }
        }
    }

    Map<String, MethodHandle> getConfigHandlers();

    static Map<String, MethodHandle> parseConfigurableProps(Class<?> klass) {
        Preconditions.checkNotNull(klass, "Class can't be null!");
        Map<String, MethodHandle> handlers = new HashMap<>();
        MethodType mt = MethodType.methodType(void.class, String.class);
        MethodHandles.Lookup lookup = MethodHandles.publicLookup();
        for (Field field : klass.getDeclaredFields()) {
            Config config = field.getAnnotation(Config.class);
            if (config == null) {
                continue;
            }

            try {
                MethodHandle old = handlers.put(config.key(), lookup.findVirtual(klass, config.configMethod(), mt));
                if (old != null) {
                    throw new IllegalStateException(
                        "Duplicate config key: " + config.key() + " in " + klass.getSimpleName());
                }
            } catch (Throwable t) {
                throw new TddlRuntimeException(ErrorCode.ERR_SERVER, "Failed to parse config properties.", t);
            }
        }

        return handlers;
    }
}
