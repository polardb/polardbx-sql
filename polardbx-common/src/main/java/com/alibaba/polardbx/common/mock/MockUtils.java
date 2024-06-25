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

package com.alibaba.polardbx.common.mock;

import java.lang.reflect.Field;

public class MockUtils {

    public interface ThrowableRunnable {
        void run() throws Throwable;
    }

    public interface ThrowableConsumer<T> {
        void accept(T t) throws Throwable;
    }

    public static <T extends Throwable> T assertThrows(Class<T> expectedType,
                                                       String expectedMessage,
                                                       ThrowableRunnable methodCall) {
        try {
            methodCall.run();
        } catch (Throwable e) {
            if (expectedType.isInstance(e)) {
                if (expectedMessage != null) {
                    if (!expectedMessage.equals(e.getMessage())) {
                        throw new AssertionError(
                            "Expected message: " + expectedMessage + ", actual: " + e.getMessage()
                        );
                    }
                }
                return (T) e;
            }
            throw new AssertionError("Expected exception: " + expectedType.getName(), e);
        }
        throw new AssertionError("Expected exception: " + expectedType.getName());
    }

    public static void setInternalState(Object target, String fieldName, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
