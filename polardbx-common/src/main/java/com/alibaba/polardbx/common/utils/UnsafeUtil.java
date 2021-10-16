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

package com.alibaba.polardbx.common.utils;

import java.lang.reflect.Field;

public class UnsafeUtil {
    public static final sun.misc.Unsafe UNSAFE = getUnsafe();

    private static sun.misc.Unsafe getUnsafe() {
        try {
            Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (sun.misc.Unsafe) unsafeField.get(null);
        } catch (SecurityException e) {
            throw new RuntimeException(
                "Could not access the sun.misc.Unsafe handle, permission denied by security manager.", e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("The static handle field in sun.misc.Unsafe was not found.");
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Bug: Illegal argument reflection access for static field.", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Access to sun.misc.Unsafe is forbidden by the runtime.", e);
        } catch (Throwable t) {
            throw new RuntimeException("Unclassified error while trying to access the sun.misc.Unsafe handle.", t);
        }
    }

}
