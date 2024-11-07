
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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.airlift.slice;

import sun.misc.Unsafe;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import static java.lang.invoke.MethodType.methodType;
import static sun.misc.Unsafe.ARRAY_BOOLEAN_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_DOUBLE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_FLOAT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

public final class JvmUtils {
    static final Unsafe unsafe;
    static final MethodHandle newByteBuffer;

    static final Field ADDRESS_ACCESSOR;
    static final MethodHandles.Lookup IMPL_LOOKUP;

    static volatile boolean CONSTRUCTOR_LOOKUP_ERROR;
    static volatile MethodHandle CONSTRUCTOR_LOOKUP;

    static {
        try {

            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }

            assertArrayIndexScale("Boolean", ARRAY_BOOLEAN_INDEX_SCALE, 1);
            assertArrayIndexScale("Byte", ARRAY_BYTE_INDEX_SCALE, 1);
            assertArrayIndexScale("Short", ARRAY_SHORT_INDEX_SCALE, 2);
            assertArrayIndexScale("Int", ARRAY_INT_INDEX_SCALE, 4);
            assertArrayIndexScale("Long", ARRAY_LONG_INDEX_SCALE, 8);
            assertArrayIndexScale("Float", ARRAY_FLOAT_INDEX_SCALE, 4);
            assertArrayIndexScale("Double", ARRAY_DOUBLE_INDEX_SCALE, 8);

            Class<?> directByteBufferClass = ClassLoader.getSystemClassLoader().loadClass("java.nio.DirectByteBuffer");
            Constructor<?> constructor =
                directByteBufferClass.getDeclaredConstructor(long.class, int.class, Object.class);
            constructor.setAccessible(true);
            newByteBuffer = MethodHandles.lookup().unreflectConstructor(constructor)
                .asType(methodType(ByteBuffer.class, long.class, int.class, Object.class));

            ADDRESS_ACCESSOR = Buffer.class.getDeclaredField("address");
            ADDRESS_ACCESSOR.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        MethodHandles.Lookup trustedLookup = null;
        try {
            Class lookupClass = MethodHandles.Lookup.class;
            Field implLookup = lookupClass.getDeclaredField("IMPL_LOOKUP");
            long fieldOffset = unsafe.staticFieldOffset(implLookup);
            trustedLookup = (MethodHandles.Lookup) unsafe.getObject(lookupClass, fieldOffset);
        } catch (Throwable ignored) {
            // ignored
        }
        if (trustedLookup == null) {
            trustedLookup = MethodHandles.lookup();
        }
        IMPL_LOOKUP = trustedLookup;
    }

    private static void assertArrayIndexScale(final String name, int actualIndexScale, int expectedIndexScale) {
        if (actualIndexScale != expectedIndexScale) {
            throw new IllegalStateException(
                name + " array index scale must be " + expectedIndexScale + ", but is " + actualIndexScale);
        }
    }

    public static long getAddress(Buffer buffer) {
        try {
            return (long) ADDRESS_ACCESSOR.get(buffer);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static MethodHandles.Lookup trustedLookup(Class objectClass) {
        if (!CONSTRUCTOR_LOOKUP_ERROR) {
            try {
                int TRUSTED = -1;

                MethodHandle constructor = CONSTRUCTOR_LOOKUP;
                if (constructor == null) {
                    constructor = IMPL_LOOKUP.findConstructor(
                        MethodHandles.Lookup.class,
                        methodType(void.class, Class.class, int.class)
                    );
                    CONSTRUCTOR_LOOKUP = constructor;
                }
                return (MethodHandles.Lookup) constructor.invoke(objectClass, TRUSTED);
            } catch (Throwable ignored) {
                CONSTRUCTOR_LOOKUP_ERROR = true;
            }
        }

        return IMPL_LOOKUP.in(objectClass);
    }

    private JvmUtils() {
    }
}
