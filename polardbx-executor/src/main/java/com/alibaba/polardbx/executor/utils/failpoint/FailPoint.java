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

package com.alibaba.polardbx.executor.utils.failpoint;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Joiner;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_ASSERT;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_RANDOM_CRASH;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_RANDOM_FAIL;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_RANDOM_HANG;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_RANDOM_SUSPEND;

/**
 * notice: only works in java assert mode
 * notice: all keys must start with "fp_"
 * <p>
 * <p>
 * useage:
 * <p>
 * -- inject fail point
 * set @fp_random_fail='17';
 * set @fp_random_suspend='17,3000';
 * set @fp_random_hang='17';
 * set @fp_random_crash='17';
 * select @fp_show;
 * <p>
 * -- remove fail point
 * set @fp_random_crash=null;
 * select @fp_show;
 * <p>
 * set @fp_clear=true;
 * select @fp_show;
 */
public class FailPoint {

    public static void inject(String key, Runnable runnable) {
        try {
            assert
                !isKeyEnable(key)
                : supply(runnable);
        } catch (AssertionError e) {
            //ignore
        }
    }

    public static void inject(String key, ExecutionContext executionContext, Runnable runnable) {
        try {
            assert
                !(isKeyEnable(key) && isKeyEnableFromHint(key, executionContext))
                : supply(runnable);
        } catch (AssertionError e) {
            //ignore
        }
    }

    public static void inject(String key, BiConsumer<String, String> consumer) {
        try {
            assert
                !isKeyEnable(key)
                : supply(key, getFromKeyMap(key), consumer);
        } catch (AssertionError e) {
            //ignore
        }
    }

    public static void inject(Runnable runnable) {
        try {
            assert
                false
                : supply(runnable);
        } catch (AssertionError e) {
            //ignore
        }
    }

    public static void injectFromHint(String key, ExecutionContext executionContext, Runnable runnable) {
        try {
            assert
                !(isKeyEnable(key) || isKeyEnableFromHint(key, executionContext))
                : supply(runnable);
        } catch (AssertionError e) {
            //ignore
        }
    }

    public static void injectFromHint(String key, ExecutionContext executionContext,
                                      BiConsumer<String, String> consumer) {
        try {
            assert
                !(isKeyEnable(key) || isKeyEnableFromHint(key, executionContext))
                : supply(
                key,
                isKeyEnableFromHint(key, executionContext) ? getValueFromHint(key, executionContext).get() :
                    getFromKeyMap(key),
                consumer);
        } catch (AssertionError e) {
            //ignore
        }
    }

    /**************************************************************************************************/

    /**
     * set @fp_random_fail='17';
     * means 17% to fail
     */
    public static void injectRandomException() {
        injectRandomException(FP_RANDOM_FAIL);
    }

    /**
     * set @fp_random_fail='17';
     * means 17% to fail
     */
    public static void injectRandomExceptionFromHint(ExecutionContext executionContext) {
        injectRandomExceptionFromHint(FP_RANDOM_FAIL, executionContext);
    }

    public static void injectRandomException(String key) {
        inject(key, (k, v) -> {
            int percentage = Integer.valueOf(v);
            int val = RandomUtils.nextInt(100);
            if (val <= percentage) {
                throw new RuntimeException("injected failure from " + key);
            }
        });
    }

    public static void injectRandomExceptionFromHint(String key, ExecutionContext executionContext) {
        injectFromHint(key, executionContext, (k, v) -> {
            int percentage = Integer.valueOf(v);
            int val = RandomUtils.nextInt(100);
            if (val <= percentage) {
                throw new RuntimeException("injected failure from " + key);
            }
        });
    }

    /**
     * set @fp_random_suspend='17,3000';
     * means 17% to suspend 3000 ms
     */
    public static void injectRandomSuspendFromHint(ExecutionContext executionContext) {
        injectFromHint(FP_RANDOM_SUSPEND, executionContext, (k, v) -> {
            String[] pair = v.replace(" ", "").split(",");
            int percentage = Integer.valueOf(pair[0]);
            int duration = Integer.valueOf(pair[1]);
            if (RandomUtils.nextInt(100) <= percentage) {
                try {
                    Thread.sleep(duration);
                } catch (Exception ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    /**
     * set @key='3000';
     * means suspend 3000 ms
     */
    public static void injectSuspend(String key) {
        inject(key, (k, v) -> {
            int duration = Integer.valueOf(v);
            try {
                Thread.sleep(duration);
            } catch (Exception ignored) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public static void injectException(String key) {
        inject(key, () -> {
            throw new RuntimeException("injected failure from " + key);
        });
    }

    public static void injectExceptionFromHint(String key, ExecutionContext executionContext) {
        injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected failure from " + key);
        });
    }

    public static void injectExceptionFromHintWithKeyEnable(String key, ExecutionContext executionContext) {
        inject(key, executionContext, () -> {
            throw new RuntimeException("injected failure from " + key);
        });
    }

    public static void throwException() {
        throw new RuntimeException("injected failure");
    }

    public static void throwException(String errMsg) {
        throw new RuntimeException(errMsg);
    }

    public static void injectCrash(String key) {
        inject(key, () -> {
            try {
                Runtime.getRuntime().halt(-1);
            } catch (Exception ignored) {
            }
        });
    }

    /**
     * set @key='3000';
     * means suspend 3000 ms
     */
    public static void injectSuspendFromHint(String key, ExecutionContext executionContext) {
        injectFromHint(key, executionContext, (k, v) -> {
            int duration = Integer.valueOf(v);
            try {
                Thread.sleep(duration);
            } catch (Exception ignored) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * set @fp_random_hang='17';
     * means 17% to hang
     */
    public static void injectRandomHang() {
        inject(FP_RANDOM_HANG, (k, v) -> {
            int percentage = Integer.valueOf(v);
            if (RandomUtils.nextInt(100) <= percentage) {
                while (true) {
                    // Infinite loop
                }
            }
        });
    }

    /**
     * set @fp_random_crash='17';
     * means 17% to crash
     */
    public static void injectRandomCrash() {
        inject(FP_RANDOM_CRASH, (k, v) -> {
            int percentage = Integer.valueOf(v);
            if (RandomUtils.nextInt(100) <= percentage) {
                try {
                    Runtime.getRuntime().halt(-1);
                } catch (Exception ignored) {
                }
            }
        });
    }

    public static void injectRandomEvent(String key, BiConsumer<String, String> consumer) {
        inject(key, (k, v) -> {
            int percentage = Integer.valueOf(v);
            if (RandomUtils.nextInt(100) <= percentage) {
                consumer.accept(k, v);
            }
        });
    }

    public static void assertTrue(Supplier<Boolean> predicate) {
        inject(FP_ASSERT, () -> {
            if (predicate.get() != true) {
                throw new RuntimeException("assert failed. expect true, actual false");
            }
        });
    }

    public static void assertFalse(Supplier<Boolean> predicate) {
        inject(FP_ASSERT, () -> {
            if (predicate.get() != false) {
                throw new RuntimeException("assert failed. expect false, actual true");
            }
        });
    }

    public static void assertNotNull(Object object) {
        inject(FP_ASSERT, () -> {
            if (object == null) {
                throw new RuntimeException("assert failed. expect false, actual true");
            }
        });
    }

    /**************************************************************************************************/

    public static final String SET_PREFIX = "fp_";
    public static final String FP_CLEAR = "fp_clear";
    public static final String FP_SHOW = "fp_show";

    private static final Map<String, String> keyMap = new ConcurrentHashMap<>(1024);

    static {
        enable(FP_ASSERT, "true");
    }

    public static void enable(String key, String value) {
        key = StringUtils.lowerCase(key);
        keyMap.put(key, value);
    }

    public static void disable(String key) {
        key = StringUtils.lowerCase(key);
        keyMap.remove(key);
    }

    public static boolean isKeyEnable(String key) {
        if (!isAssertEnable()) {
            return false;
        }
        key = StringUtils.lowerCase(key);
        if (keyMap.containsKey(key)) {
            return true;
        }
        return false;
    }

    public static void clear() {
        keyMap.clear();
    }

    public static synchronized void clearByPrefix(String prefix) {
        if (StringUtils.isBlank(prefix)) {
            return;
        }
        Set<String> toBeRemove = new HashSet<>();
        for (Map.Entry<String, String> e : keyMap.entrySet()) {
            if (StringUtils.startsWithIgnoreCase(e.getKey(), prefix)) {
                toBeRemove.add(e.getKey());
            }
        }
        for (String key : toBeRemove) {
            key = StringUtils.lowerCase(key);
            keyMap.remove(key);
        }
    }

    public static String show() {
        List<String> kvStrList = new ArrayList<>();
        keyMap.forEach((k, v) -> {
            kvStrList.add(k + "=" + v);
        });
        return Joiner.on(",").join(kvStrList);
    }

    /**
     * see jvm option: -ea
     * true: means assert is on.
     * false: means assert is off.
     */
    public static boolean isAssertEnable() {
        try {
            assert false;
        } catch (AssertionError e) {
            return true;
        }
        return false;
    }

    private static <T> Void supply(String key, String value, BiConsumer<String, String> consumer) {
        consumer.accept(key, value);
        return null;
    }

    private static <T> Void supply(Runnable runnable) {
        runnable.run();
        return null;
    }

    private static boolean isKeyEnableFromHint(String key, ExecutionContext executionContext) {
        if (!isAssertEnable()) {
            return false;
        }
        Optional<String> valueOption = getValueFromHint(key, executionContext);
        return valueOption.isPresent();
    }

    private static Optional<String> getValueFromHint(String key, ExecutionContext executionContext) {
        if (key == null || executionContext == null) {
            return Optional.empty();
        }
        Object value = executionContext.getExtraCmds().get(key);
        return value == null ? Optional.empty() : Optional.of(String.valueOf(value));
    }

    private static String getFromKeyMap(String key) {
        return keyMap.get(StringUtils.lowerCase(key));
    }

}