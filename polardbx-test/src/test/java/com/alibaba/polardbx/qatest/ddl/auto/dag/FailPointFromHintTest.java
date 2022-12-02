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

package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

@Ignore
public class FailPointFromHintTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private ExecutionContext executionContext;

    @Before
    public void before() {
        this.executionContext = new ExecutionContext();
        Map<String, Object> hint = new HashMap<>();
        executionContext.setExtraCmds(hint);
    }

    @After
    public void after() {
        FailPoint.disable("key1");
        FailPoint.disable("key2");
    }

    private void addHint(String key, Object value) {
        this.executionContext.getExtraCmds().put(key, value);
    }

    private void removeHint(String key, Object value) {
        this.executionContext.getExtraCmds().put(key, value);
    }

    @Test
    public void testInject1() {

        String key = "key1";

        /**
         * haven't register any key, so no error will be thrown
         */
        FailPoint.injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected error message");
        });

        thrown.expect(RuntimeException.class);
        FailPoint.enable(key, "value");
        /**
         * since key1 registered, RuntimeException will be thrown
         */
        FailPoint.injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected error message");
        });
        /**
         * since key2 is not registered, IllegalArgumentException won't be thrown
         */
        FailPoint.injectFromHint("key2", executionContext, () -> {
            throw new IllegalArgumentException("injected error message");
        });

        /**
         * since key1 deRegistered, RuntimeException won't be thrown
         */
        FailPoint.disable(key);
        FailPoint.injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected error message");
        });

    }

    @Test
    public void testInject2() {

        String key = "key1";

        FailPoint.injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected error message");
        });

        FailPoint.enable("foobar", "value");
        FailPoint.injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected error message");
        });

        FailPoint.disable("foobar");
        FailPoint.injectFromHint("foobar", executionContext, () -> {
            throw new RuntimeException("injected error message");
        });
    }

    @Test
    public void testInject3() {

        String key = "key1";

        long start = System.currentTimeMillis();

        FailPoint.enable("key1", "value");
        FailPoint.injectFromHint(key, executionContext, () -> {
            try {
                Thread.sleep(3 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        long end = System.currentTimeMillis();

        Assert.assertTrue((end - start) > 2500l);
    }

}