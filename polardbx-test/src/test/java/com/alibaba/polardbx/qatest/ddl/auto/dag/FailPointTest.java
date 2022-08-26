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
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FailPointTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @After
    public void after() {
        FailPoint.disable("key1");
        FailPoint.disable("key2");
    }

    @Test
    public void testInject1() {

        String key = "key1";

        /**
         * haven't register any key, so no error will be thrown
         */
        FailPoint.inject(key, () -> {
            throw new RuntimeException("injected error message");
        });

        thrown.expect(RuntimeException.class);
        FailPoint.enable(key, "value");
        /**
         * since key1 registered, RuntimeException will be thrown
         */
        FailPoint.inject(key, () -> {
            throw new RuntimeException("injected error message");
        });
        /**
         * since key2 is not registered, IllegalArgumentException won't be thrown
         */
        FailPoint.inject("key2", () -> {
            throw new IllegalArgumentException("injected error message");
        });

        /**
         * since key1 deRegistered, RuntimeException won't be thrown
         */
        FailPoint.disable(key);
        FailPoint.inject(key, () -> {
            throw new RuntimeException("injected error message");
        });

    }

    @Test
    public void testInject2() {

        String key = "key1";

        FailPoint.inject(key, () -> {
            throw new RuntimeException("injected error message");
        });

        FailPoint.enable("foobar", "value");
        FailPoint.inject(key, () -> {
            throw new RuntimeException("injected error message");
        });

        FailPoint.disable("foobar");
        FailPoint.inject("foobar", () -> {
            throw new RuntimeException("injected error message");
        });
    }

    @Test
    public void testInject3() {

        String key = "key1";

        long start = System.currentTimeMillis();

        FailPoint.enable("key1", "value");
        FailPoint.inject(key, () -> {
            try {
                Thread.sleep(3 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        long end = System.currentTimeMillis();

        Assert.assertTrue((end - start) > 2500l);
    }

    @Test
    public void testReadValue() {

        String key = "key1";

        FailPoint.enable("key1", "value1");
        FailPoint.inject(key, (k, v) -> {
            Assert.assertEquals(k, "key1");
            Assert.assertEquals(v, "value1");
        });
    }

}