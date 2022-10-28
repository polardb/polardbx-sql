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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ArrayTrieTest {

    @Test
    public void testArrayTrieOverFlow() {
        Set<String> set = new HashSet<String>() {{
            add("abc");
            add("abd");
        }};
        try {
            ArrayTrie.buildTrie(set, true, 4);
            Assert.fail("Expect trie overflow");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Array Trie overflow"));
        }
    }

    @Test
    public void testArrayTrie() {
        final int count = 10000;
        List<String> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(RandomStringUtils.random(6, true, false));
        }

        Set<String> nameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < count; i += 10) {
            nameSet.add(list.get(i));
        }
        ArrayTrie arrayTrie = null;
        try {
            arrayTrie = ArrayTrie.buildTrie(nameSet, true, count);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        for (String s : list) {
            testSameContainResult(nameSet, arrayTrie, s);
            testSameContainResult(nameSet, arrayTrie, s.substring(3));
            testSameContainResult(nameSet, arrayTrie, s + "suffix");
            testSameContainResult(nameSet, arrayTrie, s.toLowerCase());
            testSameContainResult(nameSet, arrayTrie, s.toUpperCase());
        }
    }

    @Test
    public void testArrayTrieConcurrently() {
        final int count = 10000;
        final int parallelism = 20;
        List<String> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(RandomStringUtils.random(6, true, false));
        }

        Set<String> nameSet = Collections.synchronizedSortedSet(new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
        for (int i = 0; i < count; i += 10) {
            nameSet.add(list.get(i));
        }
        ArrayTrie arrayTrie = null;
        try {
            arrayTrie = ArrayTrie.buildTrie(nameSet, true, count);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }


        List<String> errors = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch countDownLatch = new CountDownLatch(parallelism);
        for (int i = 0; i < parallelism; i++) {
            ArrayTrie finalArrayTrie = arrayTrie;
            new Thread(() -> {
                try {
                    for (int j = 0; j < count; j += parallelism) {
                        String s = list.get(j);

                        testSameContainResult(nameSet, finalArrayTrie, s);
                        testSameContainResult(nameSet, finalArrayTrie, s.substring(3));
                        testSameContainResult(nameSet, finalArrayTrie, s + "suffix");
                        testSameContainResult(nameSet, finalArrayTrie, s.toLowerCase());
                        testSameContainResult(nameSet, finalArrayTrie, s.toUpperCase());
                    }
                } catch (Throwable e) {
                    errors.add(e.getMessage() + "\n");
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        try {
            if (!countDownLatch.await(10, TimeUnit.SECONDS)) {
                Assert.fail("Trie test timeout");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(StringUtils.join(errors, "\n"), errors.isEmpty());
    }

    private void testSameContainResult(Set set, ArrayTrie trie, String s) {
        Assert
            .assertEquals(String.format("Trie should %sCONTAIN %s", set.contains(s) ? "" : "NOT ", s), set.contains(s),
                trie.contains(s));
    }
}
