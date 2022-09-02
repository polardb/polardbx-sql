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

package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

public class JunitListener extends RunListener {

    private static final Log log = LogFactory.getLog(JdbcUtil.class);
    private static final int maxSize = 20;

    long failure = 0;
    long ignored = 0;
    long finish = 0;
    Map<Description, Long> cacheDescription = new HashMap<>();
    TreeSet<TestDescription> descriptionSet = new TreeSet<>((o1, o2) -> o1.takeTime > o2.takeTime ? 1 : -1);

    public void testStarted(Description description) throws Exception {
        cacheDescription.put(description, System.currentTimeMillis());
    }

    public void testFinished(Description description) throws Exception {
        Long startTime = cacheDescription.remove(description);
        if (startTime != null) {
            descriptionSet.add(new TestDescription(description, System.currentTimeMillis() - startTime));
            if (descriptionSet.size() > maxSize) {
                descriptionSet.pollFirst();
            }
        }
        finish++;
        System.out.println(
            "[" + Thread.currentThread().getName() + "]" + " Finish " + description.getClassName() + "." + description
                .getMethodName() + " Task " + (System.currentTimeMillis() - startTime) + " millis");
        System.out
            .println("Test failed: " + failure + ", passed: " + (finish - ignored - failure) + ", ignored: " + ignored);

    }

    public void testFailure(Failure failure) throws Exception {
        this.failure++;
    }

    public void testIgnored(Description description) throws Exception {
        this.ignored++;
    }

    @Override
    public void testRunFinished(Result result) throws Exception {
        for (TestDescription testDescription : descriptionSet) {
            System.out.println(
                String.format("The class %s take %d milliseconds", testDescription.description.getDisplayName(),
                    testDescription.takeTime));
        }

    }

    public class TestDescription {
        private Description description;
        private long takeTime;

        public TestDescription(Description description, long takeTime) {
            this.description = description;
            this.takeTime = takeTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestDescription)) {
                return false;
            }
            TestDescription that = (TestDescription) o;
            return description.equals(that.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(description);
        }
    }
}
