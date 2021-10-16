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

package testframework;

import testframework.testbase.ExecutorTestBase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chuanqin on 17/9/26.
 */
public class TestRunner {

    public static void main(String[] args) {
        int succCount = 0;
        List<Class<?>> list = new ArrayList<Class<?>>();
        try {
            TestCaseScanner.scan("testframework.testcase", list);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(list.size());
        for (Class<?> cla : list) {
            System.out.println(cla.getName());
            try {
                ExecutorTestBase executorTestBase = (ExecutorTestBase) cla.newInstance();
                executorTestBase.testIt();
                succCount++;
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

        }
        System.out.println("Total: " + list.size() + " Succ: " + succCount + " Failed: " + (list.size() - succCount));
    }
}
