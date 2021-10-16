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

package com.alibaba.polardbx.transaction.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.*;

public class DiGraphTest {

    @Test
    public void detect() {

        class Testcase {
            final String[] edges;
            final Integer[] pointsOnCycle;

            public Testcase(String[] edges, Integer[] pointsOnCycle) {
                this.edges = edges;
                this.pointsOnCycle = pointsOnCycle;
            }
        }

        Testcase[] testcases = new Testcase[] {
            // one cycle.
            new Testcase(new String[] {
                "1->2",
                "2->3",
                "3->4",
                "4->5",
                "5->6",
                "6->1",
            }, new Integer[] {
                1, 2, 3, 4, 5, 6,
            }),
            // one cycle with branch.
            new Testcase(new String[] {
                "1->2",
                "2->3",
                "3->4",
                "4->5",
                "5->6",
                "6->7",
                "7->8",
                "6->1",
            }, new Integer[] {1, 2, 3, 4, 5, 6}),
            new Testcase(new String[] {
                "1->2",
                "2->3",
                "3->1",
                "3->4",
                "4->5",
                "5->3",
                "6->7",
                "8->9",
            }, new Integer[] {1, 2, 3}),
            new Testcase(new String[] {
                "1->2",
                "3->4",
                "4->3",
            }, new Integer[] {3, 4}),
            new Testcase(new String[] {
                "1->2",
                "2->3",
                "3->4",
            }, new Integer[] {}),
        };

        for (Testcase testcase : testcases) {
            DiGraph<Integer> graph = new DiGraph<>();
            for (String edgeDesc : testcase.edges) {
                String[] parts = edgeDesc.split("->");
                Integer from = Integer.valueOf(parts[0]);
                Integer to = Integer.valueOf(parts[1]);
                graph.addDiEdge(from, to);
            }
            Optional<ArrayList<Integer>> result = graph.detect();
            if (testcase.pointsOnCycle.length == 0) {
                assertFalse(result.isPresent());
            } else {
                assertTrue(result.isPresent());
                assertEquals(Arrays.asList(testcase.pointsOnCycle), result.get());
            }
        }
    }
}