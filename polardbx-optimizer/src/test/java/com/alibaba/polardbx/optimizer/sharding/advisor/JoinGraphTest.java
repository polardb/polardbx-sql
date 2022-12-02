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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.utils.Pair;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Test the correctness of search algorithm
 *
 * @author shengyu
 */
public class JoinGraphTest {

    static class GraphGen {
        static void randomGraph(JoinGraph jg, int nodeNum, int seed) {
            int col = 8;
            int rep = 1;
            int top = 2;

            String[] tables = new String[nodeNum];
            for (int i = 0; i < nodeNum; i++) {
                tables[i] = "A" + i;
            }
            Random r1 = new Random(seed);

            for (int i = 0; i < nodeNum; i++) {
                for (int j = 0; j < top; j++) {
                    int nxt = r1.nextInt(nodeNum);
                    for (int k = 0; k < rep; k++) {
                        jg.addEdge(tables[i], r1.nextInt(col), tables[nxt], r1.nextInt(col), r1.nextInt(100000));
                    }
                }
            }

            List<Pair<String, Long>> rowCounts = new ArrayList<>();
            for (int i = 0; i < nodeNum; i++) {
                rowCounts.add(new Pair<>(tables[i], r1.nextInt(10000) + 4000L));
            }
            jg.dealWithBroadCast(rowCounts);
        }

        static void redShiftGraph(JoinGraph jg, int nodeNum, int seed) {
            int vNum = nodeNum * 5;

            String[] tables = new String[nodeNum];
            for (int i = 0; i < nodeNum; i++) {
                tables[i] = "A" + i;
            }
            Random r1 = new Random(seed);
            for (int i = 0; i < vNum; i++) {
                int u = r1.nextInt(nodeNum);
                int v = r1.nextInt(nodeNum);
                jg.addEdge(tables[u], r1.nextInt(u + 1), tables[v], r1.nextInt(v + 1), r1.nextInt(100000));
            }
        }

        static void starGraph(JoinGraph jg, int starNum, int seed) {
            int nodeNum = 4;
            int vNum = 8;

            String tables[] = new String[starNum * nodeNum + 1];
            for (int i = 0; i < tables.length; i++) {
                tables[i] = "A" + i;
            }
            Random r1 = new Random(seed);
            for (int i = 0; i < starNum; i++) {
                int base = i * nodeNum + 1;
                for (int j = 0; j < vNum; j++) {
                    int u = r1.nextInt(nodeNum) + base;
                    int v = r1.nextInt(nodeNum) + base;
                    jg.addEdge(tables[u], r1.nextInt(u), tables[v], r1.nextInt(v), r1.nextInt(100000));
                }
                jg.addEdge(tables[0], 0, tables[base], r1.nextInt(base), r1.nextInt(100000));
            }
        }
    }

    @Test
    public void testKey() {
        int[] id = new int[] {Integer.MAX_VALUE, 0, 5, Integer.MAX_VALUE};
        int[] col = new int[] {0, 4, (1 << 30), Integer.MAX_VALUE};
        for (int i = 0; i < id.length; i++) {
            Long key = DirectedEdges.buildKey(id[i], col[i]);
            assert DirectedEdges.getId(key) == id[i] :
                "id wrong! Expected:\n" + Integer.toBinaryString(id[i]) +
                    ". But is : \n" + Integer.toBinaryString(DirectedEdges.getId(key));
            assert DirectedEdges.getCol(key) == col[i] :
                "col wrong! Expected: " + Integer.toBinaryString(col[i]) +
                    ". But is : " + DirectedEdges.getCol(key);
            ;
        }
    }

    void warmUp() {
        JoinGraphCut jg = new JoinGraphCut();
        GraphGen.randomGraph(jg, new Random().nextInt(4) + 6, new Random().nextInt());
        jg.analyse();
    }

    void testCommon(int ini, int ed, String method, int nodeNum, JoinGraph... jgs) {
        testCommon(ini, ed, method, nodeNum, false, jgs);
    }

    void testCommon(int ini, int ed, String method, int nodeNum, boolean checker, JoinGraph... jgs) {
        if (nodeNum == 0) {
            return;
        }
        warmUp();
        Long[] begin = new Long[jgs.length];
        Long[] total = new Long[jgs.length];
        Arrays.fill(total, 0L);
        for (int i = ini; i < ed; i++) {
            for (int j = 0; j < jgs.length; j++) {
                JoinGraph jg = jgs[j];
                jg.restart();
                try {
                    Method me = GraphGen.class.getDeclaredMethod(method, JoinGraph.class, int.class, int.class);
                    me.setAccessible(true);
                    me.invoke(null, jg, nodeNum, i);
                } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                    e.printStackTrace();
                }
                begin[j] = System.currentTimeMillis();
                jg.analyse();
                total[j] += System.currentTimeMillis() - begin[j];
            }
            assert !checker || jgs.length <= 1 || jgs[0].getBests().equals(jgs[1].getBests()) : "random seed is: " + i;
        }
        for (int j = 0; j < jgs.length; j++) {
            //System.out.println(jgs[j].getClass().getSimpleName() + ": " + (total[j]) / 1000.0 / (ed - ini) + " s ");
        }
    }

    @Test
    public void bigGraphTest() {
        testCommon(4000, 4010, "randomGraph", 10, true,
            new JoinGraphCut(), new JoinGraphCut(false));
    }

    @Test
    public void CorrectnessTest() {
        testCommon(2000, 2001, "randomGraph", 6, true,
            new JoinGraphCut(), new NaiveJoinGraph());
    }

    @Test
    public void orderTest() {
        testCommon(203, 204, "randomGraph", 4,
            new JoinGraphCut(), new JoinGraph());
    }

    @Test
    public void redShiftTest() {
        testCommon(1230, 1231, "redShiftGraph", 5,
            new JoinGraphCut(), new JoinGraph());
    }

    @Test
    public void starGraphTest() {
        testCommon(106, 107, "starGraph", 4,
            new JoinGraphCut(), new JoinGraphCut(false));
        ;
    }
}
