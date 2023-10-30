package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.statis.PlanAccessStat;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PlanAccessStatTest {

    // DFS 遍历
    private static void dfs(Map<Integer, Set<Integer>> graph, int node, Set<Integer> visited, Set<Integer> component) {
        visited.add(node);
        component.add(node);
        for (int neighbor : graph.get(node)) {
            if (!visited.contains(neighbor)) {
                dfs(graph, neighbor, visited, component);
            }
        }
    }

    // 求连通分量
    public static List<Set<Integer>> connectedComponents(Map<Integer, Set<Integer>> graph) {
        Set<Integer> visited = new HashSet<>();
        List<Set<Integer>> connected = new ArrayList<>();
        for (int node : graph.keySet()) {
            if (!visited.contains(node)) {
                Set<Integer> component = new HashSet<>();
                dfs(graph, node, visited, component);
                visited.addAll(component);
                connected.add(component);
            }
        }
        return connected;
    }

    @Test
    public void testJoinClosureCompute() throws Exception {
        // 创建一个无向图
        Map<Integer, Set<Integer>> graph = new HashMap<>();
        graph.put(1, new HashSet<>(Arrays.asList(2, 3)));
        graph.put(2, new HashSet<>(Arrays.asList(1, 3)));
        graph.put(3, new HashSet<>(Arrays.asList(1, 2, 4)));
        graph.put(4, new HashSet<>(Arrays.asList(3, 5)));
        graph.put(5, new HashSet<>(Arrays.asList(4)));
        graph.put(6, new HashSet<>(Arrays.asList(7)));
        graph.put(7, new HashSet<>(Arrays.asList(6)));

        // 求解连通分量
        List<Set<Integer>> connected = connectedComponents(graph);

        // 输出结果
        for (Set<Integer> component : connected) {
            System.out.println(component);
        }
    }

    @Test
    public void testJoinClosureBuild() throws Exception {
        // 创建一个无向图
        Map<String, Set<String>> graph = new HashMap<>();
        PlanAccessStat.addJoinTablesToJoinGraph(graph, Lists.newArrayList("u"));
        PlanAccessStat.addJoinTablesToJoinGraph(graph, Lists.newArrayList("v"));
        PlanAccessStat.addJoinTablesToJoinGraph(graph, Lists.newArrayList("a", "b", "d"));
        PlanAccessStat.addJoinTablesToJoinGraph(graph, Lists.asList("e", new String[] {"f"}));
        PlanAccessStat.addJoinTablesToJoinGraph(graph, Lists.asList("x", new String[] {"y"}));
        PlanAccessStat.addJoinTablesToJoinGraph(graph, Lists.asList("d", new String[] {"f"}));
        PlanAccessStat.addJoinTablesToJoinGraph(graph, Lists.asList("f", new String[] {"c"}));
        PlanAccessStat.addJoinTablesToJoinGraph(graph, Lists.asList("a", new String[] {"e"}));
        PlanAccessStat.addJoinTablesToJoinGraph(graph, Lists.asList("e", new String[] {"b"}));

        // 求解连通分量
        List<Set<String>> connected1 = PlanAccessStat.buildJoinClosure(graph);
        // 输出结果
        for (Set<String> component : connected1) {
            System.out.println("connected1:" + component);
        }

        Map<String, Set<String>> graph2 = new HashMap<>();
        PlanAccessStat.addJoinTablesToJoinGraph(graph2, Lists.asList("a", new String[] {"b", "d"}));
        PlanAccessStat.addJoinTablesToJoinGraph(graph2, Lists.asList("e", new String[] {"f"}));
        PlanAccessStat.addJoinTablesToJoinGraph(graph2, Lists.asList("f", new String[] {"c"}));

        // 求解连通分量
        List<Set<String>> connected2 = PlanAccessStat.buildJoinClosure(graph2);
        // 输出结果
        for (Set<String> component : connected2) {
            System.out.println("connected2:" + component);
        }

        Map<String, Set<String>> graph3 = new HashMap<>();

        for (int i = 0; i < connected1.size(); i++) {
            PlanAccessStat.addJoinTablesToJoinGraph(graph3, connected1.get(i).stream().collect(Collectors.toList()));
        }
        for (int i = 0; i < connected2.size(); i++) {
            PlanAccessStat.addJoinTablesToJoinGraph(graph3, connected2.get(i).stream().collect(Collectors.toList()));
        }

        // 求解连通分量
        List<Set<String>> connected3 = PlanAccessStat.buildJoinClosure(graph3);
        // 输出结果
        for (Set<String> component : connected3) {
            System.out.println("connected3:" + component);
        }
    }
}
