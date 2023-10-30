package com.alibaba.polardbx.optimizer.config.table;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GeneratedColumnUtilTest {
    private static final String[][] PARAMS = {
        {"a,b,c", "a,b|a,c|a,d", "a|b,c"},
        {"a,b,c", "a,B|B,c|a,D", "a|b|c"},
        {"a,b,c,d,e", "a,b|a,c|a,e", "a,d|b,c,e"},
        {"a,b", "a,c|a,d|a,e|b,a|f,g", "b|a"},
        {"a,b,c,d,e", "a,b|a,c|a,d|a,e", "a|b,c,d,e"},
        {"a,b,c,d,e", "a,b|a,c|a,d|a,e|b,c", "a|b,d,e|c"},
        {"a,b,c,d,e", "a,b|a,c|a,d|a,e|b,c|c,d", "a|b,e|c|d"},
        {"A,b,C,d,E", "a,B|A,c|A,d|A,E|b,c|C,d", "A|b,E|C|d"},
        {"中文1,中文2,中文3", "中文1,中文2|中文1,中文3|中文2,中文3", "中文1|中文2|中文3"},
        {"a,b,c,d", "a,b|b,c|c,a|d,a", ""},
        {"a,b,c,d", "a,b|b,c|c,a|a,d", ""},
        {"a,b,c", "a,a|a,b|a,c", ""},
        {"a,b,c", "a,a|b,a", ""},
    };

    @Test
    public void testTopologicalSort() {
        for (String[] param : PARAMS) {
            List<String> vertices = Arrays.asList(param[0].split(","));
            List<List<String>> edges =
                Arrays.stream(param[1].split("\\|")).map(s -> Arrays.asList(s.split(","))).collect(Collectors.toList());
            List<List<String>> expected = param[2].length() == 0 ? null :
                Arrays.stream(param[2].split("\\|")).map(s -> Arrays.asList(s.split(","))).collect(Collectors.toList());
            testTopologicalSortInternal(vertices, edges, expected);
        }
    }

    public void testTopologicalSortInternal(List<String> vertices, List<List<String>> edges,
                                            List<List<String>> expected) {
        System.out.println(vertices);
        System.out.println(edges);
        System.out.println(expected);
        System.out.println();

        GeneratedColumnUtil.TopologicalSort sort = new GeneratedColumnUtil.TopologicalSort();
        sort.addVertices(vertices);
        for (List<String> edge : edges) {
            sort.addEdge(edge.get(0), edge.get(1));
        }

        List<List<String>> result;
        try {
            result = sort.sort();
        } catch (Throwable e) {
            System.out.println(e.getMessage());
            Assert.assertNull(expected);
            return;
        }

        Assert.assertEquals(expected.size(), result.size());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals(expected.get(i).size(), result.get(i).size());
            Assert.assertTrue(expected.get(i).containsAll(result.get(i)));
        }
    }
}
