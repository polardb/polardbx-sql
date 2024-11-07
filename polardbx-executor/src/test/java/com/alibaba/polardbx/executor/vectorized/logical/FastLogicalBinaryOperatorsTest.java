package com.alibaba.polardbx.executor.vectorized.logical;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.vectorized.ColumnInput;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@RunWith(Parameterized.class)
public class FastLogicalBinaryOperatorsTest extends LogicalBinaryOperatorsTest {
    private static final Map<String, BiFunction<Object, Object, Object>> FUNCTION_MAP =
        new HashMap<String, BiFunction<Object, Object, Object>>() {{
            put("AND", LogicalBinaryOperatorsTest::and);
            put("OR", LogicalBinaryOperatorsTest::or);
        }};

    private static final List<Pair<String, String>> COLUMN_PAIRS = loadColumnPairs();

    public FastLogicalBinaryOperatorsTest(String sql,
                                          List<ColumnInput> inputs,
                                          Block output, int[] selection) {
        super(sql, inputs, output, selection);
    }

    /**
     * test long type only
     */
    protected static List<Pair<String, String>> loadColumnPairs() {
        List<Pair<String, String>> columnarPairs = new ArrayList<>();
        columnarPairs.add(new Pair<>("test_bigint", "test_bigint2"));
        return columnarPairs;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> generate() {
        List<Object[]> result = new ArrayList<>(COLUMN_PAIRS.size() * COLUMN_PAIRS.size() * FUNCTION_MAP.size());

        for (String operator : FUNCTION_MAP.keySet()) {
            for (Pair<String, String> columnPair : COLUMN_PAIRS) {
                result.addAll(generateColCol(columnPair.getKey(), columnPair.getValue(), operator));
                result.addAll(generateColConst(columnPair.getKey(), columnPair.getValue(), operator));
            }
        }
        return result;
    }

    @Before
    public void before() {
        this.executionContext.getParamManager().getProps().put(ConnectionProperties.ENABLE_AND_FAST_VEC, "true");
        this.executionContext.getParamManager().getProps().put(ConnectionProperties.ENABLE_OR_FAST_VEC, "true");
    }

}
