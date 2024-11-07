package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.PruneRawString;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PushDownOpt;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.utils.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class OptimizerUtilsTest {
    @Test
    public void testBuildInExprKey() {
        Map<Integer, ParameterContext> params = Maps.newHashMap();
        List<Integer> args = Lists.newArrayList();
        args.add(1);
        args.add(2);
        args.add(3);
        ParameterContext pc1 = new ParameterContext(ParameterMethod.setObject1,
            new Object[] {1, new RawString(args.subList(0, 3))});
        ParameterContext pc2 = new ParameterContext(ParameterMethod.setObject1,
            new Object[] {2, new RawString(args.subList(0, 1))});

        params.put(1, pc1);
        params.put(2, pc2);

        String hashMapParams = OptimizerUtils.buildInExprKey(params);
        params = new Int2ObjectOpenHashMap<>();
        params.put(1, pc1);
        params.put(2, pc2);
        String int2ObjectMapParams = OptimizerUtils.buildInExprKey(params);
        System.out.println(hashMapParams);
        System.out.println(int2ObjectMapParams);
        Assert.assertTrue(hashMapParams.equals(int2ObjectMapParams));
    }

    @Test
    public void testFindAllRawStrings() {
        // params has raw string and which size > IN_PRUNE_SIZE
        ExecutionContext executionContext = mock(ExecutionContext.class);
        Parameters parameters = new Parameters();
        Map<Integer, ParameterContext> params = Maps.newHashMap();
        params.put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] {1, new RawString(Arrays.asList(1, 2, 3))}));
        params.put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] {2, new RawString(Arrays.asList(1, 2))}));
        params.put(3,
            new ParameterContext(ParameterMethod.setObject1, new Object[] {3, "test"}));

        parameters.setParams(params);

        when(executionContext.getParams()).thenReturn(parameters);
        Assert.assertTrue(OptimizerUtils.findAllRawStrings(ImmutableSet.of(1, 2), executionContext).length == 2);
        Assert.assertTrue(OptimizerUtils.findAllRawStrings(ImmutableSet.of(2), executionContext).length == 1);
    }

    @Test
    public void testMergeRawStringParameters() {
        RawString rawString = new RawString(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0));
        Parameters parameters = new Parameters();
        Map<Integer, ParameterContext> params = Maps.newHashMap();
        params.put(1,
            new ParameterContext(ParameterMethod.setObject1,
                new Object[] {
                    1, new PruneRawString(rawString.getObjList(), PruneRawString.PRUNE_MODE.RANGE, 0, 1, null)}));
        parameters.setParams(params);

        List<Pair<Integer, PruneRawString>> r = Lists.newArrayList();
        r.add(new Pair<>(1, new PruneRawString(rawString.getObjList(), PruneRawString.PRUNE_MODE.RANGE, 1, 4, null)));
        OptimizerUtils.mergeRawStringParameters(parameters, r.toArray(new Pair[0]));
        Assert.assertTrue(((RawString) parameters.getCurrentParameter().get(1).getValue()).size() == 4);

        // intersecting ranges
        r.clear();
        r.add(new Pair<>(1, new PruneRawString(rawString.getObjList(), PruneRawString.PRUNE_MODE.RANGE, 7, 9, null)));
        OptimizerUtils.mergeRawStringParameters(parameters, r.toArray(new Pair[0]));
        Assert.assertTrue(((RawString) parameters.getCurrentParameter().get(1).getValue()).size() == 6);

        // Non-intersecting ranges
        r.clear();
        r.add(new Pair<>(1, new PruneRawString(rawString.getObjList(), PruneRawString.PRUNE_MODE.RANGE, 7, 9, null)));

        OptimizerUtils.mergeRawStringParameters(parameters, r.toArray(new Pair[0]));
        Assert.assertTrue(((RawString) parameters.getCurrentParameter().get(1).getValue()).size() == 6);
    }

    @Test
    public void testPruning() {
        // if 'DynamicConfig.getInstance().isEnablePruningIn' return false, then pruneRawStringMap is null
        try (MockedStatic<OptimizerUtils> optimizerUtilsMockedStatic =
            Mockito.mockStatic(OptimizerUtils.class, Mockito.CALLS_REAL_METHODS)) {
            LogicalView testLogicalView = mock(LogicalView.class);
            PushDownOpt pushDownOpt = mock(PushDownOpt.class);
            when(testLogicalView.getPushDownOpt()).thenReturn(pushDownOpt);
            ExecutionContext executionContext = new ExecutionContext();
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_PRUNING_IN, "false");
            Map<Pair<String, List<String>>, Parameters> rs =
                OptimizerUtils.pruningInValue(testLogicalView, executionContext);
            Assert.assertTrue(rs == null);

            // 'is need prune' return true and findAllRawStrings return no raw string
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_PRUNING_IN, "true");
            //OptimizerUtils.findAllRawStrings(ImmutableSet.of(1), executionContext)
            optimizerUtilsMockedStatic.when(
                    () -> OptimizerUtils.findAllRawStrings(ImmutableSet.of(1), executionContext))
                .thenReturn(new Pair[0]);

            rs = OptimizerUtils.pruningInValue(testLogicalView, executionContext);
            Assert.assertTrue(rs == null);

            // 'is need prune' return true and findAllRawStrings return no raw string
            Parameters parameters = new Parameters();
            Map<Integer, ParameterContext> params = Maps.newHashMap();
            params.put(1,
                new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    1, new RawString(IntStream.range(0, 10000).boxed().collect(
                    Collectors.toList()))}));
            parameters.setParams(params);
            executionContext.setParams(parameters);
            optimizerUtilsMockedStatic.when(
                    () -> OptimizerUtils.findAllRawStrings(ImmutableSet.of(1), executionContext))
                .thenCallRealMethod();

            // mock sharding
            when(pushDownOpt.getShardRelatedInTypeParamIndexes()).thenReturn(ImmutableSet.of(1));
            Mockito.doAnswer(invocationOnMock -> buildRandomSharding()).when(testLogicalView)
                .buildTargetTables(Mockito.any());
            rs = OptimizerUtils.pruningInValue(testLogicalView, executionContext);

            Assert.assertTrue(rs.size() == 25);
        }
    }

    /**
     * Tests the scenario where the provided {@code RelNode} has non-empty parameters.
     * Verifies that the returned map of parameters is not null.
     */
    @Test
    public void testGetParametersMapForOptimizerWithNonEmptyParameters() {
        // Mock a RelNode with non-empty parameters
        RelNode relNode = mock(RelNode.class);
        PlannerContext plannerContext = mock(PlannerContext.class);

        when(plannerContext.getParams()).thenReturn(new Parameters());
        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(relNode)).thenReturn(plannerContext);

            Map<Integer, ParameterContext> result = OptimizerUtils.getParametersMapForOptimizer(relNode);

            assertNotNull(result);
        }
    }

    /**
     * Validates that the method correctly retrieves non-null parameters from a PlannerContext.
     */
    @Test
    public void testGetParametersForOptimizerWithNonEmptyParametersInContext() {
        // Mock a PlannerContext with non-empty parameters
        PlannerContext plannerContext = mock(PlannerContext.class);
        when(plannerContext.getParams()).thenReturn(new Parameters());

        Parameters result = OptimizerUtils.getParametersForOptimizer(plannerContext);

        assertNotNull(result);
    }

    /**
     * Ensures that the method falls back to the thread-local parameters when PlannerContext's parameters are null.
     * Also checks that the thread-local variable is indeed null after the call.
     */
    @Test
    public void testGetParametersForOptimizerWithNullParametersInContext() {
        // Mock a PlannerContext with null parameters
        PlannerContext plannerContext = mock(PlannerContext.class);
        when(plannerContext.getParams()).thenReturn(null);

        // Call the method under test
        Parameters result = OptimizerUtils.getParametersForOptimizer(plannerContext);

        // Assert that both the thread-local and the method result are null
        assertNull(RelMetadataQuery.THREAD_PARAMETERS.get());
        assertNull(result);
    }

    @Test
    public void testIterateRawStrings() {
        Pair<Integer, RawString>[] rawStringMap = new Pair[1];
        // single raw string test
        List listSingle = IntStream.range(0, 15).boxed().collect(Collectors.toList());
        rawStringMap[0] = Pair.of(1, new RawString(listSingle));
        PruningRawStringStep iterator = OptimizerUtils.iterateRawStrings(rawStringMap, 10000);
        String ret = getIteratorCheckString(iterator);
        assert ret.equalsIgnoreCase("PruneRaw(0) \n"
            + "PruneRaw(1) \n"
            + "PruneRaw(2) \n"
            + "PruneRaw(3) \n"
            + "PruneRaw(4) \n"
            + "PruneRaw(5) \n"
            + "PruneRaw(6) \n"
            + "PruneRaw(7) \n"
            + "PruneRaw(8) \n"
            + "PruneRaw(9) \n"
            + "PruneRaw(10) \n"
            + "PruneRaw(11) \n"
            + "PruneRaw(12) \n"
            + "PruneRaw(13) \n"
            + "PruneRaw(14) \n");

        iterator = OptimizerUtils.iterateRawStrings(rawStringMap, 10);
        assert iterator == null;

        // two raw string test
        rawStringMap = new Pair[2];
        List list1 = Lists.newLinkedList();
        list1.add("string");
        list1.add("1");
        list1.add(3);
        list1.add(null);

        List list2 = Lists.newLinkedList();
        list2.add("s\n");
        list2.add("s\rt");
        list2.add("s't");
        list2.add("s\"t");

        rawStringMap[0] = Pair.of(1, new RawString(list1));
        rawStringMap[1] = Pair.of(2, new RawString(list2));
        iterator = OptimizerUtils.iterateRawStrings(rawStringMap, 16);
        ret = getIteratorCheckString(iterator);

        assert ret.equalsIgnoreCase(
            "PruneRaw('string') PruneRaw('s\\n') \n"
                + "PruneRaw('string') PruneRaw('s\\rt') \n"
                + "PruneRaw('string') PruneRaw('s\\'t') \n"
                + "PruneRaw('string') PruneRaw('s\"t') \n"
                + "PruneRaw('1') PruneRaw('s\\n') \n"
                + "PruneRaw('1') PruneRaw('s\\rt') \n"
                + "PruneRaw('1') PruneRaw('s\\'t') \n"
                + "PruneRaw('1') PruneRaw('s\"t') \n"
                + "PruneRaw(3) PruneRaw('s\\n') \n"
                + "PruneRaw(3) PruneRaw('s\\rt') \n"
                + "PruneRaw(3) PruneRaw('s\\'t') \n"
                + "PruneRaw(3) PruneRaw('s\"t') \n"
                + "PruneRaw(null) PruneRaw('s\\n') \n"
                + "PruneRaw(null) PruneRaw('s\\rt') \n"
                + "PruneRaw(null) PruneRaw('s\\'t') \n"
                + "PruneRaw(null) PruneRaw('s\"t') \n");

        // test multi raw string block by max prune time
        iterator = OptimizerUtils.iterateRawStrings(rawStringMap, 15);
        assert iterator == null;

        // test three raw string
        rawStringMap = new Pair[3];
        rawStringMap[2] = Pair.of(1, new RawString(IntStream.range(0, 10).boxed().collect(Collectors.toList())));
        rawStringMap[1] = Pair.of(2, new RawString(IntStream.range(0, 15).boxed().collect(Collectors.toList())));
        rawStringMap[0] = Pair.of(3, new RawString(IntStream.range(0, 25).boxed().collect(Collectors.toList())));
        iterator = OptimizerUtils.iterateRawStrings(rawStringMap, 10 * 15 * 25);
        int count = 0;
        Set<String> set = Sets.newHashSet();
        Pair<Integer, PruneRawString>[] next = iterator.getTargetPair();
        while (iterator.hasNext()) {
            count++;
            iterator.next();
            StringBuilder line = new StringBuilder();
            for (Pair<Integer, PruneRawString> pair : next) {
                line.append(pair.getValue().getObj(0, -1)).append(" ");
            }
            set.add(line.toString().trim());
        }
        assert count == 10 * 15 * 25;
        assert count == set.size();

        iterator = OptimizerUtils.iterateRawStrings(rawStringMap, 25 * 15 + 1);
        assert iterator == null;
    }

    @NotNull
    private String getIteratorCheckString(PruningRawStringStep iterator) {
        StringBuilder sb = new StringBuilder();
        Pair<Integer, PruneRawString>[] pairs = iterator.getTargetPair();
        while (iterator.hasNext()) {
            iterator.next();
            for (Pair<Integer, PruneRawString> pair : pairs) {
                sb.append(pair.getValue().display() + " ");
            }
            sb.append("\n");
        }
        System.out.println(sb);
        return sb.toString();
    }

    private Map<String, List<List<String>>> buildRandomSharding() {
        Random r = new Random();
        int groupIndex = r.nextInt(5);
        int tblIndex = r.nextInt(5);
        Map<String, List<List<String>>> map = new HashMap<>();
        String groupName = "random_mock_group_" + groupIndex;
        List<String> tblList = Lists.newArrayList();
        List<List<String>> tblLists = Lists.newArrayList();
        tblLists.add(tblList);
        map.put(groupName, tblLists);
        tblList.add("random_mock_tbl_" + tblIndex);
        return map;
    }
}
