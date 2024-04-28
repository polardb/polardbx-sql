package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * @author fangwu
 */
public class DrdsRelMdCostTest extends PlanTestCommon {

    private static String[][] testSqls;
    private static Class<? extends Metadata>[] metadatas;
    private static int testRound = 100;

    static {
        testSqls = new String[][] {
            {"single_table", "select count(*), sum(bid) from key_tbl where name in ('a', 'b') order by id limit 10"},
            {
                "1_join",
                "select count(*), sum(a.bid) from key_tbl a join hash_tbl b on a.bid=b.bid  where a.name in ('a', 'b')"
                    + " order by a.bid limit 10"},
            {
                "2_join",
                "select count(*), sum(a.bid) from key_tbl a join hash_tbl b on a.bid=b.bid join hash_tbl_todays"
                    + " c on b.name=c.name where a.name in ('a', 'b') order by a.bid limit 10"},
            {
                "3_join",
                "select * from key_tbl a join hash_tbl b on a.bid=b.bid join hash_tbl_todays c on b.name=c.name"
                    + " join orders d on c.id=d.order_id where a.name in ('a', 'b') order by a.bid limit 10"},
            {
                "4_join",
                "select * from key_tbl a join hash_tbl b on a.bid=b.bid join hash_tbl_todays c on b.name=c.name"
                    + " join orders d on c.id=d.order_id join orders_todays e on a.id=e.id  where a.name in ('a', 'b')"
                    + " order by a.bid limit 10"},
        };

        metadatas = new Class[] {
            BuiltInMetadata.Selectivity.class,
            BuiltInMetadata.CumulativeCost.class,
            BuiltInMetadata.NonCumulativeCost.class,
            BuiltInMetadata.UniqueKeys.class,
            BuiltInMetadata.ColumnUniqueness.class,
            BuiltInMetadata.CompositePk.class,
            BuiltInMetadata.Collation.class,
            BuiltInMetadata.Distribution.class,
            BuiltInMetadata.NodeTypes.class,
            BuiltInMetadata.RowCount.class,
            BuiltInMetadata.MaxRowCount.class,
            BuiltInMetadata.MinRowCount.class,
            BuiltInMetadata.DistinctRowCount.class,
            BuiltInMetadata.PercentageOriginalRows.class,
            BuiltInMetadata.PopulationSize.class,
            BuiltInMetadata.Size.class,
            BuiltInMetadata.ColumnOrigin.class,
            BuiltInMetadata.ColumnOriginName.class,
            BuiltInMetadata.OriginalRowType.class,
            BuiltInMetadata.DmlColumnName.class,
            BuiltInMetadata.FunctionalDependency.class,
            BuiltInMetadata.ExpressionLineage.class,
            BuiltInMetadata.TableReferences.class,
            BuiltInMetadata.ExplainVisibility.class,
            BuiltInMetadata.Predicates.class,
            BuiltInMetadata.AllPredicates.class,
            BuiltInMetadata.Memory.class,
            BuiltInMetadata.LowerBoundCost.class,
        };
    }

    public DrdsRelMdCostTest(String caseName, String targetEnvFile) {
        super(caseName, targetEnvFile);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return ImmutableList.of(
            new Object[] {"DrdsRelMdCostTest", "/com/alibaba/polardbx/optimizer/config/meta/DrdsRelMdCostTest"});
    }

    @BeforeClass
    public static void before() {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_MQ_CACHE_COST_BY_THREAD, "true");
    }

    @AfterClass
    public static void clean() {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_MQ_CACHE_COST_BY_THREAD, "true");
    }

    @Test
    public void testParrallel() throws InterruptedException {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_MQ_CACHE_COST_BY_THREAD, "true");
        ExecutorService pool = newFixedThreadPool(11, new DefaultThreadFactory("mq-test"));
        RelMetadataQuery mq = PlannerUtils.newMetadataQuery();
        ExecutionContext executionContext = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(testSqls[0][1], executionContext);
        RelNode rel = plan.getPlan();

        // thread cache test
        Semaphore semaphore = new Semaphore(0);
        AtomicBoolean getError = new AtomicBoolean(false);
        for (int i = 0; i <= 9; i++) {
            pool.submit(() -> {
                try {
                    while (semaphore.availablePermits() == 0) {
                        test(mq, rel);
                    }
                } catch (InterruptedException interruptedException) {
                } catch (Exception e) {
                    getError.set(true);
                    System.out.println("wake up");
                    semaphore.release();
                    throw new RuntimeException(e);
                }
            });
        }
        try {
            semaphore.tryAcquire(2000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (getError.get()) {
            Assert.fail("parallel thread error");
        }
        pool.shutdownNow();
    }

    @Test
    public void testRt() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        List<String> result = Lists.newArrayList();
        RelMetadataQuery mq = PlannerUtils.newMetadataQuery();

        // cache all test
        System.out.println("cache all test");
        warmup();
        result.addAll(processTest(true, mq));

        // thread cache test
        try {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_MQ_CACHE_COST_BY_THREAD, "false");
            System.out.println("thread cache test");
            mq = PlannerUtils.newMetadataQuery();
            result.addAll(processTest(false, mq));

            result.sort(String::compareToIgnoreCase);
            for (String line : result) {
                System.out.println(line);
            }
        } finally {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_MQ_CACHE_COST_BY_THREAD, "true");
        }
    }

    @Test
    public void testCorrectness() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        // thread cache test
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_MQ_CACHE_COST_BY_THREAD, "true");
        Map<Pair<Method, Object[]>, Object> threadResult = Maps.newHashMap();
        RelMetadataQuery mq = PlannerUtils.newMetadataQuery();
        Map<Pair<Method, Object[]>, Object> oldResult = Maps.newHashMap();
        try {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_MQ_CACHE_COST_BY_THREAD, "false");
            for (String[] testRel : testSqls) {
                String testSql = testRel[1];

                ExecutionContext executionContext = new ExecutionContext(appName);
                ExecutionPlan plan = getExecutionPlan(testSql, executionContext);
                RelNode rel = plan.getPlan();
                for (Class<? extends Metadata> metadataClass : metadatas) {
                    threadResult.putAll(handleMetadataQueryAndReturnResult(metadataClass, mq, rel));
                    oldResult.putAll(
                        handleMetadataQueryAndReturnResult(metadataClass, PlannerUtils.newMetadataQuery(), rel));
                }

                // check result
                for (Map.Entry<Pair<Method, Object[]>, Object> entry : oldResult.entrySet()) {
                    Pair<Method, Object[]> key = entry.getKey();
                    Object result = entry.getValue();
                    Object newResult = threadResult.get(key);
                    boolean isEqual;
                    if (result instanceof DrdsRelOptCostImpl && newResult instanceof DrdsRelOptCostImpl) {
                        double cost = ((DrdsRelOptCostImpl) newResult).getValue();
                        double newCost = ((DrdsRelOptCostImpl) result).getValue();
                        isEqual = Doubles.compare(cost, newCost) == 0;
                    } else if (result instanceof DrdsRelOptCostImpl && newResult instanceof DrdsRelOptCostImpl ||
                        key.getKey().getDeclaringClass() == BuiltInMetadata.Predicates.class ||
                        key.getKey().getDeclaringClass() == BuiltInMetadata.AllPredicates.class) {
                        isEqual = ((RelOptPredicateList) result).pulledUpPredicates.toString()
                            .equals(((RelOptPredicateList) newResult).pulledUpPredicates.toString())
                            && ((RelOptPredicateList) result).leftInferredPredicates.toString()
                            .equals(((RelOptPredicateList) newResult).leftInferredPredicates.toString())
                            && ((RelOptPredicateList) result).rightInferredPredicates.toString()
                            .equals(((RelOptPredicateList) newResult).rightInferredPredicates.toString())
                            && ((RelOptPredicateList) result).constantMap.toString()
                            .equals(((RelOptPredicateList) newResult).constantMap.toString());
                    } else if (key.getKey().getDeclaringClass() == BuiltInMetadata.ExpressionLineage.class) {
                        if (newResult == null && result == null) {
                            isEqual = true;
                        } else {
                            isEqual = newResult.toString().equals(result.toString());
                        }
                    } else if (key.getKey().getDeclaringClass() == BuiltInMetadata.DmlColumnName.class ||
                        key.getKey().getDeclaringClass() == BuiltInMetadata.NodeTypes.class) {
                        // skip this api
                        isEqual = true;
                    } else {
                        isEqual = Objects.equals(result, newResult);
                    }

                    System.out.println(key + " checked," + result + "," + newResult + "," + isEqual);
                    assert isEqual;
                }
            }
        } finally {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_MQ_CACHE_COST_BY_THREAD, "true");
        }
    }

    @Test
    public void testMqCache() {
        RelMetadataQuery mq = PlannerUtils.newMetadataQuery();
        String testSql = testSqls[0][1];

        ExecutionContext executionContext = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(testSql, executionContext);
        RelNode rel = plan.getPlan();

        // test NullSentinel.ACTIVE path, this should be cached by thread
        List key = FlatLists.of(BuiltInMetadata.Collation.DEF);
        mq.cache(rel, key, NullSentinel.ACTIVE, 0);

        assert NullSentinel.ACTIVE == mq.getCache(rel, key);
        mq.clearThreadCache();
        assert null == mq.getCache(rel, key);

        // test (key.get(classIndex) instanceof Method) path, this should be cached by thread
        List key1 = FlatLists.of(BuiltInMetadata.Collation.DEF, BuiltInMethod.COLLATIONS.method);
        Object o1 = new Object();
        mq.cache(rel, key1, o1, 1);

        assert o1 == mq.getCache(rel, key1);
        mq.clearCache(rel, key1);
        assert null == mq.getCache(rel, key1);

        // test (key.get(classIndex) instanceof MetadataDef) path, this cache choice depended on
        // org.apache.calcite.rel.metadata.RelMetadataQuery.isDeclaringClassShouldBeCached
        List<MetadataDef> testMetaDefs = Lists.newArrayList();
        testMetaDefs.add(BuiltInMetadata.DistinctRowCount.DEF);
        testMetaDefs.add(BuiltInMetadata.Size.DEF);
        testMetaDefs.add(BuiltInMetadata.ColumnOriginName.DEF);
        testMetaDefs.add(BuiltInMetadata.TableReferences.DEF);
        boolean threadCachePath = false;
        boolean normalCachePath = false;
        for (MetadataDef metadataDef : testMetaDefs) {
            List key2 = FlatLists.of(metadataDef);
            Object o2 = new Object();
            mq.cache(rel, key2, o2, 0);
            Class c = ((MetadataDef) metadataDef).metadataClass;
            if (RelMetadataQuery.isDeclaringClassShouldBeCached(c)) {
                assert o2 == mq.getCache(rel, key2);
                mq.clearThreadCache();
                assert null == mq.getCache(rel, key2);
                threadCachePath = true;
            } else {
                assert o2 == mq.getCache(rel, key2);
                mq.clearThreadCache();
                assert o2 == mq.getCache(rel, key2);
                mq.clearCache(rel);
                assert null == mq.getCache(rel, key2);
                normalCachePath = true;
            }
        }
        assert threadCachePath && normalCachePath;
    }

    private List<String> processTest(boolean customCache, RelMetadataQuery mq)
        throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        List<String> result = Lists.newArrayList();
        for (String[] testRel : testSqls) {
            String testName = testRel[0];
            String testSql = testRel[1];

            ExecutionContext executionContext = new ExecutionContext(appName);
            ExecutionPlan plan = getExecutionPlan(testSql, executionContext);
            RelNode rel = plan.getPlan();
            for (Class<? extends Metadata> metadataClass : metadatas) {
                AtomicLong allTime = new AtomicLong();
                for (int i = 0; i < testRound; i++) {
                    long invokeTime = handleMetadataQuery(metadataClass, mq, rel);
                    allTime.addAndGet(invokeTime);
                }
                result.add(
                    metadataClass.getSimpleName() + "_" + testName + "_" + (customCache ? "threadCache" : "cache")
                        + "," + format(allTime.get() / 1000000D) + ","
                        + format(allTime.get() / testRound / 1000000D));
            }
        }
        return result;
    }

    private long handleMetadataQuery(Class<? extends Metadata> metadataClass, RelMetadataQuery mq, RelNode relNode)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Metadata metadata = relNode.metadata(metadataClass, mq);
        List<Pair<Method, Object[]>> methodAndArgs = getMetadataMethodArgs(metadataClass, relNode);
        long nanoStart = System.nanoTime();
        for (Pair<Method, Object[]> pair : methodAndArgs) {
            pair.getKey().invoke(metadata, pair.getValue());
            mq.clearThreadCache();
        }
        return System.nanoTime() - nanoStart;
    }

    private Map<Pair<Method, Object[]>, Object> handleMetadataQueryAndReturnResult(
        Class<? extends Metadata> metadataClass, RelMetadataQuery mq, RelNode relNode)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Metadata metadata = relNode.metadata(metadataClass, mq);
        Map<Pair<Method, Object[]>, Object> rs = Maps.newHashMap();

        List<Pair<Method, Object[]>> methodAndArgs = getMetadataMethodArgs(metadataClass, relNode);
        for (Pair<Method, Object[]> pair : methodAndArgs) {
            Object result = pair.getKey().invoke(metadata, pair.getValue());
            mq.clearThreadCache();
            rs.put(pair, result);
        }
        return rs;
    }

    private Map<Class<? extends Metadata>, List<Pair<Method, Object[]>>> argsCache = Maps.newConcurrentMap();

    private List<Pair<Method, Object[]>> getMetadataMethodArgs(Class<? extends Metadata> metadataClass,
                                                               RelNode relNode) throws NoSuchMethodException {
        if (argsCache.containsKey(metadataClass)) {
            return argsCache.get(metadataClass);
        }
        List<Pair<Method, Object[]>> list = Lists.newArrayList();
        if (BuiltInMetadata.Selectivity.class.equals(metadataClass)) {
            RexBuilder builder = relNode.getCluster().getRexBuilder();
            Method m = BuiltInMetadata.Selectivity.class.getMethod("getSelectivity", RexNode.class);
            Object[] args = new Object[] {
                builder.makeCall(SqlStdOperatorTable.EQUALS, RexInputRef.of(0, relNode.getRowType()),
                    builder.makeLiteral("100"))
            };
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.UniqueKeys.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.UniqueKeys.class.getMethod("getUniqueKeys", boolean.class);
            Object[] args = new Object[] {false};
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.ColumnUniqueness.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.ColumnUniqueness.class.getMethod("areColumnsUnique", ImmutableBitSet.class,
                boolean.class);
            Object[] args = new Object[] {ImmutableBitSet.of(0), false};
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.CompositePk.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.CompositePk.class.getMethod("getPrimaryKey");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.Collation.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.Collation.class.getMethod("collations");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.Distribution.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.Distribution.class.getMethod("distribution");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.NodeTypes.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.NodeTypes.class.getMethod("getNodeTypes");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.RowCount.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.RowCount.class.getMethod("getRowCount");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.MaxRowCount.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.MaxRowCount.class.getMethod("getMaxRowCount");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.MinRowCount.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.MinRowCount.class.getMethod("getMinRowCount");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.DistinctRowCount.class.equals(metadataClass)) {
            RexBuilder builder = relNode.getCluster().getRexBuilder();
            RexNode rex = builder.makeCall(SqlStdOperatorTable.EQUALS, RexInputRef.of(0, relNode.getRowType()),
                builder.makeLiteral("100"));
            Method m =
                BuiltInMetadata.DistinctRowCount.class.getMethod("getDistinctRowCount", ImmutableBitSet.class,
                    RexNode.class);
            Object[] args = new Object[] {ImmutableBitSet.of(0), rex};
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.PercentageOriginalRows.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.PercentageOriginalRows.class.getMethod("getPercentageOriginalRows");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.PopulationSize.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.PopulationSize.class.getMethod("getPopulationSize", ImmutableBitSet.class);
            Object[] args = new Object[] {ImmutableBitSet.of(0)};
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.Size.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.Size.class.getMethod("averageRowSize");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
            m = BuiltInMetadata.Size.class.getMethod("averageColumnSizes");
            args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.ColumnOrigin.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.ColumnOrigin.class.getMethod("getColumnOrigins", int.class);
            Object[] args = new Object[] {0};
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.ColumnOriginName.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.ColumnOriginName.class.getMethod("getColumnOriginNames");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.OriginalRowType.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.OriginalRowType.class.getMethod("getOriginalRowType");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.DmlColumnName.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.DmlColumnName.class.getMethod("getDmlColumnNames");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.FunctionalDependency.class.equals(metadataClass)) {
            Method m =
                BuiltInMetadata.FunctionalDependency.class.getMethod("getFunctionalDependency",
                    ImmutableBitSet.class);
            Object[] args = new Object[] {ImmutableBitSet.of(0)};
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.ExpressionLineage.class.equals(metadataClass)) {
            RexBuilder builder = relNode.getCluster().getRexBuilder();
            RexNode rex = builder.makeCall(SqlStdOperatorTable.EQUALS, RexInputRef.of(0, relNode.getRowType()),
                builder.makeLiteral("100"));
            Method m = BuiltInMetadata.ExpressionLineage.class.getMethod("getExpressionLineage", RexNode.class);
            Object[] args = new Object[] {rex};
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.TableReferences.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.TableReferences.class.getMethod("getTableReferences", boolean.class);
            Object[] args = new Object[] {true};
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.CumulativeCost.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.CumulativeCost.class.getMethod("getCumulativeCost");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.NonCumulativeCost.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.NonCumulativeCost.class.getMethod("getNonCumulativeCost");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.StartUpCost.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.StartUpCost.class.getMethod("getStartUpCost");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.ExplainVisibility.class.equals(metadataClass)) {
            Method m =
                BuiltInMetadata.ExplainVisibility.class.getMethod("isVisibleInExplain", SqlExplainLevel.class);
            Object[] args = new Object[] {SqlExplainLevel.ALL_ATTRIBUTES};
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.Predicates.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.Predicates.class.getMethod("getPredicates");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.AllPredicates.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.AllPredicates.class.getMethod("getAllPredicates");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.Parallelism.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.Parallelism.class.getMethod("isPhaseTransition");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
            BuiltInMetadata.Parallelism.class.getMethod("splitCount");
            args = new Object[0];
            list.add(Pair.of(m, args));
            BuiltInMetadata.Parallelism.class.getMethod("workerCount");
            args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.Memory.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.Memory.class.getMethod("memory");
            Object[] args = new Object[0];
            list.add(Pair.of(m, args));
            m = BuiltInMetadata.Memory.class.getMethod("cumulativeMemoryWithinPhase");
            args = new Object[0];
            list.add(Pair.of(m, args));
        } else if (BuiltInMetadata.LowerBoundCost.class.equals(metadataClass)) {
            Method m = BuiltInMetadata.LowerBoundCost.class.getMethod("getLowerBoundCost", VolcanoPlanner.class);
            Object[] args = new Object[] {this.cluster.getPlanner()};
            list.add(Pair.of(m, args));
        } else {
            throw new RuntimeException("not supported metadata query:" + metadataClass.getSimpleName());
        }
        argsCache.put(metadataClass, list);
        return list;
    }

    private void warmup() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        RelMetadataQuery mq = PlannerUtils.newMetadataQuery();
        for (String[] testRel : testSqls) {
            String testSql = testRel[1];
            ExecutionContext executionContext = new ExecutionContext(appName);
            ExecutionPlan plan = getExecutionPlan(testSql, executionContext);
            RelNode rel = plan.getPlan();
            for (Class<? extends Metadata> metadataClass : metadatas) {
                for (int i = 0; i < 2; i++) {
                    handleMetadataQuery(metadataClass, mq, rel);
                }
            }
        }
        System.out.println("warmup end");
    }

    private String format(Number number) {
        return String.format("%,.5f", number);
    }

    private void test(RelMetadataQuery mq, RelNode rel)
        throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InterruptedException {
        for (Class<? extends Metadata> metadataClass : metadatas) {
            Thread.sleep(100);
            handleMetadataQuery(metadataClass, mq, rel);
        }
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Override
    public void testSql() {
        // override parent method
    }

}
