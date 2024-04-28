package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.mpp.execution.DriverSplitRunner;
import com.alibaba.polardbx.executor.mpp.execution.PipelineContext;
import com.alibaba.polardbx.executor.mpp.execution.TaskContext;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalBufferExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.mpp.split.SplitManager;
import com.alibaba.polardbx.executor.mpp.util.MoreExecutors;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.PostPlanner;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.mockito.Mockito;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy.FILE_CONCURRENT;
import static org.junit.Assert.assertEquals;

@Ignore
public class PlanFragmentTestBase extends ParameterizedTestCommon {
    protected AbstractPlanFragmentTester tester = null;

    // for input of splits.
    // This only applies to scenarios where the data is evenly distributed,
    // and does not apply to scenarios where the data is skewed
    protected int fileCountForEachPart = 3;

    // DOP of this fragment.
    protected int defaultParallelism = 16;

    // count of workers
    protected int taskNumber = 4;

    // for local partition
    // the value is not -1 means we use local partition mode.
    protected int localPartitionCount = 16;
    protected int totalPartitionCount = 64;

    protected ExecutorMode executorMode = ExecutorMode.MPP;

    public PlanFragmentTestBase(String caseName, String targetEnvFile, int sqlIndex, String sql,
                                String expectedPlan, String lineNum,
                                AbstractPlanFragmentTester tester) {
        super(caseName, targetEnvFile, sqlIndex, sql, expectedPlan, lineNum);
        this.tester = tester;
    }

    public PlanFragmentTestBase(String caseName, String targetEnvFile, int sqlIndex, String sql,
                                String expectedPlan, String lineNum) {
        super(caseName, targetEnvFile, sqlIndex, sql, expectedPlan, lineNum);
    }

    protected static List<Object[]> loadWithFragment(Class clazz) {

        URL url = clazz.getResource(clazz.getSimpleName() + ".class");
        File dir = new File(url.getPath().substring(0, url.getPath().indexOf(clazz.getSimpleName() + ".class")));
        File[] filesList = dir.listFiles();

        List<Object[]> cases = new ArrayList<>();
        for (File file : filesList) {
            if (file.isFile()) {
                if (file.getName().startsWith(clazz.getSimpleName() + ".") && file.getName().endsWith(".yml")) {
                    if (file.getName().equals(clazz.getSimpleName() + ".ddl.yml")
                        || file.getName().equals(clazz.getSimpleName() + ".udf.yml")
                        || file.getName().equals(clazz.getSimpleName() + ".outline.yml")
                        || file.getName().equals(clazz.getSimpleName() + ".statistic.yml")
                        || file.getName().equals(clazz.getSimpleName() + ".config.yml")) {
                        continue;
                    }

                    List<Map<String, String>> sqls = loadSqls(file.getName(), clazz);
                    int sqlIndex = 0;

                    for (Map<String, String> sql : sqls) {
                        cases.add(new Object[] {
                            file.getName(), sqlIndex, sql.get("sql"), sql.get("plan"),
                            sql.get("lineNum"), sql.get("fragment")});

                        sqlIndex++;
                    }
                }
            }
        }
        caseNum += cases.size();
        return cases;
    }

    protected LocalExecutionPlanner buildLocalExecutionPlanner(ExecutionContext context) {
        // for constructor of exchange operator
        ExchangeClientSupplier exchangeClientSupplier = null;

        // useless parameter for columnar plan
        final int bkaJoinParallelism = 0;

        // the number of prefetched splits, only for logical view.
        final int prefetch = 0;

        // a thread responsible for notifying spill processing.
        Executor notificationExecutor = MoreExecutors.directExecutor();

        // a factory to create spiller object.
        SpillerFactory spillerFactory = null;

        // for old version runtime filter.
        HttpClient httpClient = null;
        URI runtimeFilterUpdateUri = null;
        boolean enableRuntimeFilter = false;

        // mapping from table name to splits count.
        Map<String, Integer> splitCountMap = new HashMap<>();

        context.setExecuteMode(executorMode);

        LocalExecutionPlanner localExecutionPlanner = new LocalExecutionPlanner(
            context,

            // for constructor of exchange operator.
            exchangeClientSupplier,

            // DOP of this fragment.
            defaultParallelism,

            // useless parameter
            bkaJoinParallelism,

            // count of workers
            taskNumber,

            // the number of prefetched splits, only for logical view.
            prefetch,

            // a thread responsible for notifying spill processing.
            notificationExecutor,

            // a factory to create spiller object.
            spillerFactory,

            // for old version runtime filter.
            httpClient, runtimeFilterUpdateUri, enableRuntimeFilter,

            // for local partition
            localPartitionCount, totalPartitionCount,

            // mapping from table name to splits count.
            splitCountMap,
            new MockSplitManager(fileCountForEachPart)
        );

        return localExecutionPlanner;
    }

    @Override
    protected String getPlan(String testSql) {
        // for execution context.
        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setServerVariables(new HashMap<>());
        executionContext.setAppName(appName);
        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(
            ByteString.from(testSql), currentParameter, executionContext, false);
        setSysDefVariable(sqlParameterized.getParameters());
        Map<Integer, ParameterContext> param = OptimizerUtils.buildParam(sqlParameterized.getParameters());
        SqlNodeList astList = new FastsqlParser().parse(
            sqlParameterized.getSql(), sqlParameterized.getParameters(), executionContext);
        SqlNode ast = astList.get(0);
        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        executionContext.setParams(new Parameters(param, false));
        executionContext.getExtraCmds().putAll(configMaps);
        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName,
            executionContext.getExtraCmds(),
            executionContext.getGroupHint());
        if (explainCost) {
            executionContext.setCalcitePlanOptimizerTrace(new CalcitePlanOptimizerTrace());
            executionContext.getCalcitePlanOptimizerTrace()
                .ifPresent(x -> x.setSqlExplainLevel(SqlExplainLevel.ALL_ATTRIBUTES));
        }
        executionContext.setInternalSystemSql(false);

        // Build Plan.
        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);
        processParameter(sqlParameterized, executionContext);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);
        plannerContext.setAddForcePrimary(addForcePrimary);

        ExecutionPlan executionPlan = Planner.getInstance().getPlan(ast, plannerContext);
        executionPlan = PostPlanner.getInstance().optimize(executionPlan, executionContext);

        // Build pipeline fragments.
        RelNode root = executionPlan.getPlan();
        List<DataType> columns = CalciteUtils.getTypes(root.getRowType());

        LocalExecutionPlanner localExecutionPlanner = buildLocalExecutionPlanner(executionContext);
        OutputBufferMemoryManager localBufferManager = localExecutionPlanner.createLocalMemoryManager();
        LocalBufferExecutorFactory factory = new LocalBufferExecutorFactory(localBufferManager, columns, 1);

        // prepare for fragments building.
        localExecutionPlanner.setForbidMultipleReadConn(true);
        executionContext.setRuntimeStatistics(new RuntimeStatistics(executionContext.getTraceId(), executionContext));

        List<PipelineFactory> pipelineFactories =
            localExecutionPlanner.plan(root, factory, localBufferManager, executionContext.getTraceId());

        Assert.assertTrue(tester.test(pipelineFactories, executionContext));

        // generate plan string for comparison.
        String planStr = RelUtils
            .toString(executionPlan.getPlan(), param, RexUtils.getEvalFunc(executionContext), executionContext);

        String code = removeSubqueryHashCode(planStr, executionPlan.getPlan(),
            executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter(),
            executionContext.getSqlExplainLevel());
        return code;
    }

    private static class MockSplitManager implements SplitManager {

        private final int fileCountForEachPart;

        public MockSplitManager(int fileCountForEachPart) {
            this.fileCountForEachPart = fileCountForEachPart;
        }

        @Override
        public SplitInfo getSingleSplit(LogicalView logicalView, ExecutionContext executionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SplitInfo getSplits(LogicalView logicalView, ExecutionContext executionContext,
                                   boolean highConcurrencyQuery) {
            if (!(logicalView instanceof OSSTableScan)) {
                throw new UnsupportedOperationException();
            }
            final OSSTableScan ossTableScan = (OSSTableScan) logicalView;

            List<RelNode> sortInputs = ExecUtils.getInputs(
                ossTableScan, executionContext, !ExecUtils.isMppMode(executionContext));

            HashMap<String, String> shardSet = new HashMap<>();
            List<Split> splitList = new ArrayList<>();
            int splitCount = 0;

            // split according to all table files.
            for (RelNode input : sortInputs) {
                List<OssSplit> splits = getFileConcurrencySplits(ossTableScan, input, fileCountForEachPart);
                if (splits != null) {
                    for (OssSplit split : splits) {
                        shardSet.put(split.getPhysicalSchema(), split.getLogicalSchema());
                        splitList.add(new Split(false, split));
                        splitCount++;
                    }
                }
            }
            return new SplitInfo(ossTableScan.getRelatedId(), ossTableScan.isExpandView(),
                FILE_CONCURRENT,
                ImmutableList.of(splitList),
                shardSet, 1,
                splitCount, false);
        }

        private List<OssSplit> getFileConcurrencySplits(OSSTableScan ossTableScan, RelNode input,
                                                        int fileCountForEachPart) {
            Preconditions.checkArgument(input instanceof PhyTableOperation);
            List<OssSplit> splits = new ArrayList<>();

            PhyTableOperation phyTableOperation = (PhyTableOperation) input;
            String logicalSchema = phyTableOperation.getSchemaName();
            String physicalSchema = phyTableOperation.getDbIndex();

            String logicalTableName = phyTableOperation.getLogicalTableNames().get(0);
            List<String> phyTableNameList = phyTableOperation.getTableNames().get(0);

            for (int i = 0; i < phyTableNameList.size(); i++) {
                String phyTable = phyTableNameList.get(i);

                // build single physical table list and params,
                // and split for each file.
                List<String> singlePhyTableNameList = ImmutableList.of(phyTable);

                for (int fileNum = 0; fileNum < fileCountForEachPart; fileNum++) {

                    String fileName = fileNameOf(phyTable, fileNum);

                    OssSplit ossSplit = new OssSplit(logicalSchema, physicalSchema, new HashMap<>(),
                        logicalTableName, singlePhyTableNameList, ImmutableList.of(fileName), null,
                        null, -1, false);
                    splits.add(ossSplit);
                }

            }

            return splits;
        }

        private static String fileNameOf(String phyTable, int fileCount) {
            return phyTable + "_" + fileCount + ".orc";
        }
    }

}
