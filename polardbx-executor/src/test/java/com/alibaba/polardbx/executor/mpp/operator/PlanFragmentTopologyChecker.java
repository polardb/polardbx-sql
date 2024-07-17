package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.mpp.execution.DriverSplitRunner;
import com.alibaba.polardbx.executor.mpp.execution.PipelineContext;
import com.alibaba.polardbx.executor.mpp.execution.TaskContext;
import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import org.apache.commons.lang.StringUtils;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PlanFragmentTopologyChecker extends AbstractPlanFragmentTester {
    private final String expectedResult;

    public PlanFragmentTopologyChecker(
        int parallelism, int taskNumber, int localPartitionCount, String expectedResult) {
        super(parallelism, taskNumber, localPartitionCount);
        this.expectedResult = expectedResult;
    }

    @Override
    boolean test(List<PipelineFactory> pipelineFactories, ExecutionContext executionContext) {
        System.out.println(pipelineFactories);

        PipelineDepTree pipelineDepTree = new PipelineDepTree(pipelineFactories);
        System.out.println(pipelineDepTree);

        if (executionContext.getMemoryPool() == null) {
            executionContext.setTraceId("mock_trace_id");
            executionContext.setMemoryPool(MemoryManager.getInstance()
                .createQueryMemoryPool(true, executionContext.getTraceId(), executionContext.getExtraCmds()));
        }

        List<DriverSplitRunner> driverSplitRunners = new ArrayList<>();

        StringBuilder executionTopology = new StringBuilder();
        for (PipelineFactory pipelineFactory : pipelineFactories) {
            List<Driver> drivers = new ArrayList<>();

            for (int i = 0; i < pipelineFactory.getParallelism(); i++) {
                TaskContext taskContext = Mockito.mock(TaskContext.class);
                Mockito.when(taskContext.isSpillable()).thenReturn(true);
                Mockito.when(taskContext.getPipelineDepTree()).thenReturn(pipelineDepTree);

                PipelineContext pipelineContext = Mockito.mock(PipelineContext.class);
                Mockito.when(pipelineContext.getTaskContext()).thenReturn(taskContext);

                DriverContext driverContext = Mockito.mock(DriverContext.class);
                Mockito.when(driverContext.getPipelineContext()).thenReturn(pipelineContext);

                DriverExec driverExec = pipelineFactory.createDriverExec(executionContext, driverContext, i);
                Driver driver = new Driver(driverContext, driverExec);
                drivers.add(driver);
                driverSplitRunners.add(new DriverSplitRunner(driver));
            }

            // print
            DriverPrinter driverPrinter = new DriverPrinter();
            driverPrinter.visit(drivers);
            executionTopology.append("pipelineId = ");
            executionTopology.append(pipelineFactory.getPipelineId());
            executionTopology.append('\n');
            executionTopology.append(driverPrinter.print());
        }

        System.out.println(executionTopology.toString());

        String realPlanVal = executionTopology.toString().trim();

        assertEquals("realPlanVal = \n" + realPlanVal
            + "\n targetPlanVal = \n" + expectedResult + "\n", expectedResult, realPlanVal);

        // Assert.assertEquals(expectedResult, executionTopology.toString());

        return true;
    }

    private static class DriverPrinter {
        private static String PREFIX = StringUtils.repeat(" ", 3);

        private StringBuilder treePrinter = new StringBuilder();

        public String print() {
            return treePrinter.toString();
        }

        public void visit(List<Driver> drivers) {
            DriverExec driverExec = drivers.get(0).getDriverExec();
            int dop = drivers.size();

            treePrinter.append("[Producer]: ");
            treePrinter.append(dop);
            treePrinter.append(" * { ");
            visit(driverExec.getProducer(), 0);
            treePrinter.append(" }\n");

            treePrinter.append("[Consumer]: ");
            treePrinter.append(dop);
            treePrinter.append(" * { ");
            visit(driverExec.getConsumer(), 0);
            treePrinter.append(" }\n");
        }

        public void visit(com.alibaba.polardbx.executor.operator.Executor producer, int level) {
            treePrinter.append(producer.getClass().getSimpleName());

            List<com.alibaba.polardbx.executor.operator.Executor> inputs = producer.getInputs();
            for (com.alibaba.polardbx.executor.operator.Executor input : inputs) {
                if (!(input instanceof EmptyExecutor)) {
                    treePrinter.append(" <-- ");
                    visit(input, level + 1);
                }
            }
        }

        public void visit(ConsumerExecutor consumerExecutor, int level) {
            treePrinter.append(consumerExecutor.getClass().getSimpleName());

            if (consumerExecutor instanceof LocalExchanger) {
                List<ConsumerExecutor> consumerExecutorList = ((LocalExchanger) consumerExecutor).getExecutors();

                treePrinter.append(" --> ");
                treePrinter.append(consumerExecutorList.size());
                treePrinter.append(" * ");

                visit(consumerExecutorList.get(0), level + 1);
            }
        }
    }
}
