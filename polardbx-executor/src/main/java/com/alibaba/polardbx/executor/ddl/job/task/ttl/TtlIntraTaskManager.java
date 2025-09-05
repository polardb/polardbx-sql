package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author chenghui.lch
 */
public class TtlIntraTaskManager {

    protected BaseDdlTask parentTask;
    protected ExecutionContext ec;
    protected TtlWorkerTaskSubmitter taskSubmitter;
    protected TtlWorkerTaskMonitor taskMonitor;
    protected List<Pair<Future, TtlIntraTaskRunner>> taskFutureInfoList = new ArrayList();
    protected TtlJobContext jobContext;

    public TtlIntraTaskManager(BaseDdlTask ddlTask,
                               ExecutionContext ec,
                               TtlJobContext jobContext,
                               TtlWorkerTaskSubmitter taskSubmitter) {
        this.parentTask = ddlTask;
        this.ec = ec;
        this.jobContext = jobContext;
        this.taskSubmitter = taskSubmitter;
    }

    public TtlIntraTaskManager(BaseDdlTask ddlTask,
                               ExecutionContext ec,
                               TtlJobContext jobContext,
                               TtlWorkerTaskSubmitter taskSubmitter,
                               TtlWorkerTaskMonitor taskMonitor) {
        this.parentTask = ddlTask;
        this.ec = ec;
        this.jobContext = jobContext;
        this.taskSubmitter = taskSubmitter;
        this.taskMonitor = taskMonitor;
    }

    public void submitAndRunIntraTasks() {

        /**
         * submit a real task on thread-pool to exec
         */
        taskFutureInfoList = submitWorkerTasks();
        boolean isInterrupted = false;
        boolean needAutoExit = false;
        boolean allTaskFinished = false;
        boolean withinMaintainableTimeFrame = true;
        List<Throwable> allUnexpectedExList = new ArrayList<>();
        while (true) {

            long beginTsNano = System.nanoTime();

            isInterrupted = ec.getDdlContext().isInterrupted();
            if (isInterrupted) {
                doTaskInterrupt(allUnexpectedExList);
                statTaskTimeCost(beginTsNano);
                break;
            }

            /**
             * Check if it is in the maintainable time frame
             */
            withinMaintainableTimeFrame = checkIfWithinMaintainableTime();
            if (!withinMaintainableTimeFrame) {
                doTaskInterrupt(allUnexpectedExList);
                statTaskTimeCost(beginTsNano);
                break;
            }

            if (checkIfAllTaskFinished(false, allUnexpectedExList)) {
                allTaskFinished = true;
                statTaskTimeCost(beginTsNano);
                break;
            }

            if (!allUnexpectedExList.isEmpty()) {
                /**
                 * Found some unexpected exceptions
                 */
                statTaskTimeCost(beginTsNano);
                break;
            }

            try {
                if (taskMonitor != null) {
                    taskMonitor.doMonitoring();
                    needAutoExit = taskMonitor.checkNeedStop();
                    if (needAutoExit) {
                        doTaskInterrupt(allUnexpectedExList);
                        statTaskTimeCost(beginTsNano);
                        break;
                    }
                }

                // sleep and wait 1s
                Thread.sleep(TtlConfigUtil.getIntraTaskDelegateEachRoundWaitTime());
                statTaskTimeCost(beginTsNano);
            } catch (Throwable ex) {
                // ignore ex
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            }
        }
        if (taskMonitor != null) {
            this.taskMonitor.handleResults(allTaskFinished, isInterrupted, withinMaintainableTimeFrame);
        }
        handleIntraTaskResults(isInterrupted, needAutoExit, allUnexpectedExList);

    }

    private void statTaskTimeCost(long beginTsNano) {
        TtlJobUtil.statTaskTimeCost(this.jobContext, beginTsNano);
    }

    protected List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasks() {
        return taskSubmitter.submitWorkerTasks();
    }

    protected boolean checkIfWithinMaintainableTime() {
        return TtlJobUtil.checkIfInTtlMaintainWindow();
    }

    /**
     * Interrupt task running
     */
    protected void doTaskInterrupt(List<Throwable> allUnexpectedExList) {
        /**
         * Interrupt all intra tasks
         */
        for (int i = 0; i < taskFutureInfoList.size(); i++) {
            Pair<Future, TtlIntraTaskRunner> futureInfo = taskFutureInfoList.get(i);
            Future future = futureInfo.getKey();
            TtlIntraTaskRunner runner = futureInfo.getValue();
            runner.notifyStopTask();
            future.cancel(true);
        }
        checkIfAllTaskFinished(true, allUnexpectedExList);
        return;
    }

    protected boolean checkIfAllTaskFinished(boolean isForInterrupt,
                                             List<Throwable> allUnexpectedExListOutput) {
        int finishCnt = 0;
        int maxWaitTime = TtlConfigUtil.getIntraTaskInterruptionMaxWaitTime();
        List<Throwable> unexpectedExceptionList = new ArrayList<>();
        for (int i = 0; i < taskFutureInfoList.size(); i++) {
            Future future = taskFutureInfoList.get(i).getKey();
            TtlIntraTaskRunner runner = taskFutureInfoList.get(i).getValue();
            Throwable[] unexpectedEx = new Throwable[1];
            boolean[] waitTimeoutFlag = new boolean[1];
            boolean[] completedFlag = new boolean[1];
            checkIfFutureDone(future, maxWaitTime, completedFlag, waitTimeoutFlag, unexpectedEx);
            if (completedFlag[0]) {
                finishCnt++;
            } else {
                if (unexpectedEx[0] != null) {
                    unexpectedExceptionList.add(unexpectedEx[0]);
                }
                if (isForInterrupt) {
                    boolean waitTimeout = waitTimeoutFlag[0];
                    if (waitTimeout) {
                        runner.forceStopTask();
                    }
                }
            }
        }

        if (!unexpectedExceptionList.isEmpty() && allUnexpectedExListOutput != null) {
            allUnexpectedExListOutput.addAll(unexpectedExceptionList);
        }

        if (finishCnt == taskFutureInfoList.size()) {
            /**
             * All subtask has been finished
             */
            return true;
        }

        return false;
    }

    protected void handleIntraTaskResults(boolean isForInterrupted,
                                          boolean needAutoExit,
                                          List<Throwable> foundUnexpectedExList) {
        List<Throwable> taskUnexpectedExList = new ArrayList<>();
        if (foundUnexpectedExList != null && !foundUnexpectedExList.isEmpty()) {
            taskUnexpectedExList = foundUnexpectedExList;
        } else {
            for (int i = 0; i < taskFutureInfoList.size(); i++) {
                Future future = taskFutureInfoList.get(i).getKey();
                Throwable[] unexpectedEx = new Throwable[1];
                checkIfFutureDone(future, 0, null, null, unexpectedEx);
                if (unexpectedEx[0] != null) {
                    taskUnexpectedExList.add(unexpectedEx[0]);
                }
            }
        }

        if (!taskUnexpectedExList.isEmpty()) {
            Throwable ex = taskUnexpectedExList.get(0);
            if (isForInterrupted) {
                TtlLoggerUtil.TTL_TASK_LOGGER.info(ex);
            } else {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            }

            if (isForInterrupted) {
                if (!needAutoExit) {
                    /**
                     * actively throw an ex to make curr job from running to paused
                     */
                    throw new TtlJobRuntimeException(ex,
                        String.format("Ttl job has been interrupted by some unexpected exception, exMsg is ",
                            ex.getMessage()));
                }
            }
        } else {
            if (isForInterrupted) {
                if (!needAutoExit) {
                    /**
                     * actively throw an ex to make curr job from running to paused
                     */
                    throw new TtlJobRuntimeException(String.format("Ttl job has been interrupted"));
                }
            }

        }
    }

    protected Object checkIfFutureDone(Future future,
                                       int maxWaitTimeMills,
                                       boolean[] taskCompletedFlag,
                                       boolean[] waitTimeoutFlag,
                                       Throwable[] unexpectedExOutput) {

        Object rs = null;
        boolean completed = false;
        boolean waitTimeout = false;
        if (future.isDone()) {
            try {
                if (maxWaitTimeMills > 0) {
                    rs = future.get(maxWaitTimeMills, TimeUnit.MILLISECONDS);
                } else {
                    rs = future.get();
                }
                completed = true;
            } catch (CancellationException e) {
                //  if the computation was cancelled
            } catch (InterruptedException e) {
                //  if the current thread was interrupted while waiting
                // such as waiting rowsSpeed permits to perform cleaning up
            } catch (ExecutionException e) {
                // if the computation threw an exception
                // collection the unexpected ex
                if (unexpectedExOutput != null) {
                    unexpectedExOutput[0] = e;
                }
            } catch (TimeoutException e) {
                //  if the wait timed out
                waitTimeout = false;
                if (waitTimeoutFlag != null) {
                    waitTimeoutFlag[0] = waitTimeout;
                }
            } catch (Throwable e) {
                // other ex, such as OutOfMemoryException
                // // collection the unexpected ex
                if (unexpectedExOutput != null) {
                    unexpectedExOutput[0] = e;
                }
            }
        } else {
            completed = false;
        }

        if (taskCompletedFlag != null) {
            taskCompletedFlag[0] = completed;
        }
        return rs;
    }
}
