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

package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.executor.sync.CollectVariableSyncAction;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * @author yaozhili
 */
public class AlterSystemCompatibilityHandler {
    /**
     * If we want to downgrade the version to x, all handler with compatibility level >= x will be used.
     * For example , assuming handlerList = {
     * ("5.4.17-1234", 100, handle0),
     * ("5.4.17-5678", 200, handle1),
     * ("5.4.18-1234", 300, handle2)
     * }
     * Downgrading to 300 will call handle2().
     * Downgrading to 200 will call handle2() -> handle1().
     * Downgrading to 100 will call handle2() -> handle1() -> handle0().
     * Downgrading to Long.MAX_VALUE will do nothing.
     *
     * @see Handler
     */
    private static final ImmutableList<Handler> HANDLER_LIST = new ImmutableList.Builder<Handler>()
        .add(new Handler(
            "5.4.18-17004745",
            1000L,
            AlterSystemCompatibilityHandler::handleNewTrxLog
        ))
        .add(new Handler(
            "5.4.18-17047709",
            1100L,
            AlterSystemCompatibilityHandler::handleAutoSavepointOpt
        ))
        .build();

    /**
     * If version = 5.4.18-12345678 and compatibilityLevel = 1000,
     * then all instances with versions <= 5.4.18-12345678 are compatible up to a maximum level of 1000.
     * Therefore, before downgrading to a version <= 5.4.18-12345678,
     * the compatibility level must be downgraded to 1000 first.
     * <p>
     * Method handle() should be idempotent.
     */
    private static class Handler {
        public String version;
        public Long compatibilityLevel;
        public Callable<List<Result>> handle;

        public Handler(String version, Long compatibilityLevel, Callable<List<Result>> handle) {
            this.version = version;
            this.compatibilityLevel = compatibilityLevel;
            this.handle = handle;
        }
    }

    private static final ExecutorService singleExecutorService = Executors.newSingleThreadExecutor(
        new NamedThreadFactory("COMPATIBILITY-CHECKER", true));
    private static final Lock lock = new ReentrantLock();
    /**
     * (compatibility level, risks of downgrading to this compatibility level)
     */
    private static final Cache<Long, List<Result>> RISKS = CacheBuilder.newBuilder()
        .recordStats()
        .maximumSize(1024)
        .expireAfterWrite(6, TimeUnit.HOURS)
        .softValues()
        .build();

    private static List<Result> handleNewTrxLog() {
        List<Result> results = new ArrayList<>();
        // Handle new trx log method.
        if (0 != DynamicConfig.getInstance().getTrxLogMethod()) {
            try {
                GlobalTxLogManager.turnOffNewTrxLogMethod();
            } catch (Throwable t) {
                results.add(
                    new Result(Result.Status.ERROR,
                        "Error occurs when checking trx log method: " + t.getMessage(),
                        "-"));
            }
        }

        String error = ExecUtils.waitVarChange("trxLogMethod", "0", 5);
        if (null != error) {
            results.add(
                new Result(Result.Status.ERROR, error, "-"));
        }

        // Drain prepared trx.
        AtomicLong minTrxId = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxTrxId = new AtomicLong(Long.MIN_VALUE);
        ITopologyExecutor executor = ExecutorContext.getContext(DEFAULT_DB_NAME).getTopologyExecutor();
        ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
        Set<String> dnIds = new HashSet<>();
        Set<String> addresses = new HashSet<>();
        for (StorageInstHaContext ctx : StorageHaManager.getInstance().getMasterStorageList()) {
            // Filter same host:port.
            if (addresses.add(ctx.getCurrAvailableNodeAddr())) {
                dnIds.add(ctx.getStorageInstId());
            }
        }
        ExecUtils.scanRecoveredTrans(dnIds, executor, exceptions, minTrxId, maxTrxId);
        // Wait at most 30s.
        int retry = 0;
        int maxRetry = 6;
        int waitMilli = 5000;
        while (minTrxId.get() <= maxTrxId.get()) {
            try {
                Thread.sleep(waitMilli);
            } catch (Throwable t) {
                results.add(
                    new Result(Result.Status.ERROR,
                        "Error occurs when draining prepared trx: " + t.getMessage(),
                        "-"));
            }
            AtomicLong tmp = new AtomicLong(Long.MIN_VALUE);
            minTrxId.set(Long.MAX_VALUE);
            ExecUtils.scanRecoveredTrans(dnIds, executor, exceptions, minTrxId, tmp);
            retry++;
            if (retry > maxRetry) {
                break;
            }
        }
        if (minTrxId.get() <= maxTrxId.get()) {
            results.add(
                new Result(Result.Status.ERROR,
                    "Draining prepared trx timeout.",
                    "-"));
        }
        return results;
    }

    private static List<Result> handleAutoSavepointOpt() {
        List<Result> results = new ArrayList<>();
        if (DynamicConfig.getInstance().enableXProtoOptForAutoSp()) {
            try {
                TransactionManager.turnOffAutoSavepointOpt();
                String error = ExecUtils.waitVarChange("xProtoOptForAutoSp", "false", 5);
                if (null != error) {
                    results.add(
                        new Result(Result.Status.ERROR, error, "-"));
                }
            } catch (Throwable t) {
                results.add(new Result(Result.Status.ERROR,
                    "Error occurs when turning off auto savepoint opt: " + t.getMessage(),
                    "-"));
            }
        }
        return results;
    }

    public static Cache<Long, List<Result>> getRisks() {
        return RISKS;
    }

    public static Long versionToCompatibilityLevel(String version) {
        Long level = Long.MAX_VALUE;
        for (Handler handler : HANDLER_LIST) {
            if (compareVersion(version, handler.version) && handler.compatibilityLevel < level) {
                // downgrade to this level
                level = handler.compatibilityLevel;
            }
        }
        return level;
    }

    public static Result addCompatibilityTask(Long level) {
        Result result = new Result(Result.Status.RUNNING,
            "-",
            "Use SHOW COMPATIBILITY_LEVEL " + level + " to get result.");
        // Analyze
        lock.lock();
        try {
            List<Result> tmp = RISKS.getIfPresent(level);
            if (null == tmp || tmp.isEmpty() || !Result.Status.RUNNING.equals(tmp.get(0).status)) {
                RISKS.put(level, Collections.singletonList(result));
            } else {
                // Still running, no need to start a new analyzing task.
                return result;
            }
        } finally {
            lock.unlock();
        }

        singleExecutorService.submit(() -> handle(level));
        return result;
    }

    /**
     * Check risk of downgrading to this version, and form the results.
     */
    private static void handle(Long level) {
        List<Result> results = new ArrayList<>();

        // Find all handlers whose version >= v.
        List<Handler> handlers = new ArrayList<>();
        for (Handler handler : HANDLER_LIST) {
            if (level <= handler.compatibilityLevel) {
                handlers.add(handler);
            }
        }

        // Downgrade from higher level to lower level.
        handlers.sort((o1, o2) -> o2.compatibilityLevel.compareTo(o1.compatibilityLevel));
        for (Handler handler : handlers) {
            List<Result> tmp;
            try {
                tmp = handler.handle.call();
            } catch (Throwable t) {
                tmp = Collections.singletonList(
                    new Result(Result.Status.ERROR,
                        handler.getClass().getSimpleName() + ": " + t.getMessage(),
                        "-"));
            }
            results.addAll(tmp);
        }

        if (results.isEmpty()) {
            results.add(
                new Result(Result.Status.OK,
                    "-",
                    "Finished at " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(
                        Calendar.getInstance().getTime())));
        } else {
            results.add(
                new Result(Result.Status.ERROR,
                    "-",
                    "Finished at " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(
                        Calendar.getInstance().getTime())));
        }

        RISKS.put(level, results);
    }

    /**
     * @return true if v0 <= v1
     */
    private static boolean compareVersion(String v0, String v1) {
        // 0 is major version, 1 is minor version.
        String[] versions0 = v0.split("-");
        String[] versions1 = v1.split("-");
        // 5.4.19 => 5, 4, 19
        int[] majorVersions0 = Arrays.stream(versions0[0].split("\\.")).mapToInt(Integer::parseInt).toArray();
        long minorVersion0 = Long.parseLong(versions0[1]);
        int[] majorVersions1 = Arrays.stream(versions1[0].split("\\.")).mapToInt(Integer::parseInt).toArray();
        long minorVersion1 = Long.parseLong(versions1[1]);
        for (int i = 0; i < 3; i++) {
            if (majorVersions0[i] < majorVersions1[i]) {
                return true;
            } else if (majorVersions0[i] > majorVersions1[i]) {
                return false;
            }
        }
        return minorVersion0 <= minorVersion1;
    }

    public static class Result {
        public Status status;
        public String cause;
        public String solution;

        public enum Status {
            RUNNING,
            ERROR,
            OK
        }

        public Result(Status status, String cause, String solution) {
            this.status = status;
            this.cause = cause;
            this.solution = solution;
        }
    }
}
