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

package com.alibaba.polardbx.optimizer.ccl.service.impl;

import com.alibaba.polardbx.config.ConfigDataHandler;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleRecordsWrapper;
import com.alibaba.polardbx.optimizer.ccl.common.CclTriggerInfo;
import com.alibaba.polardbx.optimizer.ccl.common.RescheduleTask;
import com.alibaba.polardbx.optimizer.ccl.service.ICclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclTriggerService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.utils.CclUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.commons.io.Charsets.UTF_8;

/**
 * @author busu
 * date: 2020/11/1 10:33 上午
 */
@Slf4j
public class CclConfigService implements ICclConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CclConfigService.class);

    private volatile List<CclRuleInfo> cclRuleInfoList;

    protected volatile ICclService cclService;

    protected volatile ICclTriggerService cclTriggerService;

    private volatile BloomFilter<String> bloomFilter;

    private volatile boolean allFastMatch;

    private volatile boolean hasRescheduleRule;

    private volatile List<CclTriggerInfo> cclTriggerInfoList;

    private volatile List<CclRuleRecord> latestCclRuleRecords;
    private volatile long latestVersion;

    private ConfigDataHandler configDataHandler;

    private String dataId;

    /**
     * the thread scheduling the task of checking timeout reschedule task.
     */
    private ScheduledExecutorService TIMEOUT_CHECK_EXECUTOR;

    /**
     * the thread scheduling the task of process trigger task.
     */
    private ScheduledExecutorService TRIGGER_TASK_EXECUTOR;

    public CclConfigService() {
        this.cclRuleInfoList = Lists.newArrayListWithCapacity(0);
        bloomFilter = newBloomFilter(1);

        this.cclTriggerInfoList = Lists.newArrayListWithCapacity(0);
    }

    private void startRescheduleTimeoutCheckTask() {

        TIMEOUT_CHECK_EXECUTOR =
            new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("ccl-timeout-check-thread-pool", true));
        String cclRescheduleTimeoutCheckPeriodPropValue = System.getProperty("cclRescheduleTimeoutCheckPeriod");
        int cclRescheduleTimeoutCheckPeriod = 2;
        if (!StringUtils.isNotEmpty(cclRescheduleTimeoutCheckPeriodPropValue)) {
            cclRescheduleTimeoutCheckPeriod = Integer.parseInt(cclRescheduleTimeoutCheckPeriodPropValue);
        }
        TIMEOUT_CHECK_EXECUTOR.scheduleAtFixedRate(() -> {
            try {
                List<CclRuleInfo> currentCclRuleInfoList = cclRuleInfoList;
                if (CollectionUtils.isNotEmpty(currentCclRuleInfoList)) {
                    long ts = System.currentTimeMillis();
                    for (CclRuleInfo<RescheduleTask> cclRuleInfo : currentCclRuleInfoList) {
                        if (cclRuleInfo.isReschedule()) {
                            RescheduleTask rescheduleTask = cclRuleInfo.getWaitQueue().peek();
                            while (rescheduleTask != null) {
                                RescheduleTask polledRescheduleTask = cclRuleInfo.getWaitQueue().poll();
                                long deadline =
                                    rescheduleTask.getWaitStartTs() + cclRuleInfo.getCclRuleRecord().waitTimeout * 1000
                                        + CclUtils.generateNoise();
                                if (rescheduleTask == polledRescheduleTask && deadline < ts) {
                                    rescheduleTask.getReschedulable().setRescheduled(false, null);
                                    rescheduleTask.getCclRuleInfo().getCclRuntimeStat().killedCount.getAndIncrement();
                                    rescheduleTask.getReschedulable()
                                        .handleRescheduleError(new TddlRuntimeException(ErrorCode.ERR_CCL, String
                                            .format(
                                                ICclService.CCL_WAIT_TIMEOUT_MESSAGE_FORMAT,
                                                cclRuleInfo.getCclRuleRecord().parallelism,
                                                cclRuleInfo.getCclRuleRecord().id,
                                                (ts - rescheduleTask.getWaitStartTs())),
                                            null));
                                    cclRuleInfo.getStayCount().decrementAndGet();
                                } else {
                                    cclRuleInfo.getWaitQueue().addFirst(polledRescheduleTask);
                                    //并使尝试唤醒
                                    CclUtils.tryPollWaitQueue(cclRuleInfo);
                                    break;
                                }
                                rescheduleTask = cclRuleInfo.getWaitQueue().peek();
                            }
                        }
                    }
                }
            } catch (Throwable e) {
                log.error("Fail to check timeout of ccl queue", e);
            }
        }, 0, cclRescheduleTimeoutCheckPeriod, TimeUnit.SECONDS);
    }

    private void endRescheduleTimeoutCheckTask() {
        if (TIMEOUT_CHECK_EXECUTOR != null && !TIMEOUT_CHECK_EXECUTOR.isShutdown()) {
            TIMEOUT_CHECK_EXECUTOR.shutdownNow();
            try {
                TIMEOUT_CHECK_EXECUTOR.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                LOGGER.error("An exception occurs when waiting for termination.", e);
            }
            TIMEOUT_CHECK_EXECUTOR = null;
        }
    }

    @Override
    public synchronized void refreshWithRules(Set<CclRuleRecord> cclRuleRecords) {
        if (cclRuleRecords == null) {
            cclRuleRecords = Collections.EMPTY_SET;
        }

        long startTime = System.nanoTime();

        Set<CclRuleInfo> latestCclRuleInfoSet = Sets.newHashSetWithExpectedSize(cclRuleRecords.size());
        for (CclRuleRecord cclRuleRecord : cclRuleRecords) {
            try {
                CclRuleInfo cclRuleInfo = CclRuleInfo.create(cclRuleRecord);
                latestCclRuleInfoSet.add(cclRuleInfo);
            } catch (Exception e) {
                LOGGER.error(
                    MessageFormat.format("Failed to create CclRuleInfo cclRuleRecord {0}", cclRuleRecord.toString()),
                    e);
            }
        }

        Set<CclRuleInfo> oldCclRuleInfoSet = Sets.newHashSet(cclRuleInfoList);

        //disable dropped rule info
        Set<CclRuleInfo> droppedRuleInfoSet = Sets.difference(oldCclRuleInfoSet, latestCclRuleInfoSet);

        // recycle ccl rule
        for (CclRuleInfo cclRuleInfo : droppedRuleInfoSet) {
            disableRule(cclRuleInfo);
        }
        LOGGER.info(
            "recycle rules " + droppedRuleInfoSet.stream().map((e) -> e.getCclRuleRecord().id).collect(
                Collectors.toList()));

        //enable created rule info
        Set<CclRuleInfo> createdRuleInfoSet = Sets.difference(latestCclRuleInfoSet, oldCclRuleInfoSet);
        for (CclRuleInfo cclRuleInfo : createdRuleInfoSet) {
            enableRule(cclRuleInfo);
        }

        //result set
        Set<CclRuleInfo> resultSet = Sets.newHashSet(createdRuleInfoSet);
        resultSet.addAll(Sets.intersection(oldCclRuleInfoSet, latestCclRuleInfoSet));
        List<CclRuleInfo> resultList = Lists.newArrayList(resultSet);
        resultList.sort(Comparator.comparing(CclRuleInfo::getOrderValue));

        //to cause no error when it is in a medium state by adding new keywords to the current bloomfilter
        for (CclRuleInfo cclRuleInfo : resultList) {
            if (CollectionUtils.isNotEmpty(cclRuleInfo.getKeywords())) {
                List<String> keywords = cclRuleInfo.getKeywords();
                for (String keyword : keywords) {
                    bloomFilter.put(keyword);
                }
            }

            Map<Integer, Object> posParamMap = cclRuleInfo.getPosParamMap();
            if (posParamMap != null && !posParamMap.isEmpty()) {
                for (Object paramValue : posParamMap.values()) {
                    bloomFilter.put(String.valueOf(paramValue));
                }
            }
        }

        allFastMatch = false;
        cclRuleInfoList = resultList;
        this.latestCclRuleRecords = Lists.newArrayList(cclRuleRecords);
        boolean allFastMatchTmp = true;
        for (CclRuleInfo cclRuleInfo : cclRuleInfoList) {
            if (cclRuleInfo.getCclRuleRecord().fastMatch <= 0) {
                allFastMatchTmp = false;
                break;
            }
        }
        allFastMatch = allFastMatchTmp;

        int kwdCount = 0;
        for (CclRuleInfo cclRuleInfo : resultList) {
            if (CollectionUtils.isNotEmpty(cclRuleInfo.getKeywords())) {
                kwdCount += cclRuleInfo.getKeywords().size();
            }
        }

        BloomFilter<String> newBloomFilter = newBloomFilter(kwdCount);
        for (CclRuleInfo cclRuleInfo : resultList) {
            if (CollectionUtils.isNotEmpty(cclRuleInfo.getKeywords())) {
                List<String> keywords = cclRuleInfo.getKeywords();
                for (String keyword : keywords) {
                    newBloomFilter.put(keyword);
                }
            }
            Map<Integer, Object> posParamMap = cclRuleInfo.getPosParamMap();
            if (posParamMap != null && !posParamMap.isEmpty()) {
                for (Object paramValue : posParamMap.values()) {
                    newBloomFilter.put(String.valueOf(paramValue));
                }
            }
        }
        bloomFilter = newBloomFilter;

        cclService.clearCache();
        LOGGER.info(String.format("refresh ccl rule config cost %d ms", (System.nanoTime() - startTime) / 1000000));

        boolean currentHasRescheduleRule = checkRescheduleRule();
        if (currentHasRescheduleRule && !this.hasRescheduleRule) {
            startRescheduleTimeoutCheckTask();
        } else if (!currentHasRescheduleRule && this.hasRescheduleRule) {
            endRescheduleTimeoutCheckTask();
        }
        this.hasRescheduleRule = currentHasRescheduleRule;
    }

    @Override
    public synchronized void refreshWithRules(CclRuleRecordsWrapper cclRuleRecordsWrapper) {
        if (cclRuleRecordsWrapper.getVersion() > latestVersion && cclRuleRecordsWrapper.getCclRuleRecords() != null) {
            refreshWithRules(Sets.newHashSet(cclRuleRecordsWrapper.getCclRuleRecords()));
            latestVersion = cclRuleRecordsWrapper.getVersion();
        }
    }

    private void startProcessTriggerTask() {
        TRIGGER_TASK_EXECUTOR =
            new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("ccl-process-trigger-task-thread-pool", true));

        int processCclTriggerPeriod = 2;
        String processCclTriggerPeriodPropValue = System.getProperty("processCclTriggerPeriod");
        if (!StringUtils.isNotEmpty(processCclTriggerPeriodPropValue)) {
            processCclTriggerPeriod = Integer.parseInt(processCclTriggerPeriodPropValue);
        }
        TRIGGER_TASK_EXECUTOR.scheduleAtFixedRate(() -> {
            try {
                long startTs = System.nanoTime();
                LOGGER.debug("begin process ccl trigger samples");

                this.cclTriggerService.processSamples();

                long endTs = System.nanoTime();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.info(MessageFormat
                        .format("end process ccl trigger samples, time cost {0} ms ", (endTs - startTs) / 1000_000));
                }
            } catch (Throwable throwable) {
                LOGGER.error("Failed to process ccl trigger samples", throwable);
            }
        }, 0, processCclTriggerPeriod, TimeUnit.SECONDS);
    }

    private void endProcessTriggerTask() {
        if (TRIGGER_TASK_EXECUTOR != null && !TRIGGER_TASK_EXECUTOR.isShutdown()) {
            TRIGGER_TASK_EXECUTOR.shutdownNow();
            try {
                TRIGGER_TASK_EXECUTOR.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                LOGGER.error("An exception occurs when waiting for termination.", e);
            }
            TRIGGER_TASK_EXECUTOR = null;
        }
        this.cclTriggerService.clearSamples();
    }

    @Override
    public synchronized void refreshWithTrigger(Set<CclTriggerRecord> cclTriggerRecords) {
        if (CollectionUtils.isEmpty(cclTriggerRecords)) {
            cclTriggerRecords = Sets.newHashSet();
        }

        long startTime = System.nanoTime();
        Set<CclTriggerInfo> latestCclTriggerInfoSet = Sets.newHashSet();
        for (CclTriggerRecord cclTriggerRecord : cclTriggerRecords) {
            try {
                CclTriggerInfo cclTriggerInfo = CclTriggerInfo.create(cclTriggerRecord);
                latestCclTriggerInfoSet.add(cclTriggerInfo);
            } catch (Throwable throwable) {
                LOGGER.error(MessageFormat
                    .format("Fail to create ccl trigger info using the ccl trigger record whose name is {0}",
                        cclTriggerRecord.id), throwable);
            }
        }

        Set<CclTriggerInfo> oldCclTriggerInfoSet = Sets.newHashSet(this.cclTriggerInfoList);

        Set<CclTriggerInfo> droppedTriggerInfoSet = Sets.difference(oldCclTriggerInfoSet, latestCclTriggerInfoSet);
        for (CclTriggerInfo cclTriggerInfo : droppedTriggerInfoSet) {
            disableTrigger(cclTriggerInfo);
        }
        log.info("recycle trigger info {}",
            droppedTriggerInfoSet.stream().map((e) -> e.getTriggerRecord().id).collect(Collectors.toList()));

        Set<CclTriggerInfo> createdTriggerInfoSet = Sets.difference(latestCclTriggerInfoSet, oldCclTriggerInfoSet);
        for (CclTriggerInfo cclTriggerInfo : createdTriggerInfoSet) {
            enableTrigger(cclTriggerInfo);
        }
        log.info("create trigger info {}",
            createdTriggerInfoSet.stream().map((e) -> e.getTriggerRecord().id).collect(Collectors.toList()));

        //根据priority排序
        Set<CclTriggerInfo> resultSet = Sets.newHashSet(createdTriggerInfoSet);
        resultSet.addAll(Sets.intersection(oldCclTriggerInfoSet, latestCclTriggerInfoSet));
        List<CclTriggerInfo> resultList = Lists.newArrayList(resultSet);
        resultList.sort(Comparator.comparing(CclTriggerInfo::getOrderValue));

        if (CollectionUtils.isNotEmpty(this.cclTriggerInfoList) && CollectionUtils.isEmpty(resultList)) {
            endProcessTriggerTask();
        } else if (CollectionUtils.isEmpty(this.cclTriggerInfoList) && CollectionUtils.isNotEmpty(resultList)) {
            this.cclTriggerService.startWorking();
            startProcessTriggerTask();
        }

        this.cclTriggerInfoList = resultList;

        log.info("refresh ccl trigger config cost {} ms", (System.nanoTime() - startTime) / 1000000);
    }

    private void registerConfigListener() {
        String dataId = MetaDbDataIdBuilder.getCclRuleDataId(InstIdUtil.getInstId());
        MetaDbConfigManager.getInstance().register(dataId, null);
        CclRuleListener listener = new CclRuleListener();
        MetaDbConfigManager.getInstance().bindListener(dataId, listener);
    }

    private boolean checkRescheduleRule() {
        if (CollectionUtils.isNotEmpty(cclRuleInfoList)) {
            for (CclRuleInfo cclRuleInfo : cclRuleInfoList) {
                if (cclRuleInfo.isReschedule()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public List<CclRuleInfo> getCclRuleInfos() {
        return this.cclRuleInfoList;
    }

    @Override
    public synchronized CclRuleRecordsWrapper getLatestCclRuleRecords() {
        CclRuleRecordsWrapper cclRuleRecordsWrapper = new CclRuleRecordsWrapper();
        cclRuleRecordsWrapper.setVersion(this.latestVersion);
        cclRuleRecordsWrapper.setCclRuleRecords(this.latestCclRuleRecords);
        return cclRuleRecordsWrapper;
    }

    @Override
    public List<CclTriggerInfo> getCclTriggerInfos() {
        return this.cclTriggerInfoList;
    }

    @Override
    public void setConfigDataHandler(ConfigDataHandler configDataHandler) {
        this.configDataHandler = configDataHandler;
    }

    @Override
    public ConfigDataHandler getConfigDataHandler() {
        return this.configDataHandler;
    }

    @Override
    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    @Override
    public String getDataId() {
        return this.dataId;
    }

    private void enableRule(CclRuleInfo cclRuleInfo) {
        cclRuleInfo.setEnabled(true);
    }

    private void disableRule(CclRuleInfo cclRuleInfo) {

        //force execution order.
        // step 1: set flag
        // step 2: poll queue

        cclRuleInfo.setEnabled(false);
        cclRuleInfo.setMaxStayCount(Integer.MAX_VALUE);
        cclRuleInfo.getCclRuleRecord().queueSize = Integer.MAX_VALUE;
        cclRuleInfo.getCclRuleRecord().parallelism = Integer.MAX_VALUE;
        cclService.invalidateCclRule(cclRuleInfo);
    }

    private void enableTrigger(CclTriggerInfo cclTriggerInfo) {
        cclTriggerInfo.setEnabled(true);
    }

    private void disableTrigger(CclTriggerInfo cclTriggerInfo) {
        cclTriggerInfo.setEnabled(false);
    }

    @Override
    public synchronized void reloadConfig() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            //refresh ccl rules
            CclRuleAccessor cclRuleAccessor = new CclRuleAccessor();
            cclRuleAccessor.setConnection(metaDbConn);
            List<CclRuleRecord> allRecords = cclRuleAccessor.query();
            refreshWithRules(Sets.newHashSet(allRecords));
            LOGGER.info("finish reload ccl rules");

            //refresh ccl triggers
            CclTriggerAccessor cclTriggerAccessor = new CclTriggerAccessor();
            cclTriggerAccessor.setConnection(metaDbConn);
            List<CclTriggerRecord> allTriggerRecords = cclTriggerAccessor.query();
            refreshWithTrigger(Sets.newHashSet(allTriggerRecords));
            LOGGER.info("finish reload ccl trigger");
        } catch (Exception e) {
            LOGGER.error("failed to reload ccl rules", e);
        }

    }

    @Override
    public void init(ICclService cclService, ICclTriggerService cclTriggerService) {
        this.cclService = cclService;
        this.cclTriggerService = cclTriggerService;
        this.dataId = MetaDbDataIdBuilder.getCclRuleDataId(InstIdUtil.getInstId());
        registerConfigListener();
        reloadConfig();
    }

    private class CclRuleListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            LOGGER.info(String.format("start reload ccl rules , newOpVersion: %d", newOpVersion));
            reloadConfig();
        }

    }

    private BloomFilter<String> newBloomFilter(int expectedSize) {

        return BloomFilter.create(new Funnel<String>() {
            @Override
            public void funnel(String str, PrimitiveSink primitiveSink) {
                primitiveSink.putString(str, UTF_8);
            }
        }, expectedSize);
    }

    @Override
    public boolean mightContainKeyword(String keyword) {
        return bloomFilter.mightContain(keyword);
    }

    @Override
    public boolean isAllFastMatch() {
        return this.allFastMatch;
    }

}
