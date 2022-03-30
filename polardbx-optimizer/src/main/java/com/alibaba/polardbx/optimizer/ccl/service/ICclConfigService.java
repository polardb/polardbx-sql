package com.alibaba.polardbx.optimizer.ccl.service;

import com.alibaba.polardbx.config.ConfigDataHandler;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerRecord;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleRecordsWrapper;
import com.alibaba.polardbx.optimizer.ccl.common.CclTriggerInfo;

import java.util.List;
import java.util.Set;

/**
 * @author busu
 * date: 2020/11/1 10:09 上午
 */
public interface ICclConfigService {

    void refreshWithRules(Set<CclRuleRecord> cclRuleRecords);

    void refreshWithRules(CclRuleRecordsWrapper cclRuleRecordsWrapper);

    void refreshWithTrigger(Set<CclTriggerRecord> cclTriggerRecords);

    List<CclRuleInfo> getCclRuleInfos();

    CclRuleRecordsWrapper getLatestCclRuleRecords();

    void init(ICclService cclService, ICclTriggerService cclTriggerService);

    boolean mightContainKeyword(String keyword);

    boolean isAllFastMatch();

    void reloadConfig();

    List<CclTriggerInfo> getCclTriggerInfos();

    void setConfigDataHandler(ConfigDataHandler configDataHandler);

    ConfigDataHandler getConfigDataHandler();

    void setDataId(String dataId);

    String getDataId();
}
