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

package com.alibaba.polardbx.optimizer.ccl.service;

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

    void setDataId(String dataId);

    String getDataId();
}
