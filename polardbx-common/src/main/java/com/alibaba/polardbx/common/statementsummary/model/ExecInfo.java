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

package com.alibaba.polardbx.common.statementsummary.model;

import lombok.Data;

/**
 * @author busu
 * date: 2021/11/9 1:51 下午
 */
@Data
public class ExecInfo {
    private long timestamp;
    private boolean slow;
    private boolean internalUser;
    private String schema;
    private String sqlType;
    private String sampleSql;
    private String prevTemplateText;
    private String templateText;
    private String sampleTraceId;
    private String workloadType;
    private String executeMode;
    private int templateHash;
    private int prevTemplateHash;
    private int planHash;
    private int errorCount;
    private long affectedRows;
    private long transTime;
    private long responseTime;
    private long parseTime;
    private long execPlanCpuTime;
    private long physicalTime;
    private long physicalExecCount;
    private long phyFetchRows;
}
