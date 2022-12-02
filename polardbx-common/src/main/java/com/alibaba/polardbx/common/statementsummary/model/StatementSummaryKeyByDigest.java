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
import lombok.NoArgsConstructor;

/**
 * @author busu
 * date: 2021/11/8 4:16 下午
 */
@Data
@NoArgsConstructor
public class StatementSummaryKeyByDigest {
    private int templateId;
    private int prevTemplateId;
    private String schema;
    private int planHash;

    public StatementSummaryKeyByDigest(String schema, int templateId, int prevTemplateId, int planHash) {
        this.schema = schema;
        this.templateId = templateId;
        this.prevTemplateId = prevTemplateId;
        this.planHash = planHash;
    }

    public static StatementSummaryKeyByDigest create(String schema, int templateId, int prevTemplateId, int planHash) {
        return new StatementSummaryKeyByDigest(schema, templateId, prevTemplateId, planHash);
    }

}
