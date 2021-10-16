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

package com.alibaba.polardbx.optimizer.ccl.common;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author busu
 * date: 2021/4/2 11:37 上午
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CclSqlMetric {

    public final static String METRIC_NAME_RESPONSE_TIME = "response_time".toUpperCase();
    public final static String METRIC_NAME_AFFECTED_ROWS = "affected_rows".toUpperCase();
    public final static String METRIC_NAME_FETCH_ROWS = "fetch_rows".toUpperCase();
    public final static String METRIC_NAME_PHY_AFFECTED_ROWS = "phy_affected_rows".toUpperCase();
    public final static String METRIC_NAME_PHYSICAL_SQL_COUNT = "physical_sql_count".toUpperCase();
    public final static String METRIC_NAME_ACTIVE_SESSION = "active_session".toUpperCase();
    public final static String METRIC_SQL_TYPE = "sql_type".toUpperCase();

    public final static long DEFAULT_VALUE = Long.MIN_VALUE;

    private String originalSql;
    private String schemaName;
    private long responseTime = DEFAULT_VALUE;
    private long affectedRows = DEFAULT_VALUE;
    private long fetchRows = DEFAULT_VALUE;
    private long affectedPhyRows = DEFAULT_VALUE;
    private long phySqlCount = DEFAULT_VALUE;
    private long activeSession = DEFAULT_VALUE;
    private long sqlType = DEFAULT_VALUE;
    private List<Pair<Integer, ParameterContext>> params;
    private String templateId;
}

