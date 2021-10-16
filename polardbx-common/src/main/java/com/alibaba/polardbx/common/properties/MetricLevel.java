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

package com.alibaba.polardbx.common.properties;

public enum MetricLevel {

    DEFAULT(0),
    SQL(1),
    PIPELINE(2),
    OPERATOR(3);

    MetricLevel(int metricLevel) {
        this.metricLevel = metricLevel;
    }

    public final int metricLevel;

    public static int convertFromString(String strLevel) {
        if ("default".equalsIgnoreCase(strLevel)) {
            return 0;
        } else if ("sql".equalsIgnoreCase(strLevel)) {
            return 1;
        } else if ("pipeline".equalsIgnoreCase(strLevel)) {
            return 2;
        } else if ("operator".equalsIgnoreCase(strLevel)) {
            return 3;
        } else {
            return 0;
        }
    }

    public static String convertValueToString(int metricLevel) {
        switch (metricLevel) {
        case 0:
            return "default";
        case 1:
            return "sql";
        case 2:
            return "pipeline";
        case 3:
            return "operator";
        default:
            return "default";
        }
    }

    public static boolean isSQLMetricEnabled(int metricLevel) {
        return metricLevel >= MetricLevel.SQL.metricLevel;
    }

    public static boolean isPipelineMetricEnabled(int metricLevel) {
        return metricLevel >= MetricLevel.PIPELINE.metricLevel;
    }

    public static boolean isOperatorMetricEnabled(int metricLevel) {
        return metricLevel >= MetricLevel.OPERATOR.metricLevel;
    }
}