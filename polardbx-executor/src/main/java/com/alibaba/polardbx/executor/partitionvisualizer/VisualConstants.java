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

package com.alibaba.polardbx.executor.partitionvisualizer;

/**
 * @author ximing.yd
 */
public class VisualConstants {

    public static final String LAST_ONE_HOURS = "LAST_ONE_HOURS";
    public static final String LAST_SIX_HOURS = "LAST_SIX_HOURS";
    public static final String LAST_ONE_DAYS = "LAST_ONE_DAYS";
    public static final String LAST_THREE_DAYS = "LAST_THREE_DAYS";
    public static final String LAST_SEVEN_DAYS = "LAST_SEVEN_DAYS";

    //存放分区热度信息的用户库、表
    public static final String VISUAL_SCHEMA_NAME = "information_schema";
    public static final String VISUAL_TABLE_NAME = "partitions_heatmap";

    public static final String DUAL_TABLE_NAME = "DUAL";

}
