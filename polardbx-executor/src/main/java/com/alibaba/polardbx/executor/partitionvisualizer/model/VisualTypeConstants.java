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

package com.alibaba.polardbx.executor.partitionvisualizer.model;

import java.util.Arrays;
import java.util.List;

/**
 * @author ximing.yd
 */
public class VisualTypeConstants {

    public static final String READ_ROWS = "READ_ROWS";
    public static final String WRITTEN_ROWS = "WRITTEN_ROWS";
    public static final String READ_WRITTEN_ROWS = "READ_WRITTEN_ROWS";

    public static final String READ_ROWS_WITH_DN = "READ_ROWS_WITH_DN";
    public static final String WRITTEN_ROWS_WITH_DN = "WRITTEN_ROWS_WITH_DN";
    public static final String READ_WRITTEN_ROWS_WITH_DN = "READ_WRITTEN_ROWS_WITH_DN";

    public static final List<String> TYPE_WITH_DN_OPTIONS = Arrays.asList(READ_ROWS_WITH_DN, WRITTEN_ROWS_WITH_DN,
        READ_WRITTEN_ROWS_WITH_DN);

    public static final List<String> TYPE_OPTIONS = Arrays.asList(READ_ROWS, WRITTEN_ROWS, READ_WRITTEN_ROWS,
        READ_ROWS_WITH_DN, WRITTEN_ROWS_WITH_DN, READ_WRITTEN_ROWS_WITH_DN);

}
