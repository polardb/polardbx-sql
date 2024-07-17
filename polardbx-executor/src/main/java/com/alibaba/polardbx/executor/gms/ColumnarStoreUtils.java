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

package com.alibaba.polardbx.executor.gms;

public class ColumnarStoreUtils {
    /**
     * The default column index of `pk` (immutable).
     */
    //public static final int PK_COLUMN_INDEX = 1;
    /**
     * The default column index of `tso` (immutable).
     */
    public static final int TSO_COLUMN_INDEX = 0;
    /**
     * The default column index of `pk_tso` (immutable).
     */
    //public static final int PK_TSO_COLUMN_INDEX = 3;
    /**
     * The default column index of `position` (immutable).
     */
    public static final int POSITION_COLUMN_INDEX = 1;

    /**
     * number of implicit column BEFORE real physical column
     */
    public static final int IMPLICIT_COLUMN_CNT = 2;
}
