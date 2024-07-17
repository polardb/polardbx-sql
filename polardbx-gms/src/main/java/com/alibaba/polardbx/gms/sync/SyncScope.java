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

package com.alibaba.polardbx.gms.sync;

public enum SyncScope {

    ALL, //all cn nodes
    MASTER_ONLY, //the cn nodes on master instance
    SLAVE_ONLY,  //the cn nodes on row-slave&columnar-slave instance
    NOT_COLUMNAR_SLAVE,  //the cn nodes on master&row-slave instance
    ROW_SLAVE_ONLY, //all cn nodes on row-slave instance
    COLUMNAR_SLAVE_ONLY, //all cn nodes on columnar-slave instance
    CURRENT_ONLY; //the current nodes on current instance

    public static SyncScope DEFAULT_SYNC_SCOPE = ALL;

}
