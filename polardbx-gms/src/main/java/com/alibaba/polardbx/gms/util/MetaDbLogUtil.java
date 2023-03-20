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

package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * @author chenghui.lch
 */
public class MetaDbLogUtil {

    /**
     * Print all the info for server when startup
     * <p>
     * <pre>
     *  This information included by this log is as followed:
     *      1. server port
     *      2. server HTAP port
     *      3. manager port
     *      4. mpp rpc port
     *      5. metaDb addr list
     *      6. mysql url for metaDb
     *      7. local white ip list
     *      9. server pid
     *      10. master/slave mode of server
     *      11. db type of server (such as DRDS/PolarDB-X)
     * </pre>
     */
    public static final Logger START_UP_LOG = LoggerFactory.getLogger("START_UP_LOG");

    /**
     * Print all dynamic configs from the change of opVersion of data_id
     */
    public static final Logger META_DB_DYNAMIC_CONFIG = LoggerFactory.getLogger("TDDL_DYNAMIC_CONFIG");

    /**
     * Print all the log of async running tasks of meta db
     */
    public static final Logger META_DB_LOG = LoggerFactory.getLogger("META_DB_LOG");

    /**
     * Print all the check ha result of ha checker
     */
    public static final Logger CHECK_HA_LOG = LoggerFactory.getLogger("CHECK_HA_LOG");

}
