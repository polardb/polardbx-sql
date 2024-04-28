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

package com.alibaba.polardbx.gms.listener;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public interface ConfigManager {

    /**
     * Bind a config listener to dataId,
     * If the dataId does NOT exist in MetaDB, then this bind will be ignored.
     * <p>
     * After binding, it will begin to subscribe and handle all changes of the dataId by the listener.
     * <p>
     * Notice:
     * 1. dataId and config listener are in a one-to-one relationship
     * 2. make sure that dataId has been registered (use register(...) )
     * before binding listener( use bindListener(...)).
     *
     * @param dataId a dataId that the caller defines
     * @param listener a config listener that the caller uses to handle its own config data,
     * e.g. typically refresh cache of config data on each server node
     */
    void bindListener(String dataId, ConfigListener listener);

    /**
     * Bind a config listener to dataId,
     * If the dataId does NOT exist in MetaDB, then continue register the bind.
     * <p>
     * After binding, it will begin to subscribe and handle all changes of the dataId by the listener.
     * <p>
     * Notice:
     * 1. dataId and config listener are in a one-to-one relationship
     * 2. make sure that dataId has been registered (use register(...) )
     * before binding listener( use bindListener(...)).
     *
     * @param dataId a dataId that the caller defines
     * @param opVersion the verison
     * @param listener a config listener that the caller uses to handle its own config data,
     * e.g. typically refresh cache of config data on each server node
     */
    void bindListener(String dataId, long opVersion, ConfigListener listener);

    /**
     * Batch bind listeners to dataIds with same prefix
     * Notice: count of dataIds start with dataIdPrefix must equals to listeners.size
     *
     * @param dataIdPrefix dataId prefix
     * @param listeners config listeners that the caller uses to handle its own config data,
     */
    void bindListeners(String dataIdPrefix, Map<String, ConfigListener> listeners);

    /**
     * Register a dataId into metaDB by using a transaction connection,
     * if the dataId already exists in metaDB, then ignored.
     * <p>
     * Notice:
     * if trxConn == null, then register a dataId into metaDB by using a new transaction connection
     */
    void register(String dataId, Connection trxConn);

    /**
     * Unbind a config listener from dataId.
     * If the dataId does NOT exists, then ignored
     * <p>
     * After unbinding, it will begin to ignore all changes of the dataId,
     * but the dataId is still in MetaDB.
     *
     * @param dataId a dataId that the caller defines
     */
    void unbindListener(String dataId);

    /**
     * Unregister a dataId from MetaDB by using an existing transaction
     * <p>
     * After unregistering, the dataId will be set the status of REMOVED, and its change will be ignored and
     * will be clean by the async clean task of ConfigManager
     * <p>
     * Notice:
     * if trxConn == null, then unregister a dataId from metaDB by using a new transaction connection
     */
    void unregister(String dataId, Connection trxConn);

    /**
     * Notify a new config change of the dataId by updating its opVersion of MetaDb under an existing transaction,
     * and return the final opVersion
     * <p>
     * Notice:
     * if trxConn == null, then upgrade the opVersion of the dataId by using a new transaction connection
     *
     * @param dataId a dataId that the caller defines
     * @param trxConn a existing transaction connection
     * @return new generated version
     */
    long notify(String dataId, Connection trxConn);

    /**
     * Notify a new config change of multiple dataIds by updating their opVersion of MetaDb under an existing transaction,
     * and return the final opVersions
     * <p>
     * Notice:
     * if trxConn == null, then upgrade the opVersion of the dataId by using a new transaction connection
     *
     * @param dataIds a list of dataId that the caller defines
     * @param trxConn a existing transaction connection
     * @param ignoreCntError check the update count or not.
     */
    void notifyMultiple(List<String> dataIds, Connection trxConn, boolean ignoreCntError);

    /**
     * Synchronize the config change of dataId to other server nodes
     * by calling the listener of dataId that is bind before.
     */
    void sync(String dataId);

    /**
     * Synchronize other server nodes
     * to unbind the listener of dataId.
     */
    void syncUnbind(String dataId);

    /**
     * Synchronize sync the listener on current Node. Don't update the version.
     */
    boolean localSync(String dataId);
}
