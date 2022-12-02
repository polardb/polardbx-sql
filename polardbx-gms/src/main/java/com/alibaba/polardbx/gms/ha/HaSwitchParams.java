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

package com.alibaba.polardbx.gms.ha;

import com.alibaba.polardbx.gms.ha.impl.StorageNodeHaInfo;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfig;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author chenghui.lch
 */
public class HaSwitchParams {

    public String userName;
    public String passwdEnc;
    public String phyDbName;
    public String curAvailableAddr;
    public int xport = -1; // <0 means not available. =0 means default. >0 means selected.
    public ConnPoolConfig storageConnPoolConfig;
    public Map<String, StorageNodeHaInfo> storageHaInfoMap;
    public int storageKind;
    public String storageInstId;
    public String instId;

    /**
     * When haLock != null, that mean need unlock manually
     */
    public boolean autoUnlock = true;
    public ReentrantReadWriteLock haLock;

    public HaSwitchParams() {
    }

}
