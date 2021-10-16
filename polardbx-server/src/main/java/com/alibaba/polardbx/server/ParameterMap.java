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

package com.alibaba.polardbx.server;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * Created by simiao.zw on 2014/7/31.
 */
public class ParameterMap {

    private static final Logger logger = LoggerFactory.getLogger(ParameterMap.class);
    private Map<String, String> hm     = TreeMaps.caseInsensitiveMap();
    private ReentrantLock       lock   = new ReentrantLock();

    public ParameterMap(){

    }

    public void insert(String key, String value) {
        lock.lock();

        try {
            if (hm.get(key) == null) {
                hm.put(key, value);
            } else {
                logger.error("insert sm failure " + key);
            }
        } finally {
            lock.unlock();
        }
    }

    public void overwrite(String key, String value) {
        lock.lock();

        try {
            hm.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    public boolean contains(String key) {
        lock.lock();

        try {
            return hm.containsKey(key);
        } finally {
            lock.unlock();
        }
    }

    public String delete(String key) {
        lock.lock();

        try {
            String s = hm.remove(key);
            if (s == null) {
                logger.warn("remove key is null " + key);
                return null;
            } else {
                return s;
            }
        } finally {
            lock.unlock();
        }
    }

    public String find(String key) {
        lock.lock();

        try {
            return hm.get(key);
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        return hm.size();
    }
}
