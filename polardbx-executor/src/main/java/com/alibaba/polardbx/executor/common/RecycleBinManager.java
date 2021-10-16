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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;

import javax.sql.DataSource;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RecycleBinManager {
    private final static Logger logger = LoggerFactory.getLogger(RecycleBinManager.class);
    public static RecycleBinManager instance = new RecycleBinManager();
    public Map<String, RecycleBin> apps;

    private RecycleBinManager() {
        apps = new ConcurrentHashMap<>();

        // mock mode avoid recycle bin
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        //start auto purge
        final ScheduledExecutorService ses =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("AutoPurge", true));
        Random rand = new Random();
        ses.scheduleWithFixedDelay(() -> {
            try {
                for (RecycleBin bin : apps.values()) {
                    bin.purge(true);
                }
            } catch (Throwable e) {
                logger.info("auto purge recycle bin error: " + e);
            }
        }, rand.nextInt(60), 60, TimeUnit.MINUTES);
    }

    public RecycleBin getByAppName(String appName) {
        return apps.get(appName);
    }

    public void initApp(String appName, String schemaName, DataSource dataSource, Map<String, Object> cmds) {
        // mock mode avoid recycle bin
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        if (ConfigDataMode.isMasterMode()) {
            if (apps.get(appName) != null) {
                if (dataSource != null) {
                    apps.get(appName).setDataSource(dataSource);
                }
            }
            RecycleBin rb = new RecycleBin(appName, schemaName, dataSource, cmds);
            apps.putIfAbsent(appName, rb);
            rb = apps.get(appName);
            rb.init();
        }
    }

    public void destroyApp(String appName) {
        RecycleBin rb = apps.remove(appName);
        if (rb != null) {
            rb.destroy();
        }
    }

    public static class RecycleBinParam {
        public String name;
        public String originalName;
        public Date created;

        public RecycleBinParam(String name, String originalName, Date created) {
            this.name = name;
            this.originalName = originalName;
            this.created = created;
        }
    }
}
