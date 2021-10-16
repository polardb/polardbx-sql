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

package com.alibaba.polardbx.gms.metadb.scheduler;

import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.ClassFinder;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author hongxi.chx
 */
public class MetaDbCleanManager extends AbstractLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(MetaDbCleanManager.class);
    private static MetaDbCleanManager instance = new MetaDbCleanManager();
    private final List<Class> classes = Lists.newArrayList();
    private final ScheduledExecutorService cleanTaskExecutor =
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("MetaDb-Cleaner", true));

    public static MetaDbCleanManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    private MetaDbCleanManager() {
    }

    @Override
    protected void doInit() {
        synchronized (this) {
            try {
                start();
            } catch (IllegalAccessException | InstantiationException e) {
                logger.error("Failed to start meta DB clean task.", e);
            }
        }
    }

    public void start() throws IllegalAccessException, InstantiationException {
        classes.addAll(ClassFinder.getAllClassByInterface(Cleanable.class, "com.alibaba.polardbx.gms.metadb"));
        for (int i = 0; i < classes.size(); i++) {
            Class aClass = classes.get(i);
            Cleanable o = (Cleanable) aClass.newInstance();
            final Task task = new Task(o);

            Date date = new Date();
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int hours = cal.get(Calendar.HOUR_OF_DAY);
            int fixedTime = 4;
            // the first run time, maybe today or tomorrow
            if (hours > fixedTime) {
                cal.add(Calendar.HOUR_OF_DAY, fixedTime - hours + 24);
            } else {
                cal.set(Calendar.HOUR_OF_DAY, fixedTime);
            }
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            cleanTaskExecutor
                .scheduleAtFixedRate(task, (cal.getTime().getTime() - System.currentTimeMillis()) / 1000 / 60 / 60,
                    task.getDelayHours(), TimeUnit.HOURS);
        }
    }

}
