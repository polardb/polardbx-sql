/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.ha.impl;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class StorageHaManagerTest {
    @Test
    public void testChangePeriod() {
        final StorageHaManager manager = new StorageHaManager();
        manager.adjustStorageHaTaskPeriod(1000);
        Assert.assertEquals(1000, manager.checkStorageTaskPeriod);
    }

    @Test
    public void testTaskRun() {
        final StorageHaManager manager = Mockito.mock(StorageHaManager.class);
        manager.adjustStorageHaTaskPeriod(1000);

        final StorageHaManager.CheckStorageHaTask checkStorageHaTask = new StorageHaManager.CheckStorageHaTask(manager);
        checkStorageHaTask.run();
    }
}
