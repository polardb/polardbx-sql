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

package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.SerializableClassMapper;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * testcase for DdlEngineAccessorDelegate.class and Task Serialize/DeSerialize functions
 */
@Ignore
public class DdlEngineAccessorDelegateTest {

    @BeforeClass
    public static void beforeClass() {
        String addr =
            ConnectionManager.getInstance().getMetaAddress() + ":" + ConnectionManager.getInstance().getMetaPort();
        String dbName = PropertiesUtil.getMetaDB;
        String props = "useUnicode=true&characterEncoding=utf-8&useSSL=false";
        String usr = ConnectionManager.getInstance().getMetaUser();
        String pwd = PropertiesUtil.configProp.getProperty(ConfigConstant.META_PASSWORD);
        MetaDbDataSource.initMetaDbDataSource(addr, dbName, props, usr, pwd);
    }

    @Test
    public void testInsert() {
        /**
         * case 1
         * given: a task
         * when: query it
         * then: gets it
         */
        String schema = "1";
        TestEmptyTask task = new TestEmptyTask(schema);
        Long jobId = IdGenerator.getIdGenerator().nextId();
        Long taskId = IdGenerator.getIdGenerator().nextId();
        task.setJobId(jobId);
        task.setTaskId(taskId);
        SerializableClassMapper.register("EmptyTask", TestEmptyTask.class);
        DdlEngineTaskRecord record = TaskHelper.toDdlEngineTaskRecord(task);
        new DdlEngineAccessorDelegate<Void>() {
            @Override
            protected Void invoke() {
                engineTaskAccessor.insert(Lists.newArrayList(record));
                return null;
            }
        }.execute();
        DdlEngineTaskRecord taskInDB = new DdlEngineAccessorDelegate<DdlEngineTaskRecord>() {
            @Override
            protected DdlEngineTaskRecord invoke() {
                return engineTaskAccessor.query(jobId, taskId);
            }
        }.execute();
        Assert.assertTrue(jobId == taskInDB.jobId);
        Assert.assertTrue(taskId == taskInDB.taskId);
        Assert.assertTrue(schema.equals(taskInDB.schemaName));
        TestEmptyTask deSerializeTask = (TestEmptyTask) TaskHelper.fromDdlEngineTaskRecord(taskInDB);
        Assert.assertTrue(jobId.equals(deSerializeTask.getJobId()));
        Assert.assertTrue(taskId.equals(deSerializeTask.getTaskId()));
        Assert.assertTrue(schema.equals(deSerializeTask.getSchemaName()));
        Assert.assertTrue(deSerializeTask.getState() == DdlTaskState.READY);

        /**
         * case 2
         * given: a task
         * when: update it done
         * then: get it doen
         */
        new DdlEngineAccessorDelegate<Void>() {
            @Override
            protected Void invoke() {
                engineTaskAccessor.updateTaskDone(jobId, taskId);
                return null;
            }
        }.execute();
        DdlEngineTaskRecord doneRecord = new DdlEngineAccessorDelegate<DdlEngineTaskRecord>() {
            @Override
            protected DdlEngineTaskRecord invoke() {
                return engineTaskAccessor.query(jobId, taskId);
            }
        }.execute();
        TestEmptyTask doneTask = (TestEmptyTask) TaskHelper.fromDdlEngineTaskRecord(doneRecord);
        Assert.assertTrue(jobId.equals(doneTask.getJobId()));
        Assert.assertTrue(taskId.equals(doneTask.getTaskId()));
        Assert.assertTrue(schema.equals(doneTask.getSchemaName()));
        Assert.assertTrue(doneTask.getState() == DdlTaskState.SUCCESS);

        /**
         * case 3
         * given: a task
         * when: delete by jobId
         * then: gets null
         */
        new DdlEngineAccessorDelegate<Void>() {
            @Override
            protected Void invoke() {
                engineTaskAccessor.deleteByJobId(jobId);
                return null;
            }
        }.execute();
        DdlEngineTaskRecord nullTask = new DdlEngineAccessorDelegate<DdlEngineTaskRecord>() {
            @Override
            protected DdlEngineTaskRecord invoke() {
                return engineTaskAccessor.query(jobId, taskId);
            }
        }.execute();
        Assert.assertTrue(nullTask == null);

    }

}
