package com.alibaba.polardbx.executor.ddl.newengine.meta;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;

import org.mockito.junit.MockitoJUnitRunner;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@RunWith(MockitoJUnitRunner.class)
public class DdlJobManagerTest {

    private DdlEngineAccessor engineAccessor;

    private DdlEngineTaskAccessor engineTaskAccessor;

    private Connection mockConnection;
    private MockedStatic<MetaDbUtil> mockMetaDbUtil;
    private MockedConstruction<DdlEngineAccessor> mockEngineCtor;
    private MockedConstruction<DdlEngineTaskAccessor> mockTaskCtor;

    @Before
    public void setUp() throws Exception {
        // 创建 Connection 的模拟
        mockConnection = mock(Connection.class);

        engineAccessor = mock(DdlEngineAccessor.class);

        engineTaskAccessor = mock(DdlEngineTaskAccessor.class);

        mockMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class); // 模拟静态方法所在的类

        // 设置期望的静态方法行为
        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenAnswer(i -> mockConnection);

        mockEngineCtor = Mockito.mockConstruction(DdlEngineAccessor.class, (mock, context) -> {
            Mockito.when(mock.queryArchive(anyString()))
                .thenAnswer(i -> engineAccessor.queryArchive(i.getArgument(0)));
            Mockito.when(mock.deleteArchive(anyLong()))
                .thenAnswer(i -> engineAccessor.deleteArchive(i.getArgument(0)));
            Mockito.when(mock.queryOutdateArchiveDDLEngine(anyLong()))
                .thenAnswer(i -> engineAccessor.queryOutdateArchiveDDLEngine(i.getArgument(0)));
            Mockito.when(mock.query(anyLong()))
                .thenAnswer(i -> engineAccessor.query((long) i.getArgument(0)));
            Mockito.when(mock.delete(anyLong()))
                .thenAnswer(i -> engineAccessor.delete(i.getArgument(0)));
        });
        mockTaskCtor = Mockito.mockConstruction(DdlEngineTaskAccessor.class, (mock, context) -> {
            Mockito.when(mock.deleteArchiveByJobId(anyLong()))
                .thenAnswer(i -> engineTaskAccessor.deleteArchiveByJobId(i.getArgument(0)));
            Mockito.when(mock.deleteByJobId(anyLong()))
                .thenAnswer(i -> engineTaskAccessor.deleteByJobId(i.getArgument(0)));
        });
    }

    @After
    public void cleanUp() {
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }
        if (mockEngineCtor != null) {
            mockEngineCtor.close();
        }
        if (mockTaskCtor != null) {
            mockTaskCtor.close();
        }
    }

    /**
     * 测试清理指定schema归档（测试用例4：schema存在记录，预期结果：查询并删除记录）
     */
    @Test
    public void testCleanUpArchiveSchema_withSchemaRecords() {
        List<DdlEngineRecord> records = new ArrayList<>();
        records.add(mock(DdlEngineRecord.class));
        when(engineAccessor.queryArchive(anyString())).thenReturn(records);
        when(engineAccessor.deleteArchive(anyLong())).thenReturn(1);
        when(engineTaskAccessor.deleteArchiveByJobId(anyLong())).thenReturn(1);

        int result = DdlJobManager.cleanUpArchiveSchema("existing_schema");
        Assert.assertEquals(0, result);
        verify(engineAccessor, times(1)).queryArchive("existing_schema");
        verify(engineAccessor, times(1)).deleteArchive(anyLong());
        verify(engineTaskAccessor, times(1)).deleteArchiveByJobId(anyLong());
    }

    @Test
    public void testCleanUpArchive_minutes() {
        List<DdlEngineRecord> records = new ArrayList<>();
        records.add(mock(DdlEngineRecord.class));
        when(engineAccessor.queryOutdateArchiveDDLEngine(anyLong())).thenReturn(records);
        when(engineAccessor.deleteArchive(anyLong())).thenReturn(1);
        when(engineTaskAccessor.deleteArchiveByJobId(anyLong())).thenReturn(1);

        DdlJobManager manager = new DdlJobManager();
        int result = manager.cleanUpArchive(0l);
        Assert.assertEquals(0, result);
        verify(engineAccessor, times(1)).queryOutdateArchiveDDLEngine(0l);
        verify(engineAccessor, times(1)).deleteArchive(anyLong());
        verify(engineTaskAccessor, times(1)).deleteArchiveByJobId(anyLong());
    }

    @Test
    public void testRemoveJob_withoutSubJobs() {
        List<DdlEngineRecord> records = new ArrayList<>();
        records.add(mock(DdlEngineRecord.class));
        when(engineTaskAccessor.deleteByJobId(anyLong())).thenReturn(1);
        DdlEngineRecord ddlEngineRecord = mock(DdlEngineRecord.class);
        ddlEngineRecord.state = DdlState.COMPLETED.name();
        when(engineAccessor.query(anyLong())).thenReturn(ddlEngineRecord);
        when(engineAccessor.delete(anyLong())).thenReturn(1);

        DdlJobManager manager = new DdlJobManager();
        DdlJobManager spyManager = Mockito.spy(manager);
        doCallRealMethod().when(spyManager).removeJob(anyLong());

        List<SubJobTask> emptyList = new ArrayList<>();
        Mockito.doReturn(emptyList).when(spyManager)
            .fetchSubJobsRecursive(anyLong(), any(DdlEngineTaskAccessor.class), anyBoolean());
        Mockito.doNothing().when(spyManager).validateDdlStateContains(any(), any());

        Assert.assertEquals(true, spyManager.removeJob(1L));
        verify(engineTaskAccessor, times(1)).deleteByJobId(1L);
        verify(engineAccessor, times(1)).query(1L);
        verify(spyManager, times(1)).fetchSubJobsRecursive(anyLong(), any(DdlEngineTaskAccessor.class), anyBoolean());
        verify(engineAccessor, times(1)).delete(1L);
    }

    /**
     * 有子任务，删除主任务及所有子任务
     */
    @Test
    public void testRemoveJob_withSubJobs() {
        List<DdlEngineRecord> records = new ArrayList<>();
        records.add(mock(DdlEngineRecord.class));
        when(engineTaskAccessor.deleteByJobId(anyLong())).thenReturn(1);
        DdlEngineRecord ddlEngineRecord = mock(DdlEngineRecord.class);
        ddlEngineRecord.state = DdlState.COMPLETED.name();
        when(engineAccessor.query(anyLong())).thenReturn(ddlEngineRecord);
        when(engineAccessor.delete(anyLong())).thenReturn(1);

        DdlJobManager manager = new DdlJobManager();
        DdlJobManager spyManager = Mockito.spy(manager);
        doCallRealMethod().when(spyManager).removeJob(anyLong());

        List<SubJobTask> subJobTasks = new ArrayList<>();
        SubJobTask subJobTask = new SubJobTask("", "", 10, "", 101);
        subJobTasks.add(subJobTask);
        subJobTask = new SubJobTask("", "", 11, "", 111);
        subJobTasks.add(subJobTask);
        Mockito.doReturn(subJobTasks).when(spyManager)
            .fetchSubJobsRecursive(anyLong(), any(DdlEngineTaskAccessor.class), anyBoolean());
        Mockito.doNothing().when(spyManager).validateDdlStateContains(any(), any());

        Assert.assertEquals(true, spyManager.removeJob(1L));
        verify(engineTaskAccessor, times(5)).deleteByJobId(anyLong());
        verify(engineAccessor, times(1)).query(1L);
        verify(spyManager, times(1)).fetchSubJobsRecursive(anyLong(), any(DdlEngineTaskAccessor.class), anyBoolean());
        verify(engineAccessor, times(5)).delete(anyLong());
    }
}
