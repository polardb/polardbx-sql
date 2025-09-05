package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.google.common.collect.Maps;
import org.junit.Test;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.gms.metadb.MetaDbDataSource.DEFAULT_META_DB_GROUP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class PlannerTest {

    @Test
    public void testEnableDirectPlanFalse() {
        PlannerContext pc = new PlannerContext();
        pc.getParamManager().getProps().put(ConnectionProperties.ENABLE_DIRECT_PLAN, "false");
        ExecutionPlan.DirectMode mode = new Planner().shouldDirectByTable(null, null, pc, null);

        assertEquals(ExecutionPlan.DirectMode.NONE, mode);
    }

    @Test
    public void testDnHint() {
        PlannerContext pc = new PlannerContext();
        pc.getParamManager().getProps().put(ConnectionProperties.DN_HINT, "test hint");
        ExecutionPlan.DirectMode mode = new Planner().shouldDirectByTable(null, null, pc, null);

        assertEquals(ExecutionPlan.DirectMode.NONE, mode);
    }

    /**
     * cmdBean为空的情况
     */
    @Test
    public void testIsDirectHintWithGroupName_CmdBeanIsNull() {
        assertTrue(!Planner.isDirectHintWithGroupName("schema", null, Maps.newHashMap()));
    }

    /**
     * cmdBean不包含JSON提示的情况
     */
    @Test
    public void testIsDirectHintWithGroupName_CmdBeanNotJsonHint() {
        HintCmdOperator.CmdBean cmdBean = mock(HintCmdOperator.CmdBean.class);
        when(cmdBean.jsonHint()).thenReturn(false);
        assertFalse(Planner.isDirectHintWithGroupName("schema", cmdBean, Maps.newHashMap()));
    }

    /**
     * 直接路由条件的数据库ID为partition name的情况
     */
    @Test
    public void testIsDirectHintWithGroupName_DbIdIsNull() {
        String schema = "test_schema";
        HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(schema, Maps.newHashMap(), "");
        cmdBean.setJson(
            "{'extra':{'MERGE_UNION':'false'},'type':'direct','vtab':'t_school_note','dbid':'p1','realtabs':['t_school_note_6pDG_00001']}");
        assertTrue(!Planner.isDirectHintWithGroupName("schema", cmdBean, Maps.newHashMap()));
    }

    /**
     * 数据库ID包含"GROUP"的情况
     */
    @Test
    public void testIsDirectHintWithGroupName_DbIdContainsGroup() {
        String schema = "test_schema";
        HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(schema, Maps.newHashMap(), "");
        cmdBean.setJson(
            "{'extra':{'MERGE_UNION':'false'},'type':'direct','vtab':'t_school_note','dbid':'xx_group','realtabs':['t_school_note_6pDG_00001']}");
        assertTrue(Planner.isDirectHintWithGroupName("schema", cmdBean, Maps.newHashMap()));
    }

    /**
     * 数据库ID等于 DEFAULT_META_DB_GROUP_NAME 的情况
     */
    @Test
    public void testIsDirectHintWithGroupName_DbIdEqualsDefaultMetaDbGroupName() {
        String schema = "test_schema";
        HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(schema, Maps.newHashMap(), "");
        cmdBean.setJson("{'extra':{'MERGE_UNION':'false'},'type':'direct','vtab':'t_school_note','dbid':'"
            + DEFAULT_META_DB_GROUP_NAME + "','realtabs':['t_school_note_6pDG_00001']}");
        assertTrue(Planner.isDirectHintWithGroupName("schema", cmdBean, Maps.newHashMap()));
    }

    @Test
    public void testForceIndexHint() {
        PlannerContext pc = new PlannerContext();
        pc.setLocalIndexHint(true);
        ExecutionPlan.DirectMode mode = new Planner().shouldDirectByTable(null, null, pc, null);

        assertEquals(ExecutionPlan.DirectMode.NONE, mode);
    }
}
