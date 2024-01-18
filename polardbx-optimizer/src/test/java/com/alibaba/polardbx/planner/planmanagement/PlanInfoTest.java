package com.alibaba.polardbx.planner.planmanagement;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.google.common.collect.Maps;
import org.apache.calcite.util.JsonBuilder;
import org.junit.Test;

import java.util.Map;

/**
 * @author fangwu
 */
public class PlanInfoTest {

    @Test
    public void testFixHintEncodeDecode() {
        final JsonBuilder jsonBuilder = new JsonBuilder();
        Map<String, Object> extendMap = Maps.newHashMap();
        String fixHint = "ENABLE_BKA_JOIN=FALSE";
        extendMap.put("FIX_HINT", fixHint);
        PlanInfo planInfo = new PlanInfo(1, "fake plan json", -1L, -1L,
            0, 0L, 0L, true, true, "fake trace id", "",
            jsonBuilder.toJsonString(extendMap), -1);

        Assert.assertTrue(planInfo.getFixHint().equals(fixHint));

        String json = PlanInfo.serializeToJson(planInfo);
        PlanInfo planInfo1 = PlanInfo.deserializeFromJson(json);

        Assert.assertTrue(planInfo1.getFixHint().equals(fixHint));
    }
}
