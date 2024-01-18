package com.alibaba.polardbx.planner.planmanagement;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author fangwu
 */
public class PlanManagerTest {
    static String schema1 = "plan_manager_test_schema1";
    static String schema2 = "plan_manager_test_schema2";
    static String schema3 = "plan_manager_test_schema3";

    @Test
    public void testInvalidateSchema() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        buildSchema(schema1, "xx", false, planManager);
        buildSchema(schema2, "x1", true, planManager);

        System.out.println(planManager.resources());
        Assert.assertTrue(planManager.resources().contains(schema1 + " baseline size:"));
        Assert.assertTrue(planManager.resources().contains(schema2 + " baseline size:"));
        planManager.invalidateSchema(schema1);
        System.out.println(planManager.resources());
        Assert.assertTrue(!planManager.resources().contains(schema1 + " baseline size:"));
        Assert.assertTrue(planManager.resources().contains(schema2 + " baseline size:"));
        planManager.invalidateSchema(schema2);
        System.out.println(planManager.resources());
        Assert.assertTrue(!planManager.resources().contains(schema2 + " baseline size:"));
    }

    @Test
    public void testInvalidateTable() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        buildSchema(schema1, "x1", false, planManager);
        buildSchema(schema2, "x2", false, planManager);
        buildSchema(schema3, "x3", true, planManager);

        Assert.assertTrue(planManager.resources().contains(schema1 + " baseline size:" + 1));
        Assert.assertTrue(planManager.resources().contains(schema2 + " baseline size:" + 1));
        planManager.invalidateTable(schema1, "X1");
        planManager.invalidateTable(schema2, "wrong_table");
        planManager.invalidateTable(schema3, "x3");

        Assert.assertTrue(!planManager.resources().contains(schema1 + " baseline size:"));
        Assert.assertTrue(planManager.resources().contains(schema2 + " baseline size:" + 1));
        Assert.assertTrue(planManager.resources().contains(schema3 + " baseline size:" + 1));

        planManager.invalidateTable(schema3, "X3", true);
        Assert.assertTrue(!planManager.resources().contains(schema3 + " baseline size:" + 1));
    }

    @Test
    public void testInvalidateTableParrallel() throws InterruptedException {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        AtomicBoolean testFailed = new AtomicBoolean(false);
        // test processor work, random add or/invalidate table
        Runnable r = () -> {
            try {
                while (true) {
                    Thread.sleep(10);

                    Random random = new Random();
                    if (random.nextBoolean()) {
                        buildSchema(schema1, "x1", false, planManager);
                        buildSchema(schema2, "x2", false, planManager);
                        buildSchema(schema3, "x3", true, planManager);
                    } else {
                        planManager.invalidateTable(schema1, "X1");
                        planManager.invalidateTable(schema2, "wrong_table");
                        planManager.invalidateTable(schema3, "x3");
                        planManager.invalidateTable(schema3, "X3", true);
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("thread out");
            } catch (Exception e) {
                e.printStackTrace();
                testFailed.set(true);
                Assert.fail("parrallel test fail :" + e.getMessage());
            }
        };

        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            threads[i] = new Thread(r);
            threads[i].start();
        }

        for (int i = 0; i < 100; i++) {
            Thread.sleep(60);
            if (testFailed.get()) {
                for (int j = 0; j < 10; j++) {
                    threads[i].interrupt();
                }
                Assert.fail();
            }
        }

        Arrays.stream(threads).forEach(t -> t.interrupt());
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void buildSchema(String schema, String tableName, boolean hasFixPlan,
                             PlanManager planManager) {
        Map<String, BaselineInfo> schemaMap1 = planManager.getBaselineMap(schema);

        String sql = "select * from " + tableName;
        Set<Pair<String, String>> tableSet = Sets.newHashSet();
        tableSet.add(Pair.of(schema, tableName));
        if (hasFixPlan) {
            schemaMap1.put(sql, BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, tableSet));
        } else {
            schemaMap1.put(sql, BaselineInfoTest.buildBaselineInfoWithoutFixedPlan(sql, tableSet));
        }
    }

}
