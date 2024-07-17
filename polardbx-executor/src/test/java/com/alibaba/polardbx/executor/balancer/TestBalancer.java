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

package com.alibaba.polardbx.executor.balancer;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.balancer.action.ActionLockResource;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.balancer.action.ActionMergePartition;
import com.alibaba.polardbx.executor.balancer.action.ActionMoveGroup;
import com.alibaba.polardbx.executor.balancer.action.ActionMoveGroups;
import com.alibaba.polardbx.executor.balancer.action.ActionSplitPartition;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.policy.PolicyDataBalance;
import com.alibaba.polardbx.executor.balancer.policy.PolicyDrainNode;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.gms.rebalance.RebalanceTarget;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author moyi
 * @since 2021/05
 */
public class TestBalancer {

    private List<GroupDetailInfoExRecord> groupLists(String dn, int startId, int endId) {
        return IntStream.range(startId, endId)
            .mapToObj(id -> new GroupDetailInfoExRecord("g" + id, dn))
            .collect(Collectors.toList());
    }

    private Map<String, Pair<Long, Long>> buildDataSizeMap(String dn, int startId, int endId, long dataSize) {
        return IntStream.range(startId, endId)
            .mapToObj(id -> Pair.of("g" + id, dataSize))
            .collect(Collectors.toMap(e -> e.getKey(), e -> Pair.of(0L, e.getValue())));
    }

    private Map<String, List<GroupStats.GroupsOfStorage>> makeGroups0() {
        List<GroupStats.GroupsOfStorage> storageList = Arrays.asList(
            new GroupStats.GroupsOfStorage("dn1", Lists.newArrayList()),
            new GroupStats.GroupsOfStorage("dn2", Lists.newArrayList())
        );

        Map<String, List<GroupStats.GroupsOfStorage>> groups = new HashMap<>();
        groups.put("db1", storageList);
        return groups;
    }

    private Map<String, List<GroupStats.GroupsOfStorage>> makeGroups1() {
        List<GroupStats.GroupsOfStorage> storageList = Arrays.asList(
            new GroupStats.GroupsOfStorage(
                "dn1",
                groupLists("dn1", 0, 4),
                buildDataSizeMap("dn1", 0, 4, 100)
            ),
            new GroupStats.GroupsOfStorage(
                "dn2",
                groupLists("dn2", 4, 8),
                buildDataSizeMap("dn2", 4, 8, 100)
            ),
            new GroupStats.GroupsOfStorage(
                "dn3",
                Lists.newArrayList()
            )
        );

        Map<String, List<GroupStats.GroupsOfStorage>> groups = new HashMap<>();
        groups.put("db1", storageList);
        return groups;
    }

    private Map<String, List<GroupStats.GroupsOfStorage>> makeGroups2() {
        List<GroupStats.GroupsOfStorage> storageList = Arrays.asList(
            new GroupStats.GroupsOfStorage("dn1", groupLists("dn1", 0, 4)),
            new GroupStats.GroupsOfStorage("dn2", groupLists("dn2", 4, 8)));

        Map<String, List<GroupStats.GroupsOfStorage>> groups = new HashMap<>();
        groups.put("db1", storageList);
        return groups;
    }

    private Map<String, List<GroupStats.GroupsOfStorage>> makeGroups3() {
        List<GroupStats.GroupsOfStorage> storageList = Arrays.asList(
            new GroupStats.GroupsOfStorage("dn1", groupLists("dn1", 0, 4)),
            new GroupStats.GroupsOfStorage("dn2", groupLists("dn2", 4, 8)),
            new GroupStats.GroupsOfStorage("dn3", Lists.newArrayList()),
            new GroupStats.GroupsOfStorage("dn4", Lists.newArrayList())
        );

        Map<String, List<GroupStats.GroupsOfStorage>> groups = new HashMap<>();
        groups.put("db1", storageList);
        return groups;
    }

    private Map<String, List<GroupStats.GroupsOfStorage>> makeGroups4() {
        List<GroupStats.GroupsOfStorage> storageList = Arrays.asList(
            new GroupStats.GroupsOfStorage("dn1", groupLists("dn1", 0, 1)),
            new GroupStats.GroupsOfStorage("dn2", groupLists("dn2", 4, 9)));

        Map<String, List<GroupStats.GroupsOfStorage>> groups = new HashMap<>();
        groups.put("db1", storageList);
        return groups;
    }

    class MockedPolicyDataBalance extends PolicyDataBalance {

        @Override
        protected boolean isStorageReady(String storageInst) {
            return true;
        }
    }

    private List<BalanceAction> drainNode(Map<String, List<GroupStats.GroupsOfStorage>> groups,
                                          BalanceOptions options) {
        PolicyDrainNode policy = new MockedPolicyDrainNode();
        List<BalanceAction> result = new ArrayList<>();
        for (val kv : groups.entrySet()) {
            String schema = kv.getKey();
            List<GroupStats.GroupsOfStorage> groupList = kv.getValue();
            BalanceStats stats = BalanceStats.createForSharding(schema, groupList);
            result.addAll(policy.applyToShardingDb(null, options, stats, schema));
        }

        return result;
    }

    protected List<BalanceAction> balanceGroups(Map<String, List<GroupStats.GroupsOfStorage>> groups,
                                                BalanceOptions options) {
        MockedPolicyDataBalance policy = new MockedPolicyDataBalance();
        List<BalanceAction> result = new ArrayList<>();
        for (val kv : groups.entrySet()) {
            String schema = kv.getKey();
            List<GroupStats.GroupsOfStorage> groupList = kv.getValue();
            BalanceStats stats = BalanceStats.createForSharding(schema, groupList);
            result.addAll(policy.applyToShardingDb(null, options, stats, schema));
        }
        return result;
    }

    class MockedPolicyDrainNode extends PolicyDrainNode {

        @Override
        protected void doValidate(DrainNodeInfo info) {
        }
    }

    @Test
    public void testGroupBalance() {
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        // all empty
        {
            Map<String, List<GroupStats.GroupsOfStorage>> groups = makeGroups0();
            List<BalanceAction> actions = balanceGroups(groups, BalanceOptions.withDefault());
            List<BalanceAction> expectedActions = Collections.emptyList();
            Assert.assertEquals(expectedActions, actions);
        }

        // uniformed storage nodes
        {
            Map<String, List<GroupStats.GroupsOfStorage>> groups = makeGroups2();
            List<BalanceAction> actions = balanceGroups(groups, BalanceOptions.withDefault());
            List<BalanceAction> expectedActions = Collections.emptyList();
            Assert.assertEquals(expectedActions, actions);
        }

        // non-uniformed storage nodes
        {
            Map<String, List<GroupStats.GroupsOfStorage>> groups = makeGroups4();
            List<BalanceAction> actions = balanceGroups(groups, BalanceOptions.withDefault());
            List<BalanceAction> expectedActions =
                Arrays.asList(
                    new ActionLockResource(
                        "db1",
                        Sets.newHashSet(ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, "db1"), "db1")
                    ),
                    new ActionMoveGroups("db1",
                        Arrays.asList(
                            new ActionMoveGroup("db1", Arrays.asList("g4"), "dn1"),
                            new ActionMoveGroup("db1", Arrays.asList("g5"), "dn1")
                        ))
                );
            List<BalanceAction> actMoveAction = new ArrayList<>();
            for (BalanceAction action : actions) {
                if (action instanceof ActionMoveGroups) {
                    ActionMoveGroups mg = (ActionMoveGroups) action;
                    Collections.sort(mg.getActions());
                    actMoveAction.add(action);
                } else if (action instanceof ActionLockResource) {
                    actMoveAction.add(action);
                }
            }
            Assert.assertEquals(expectedActions, actMoveAction);
        }

        // one empty storage node
        {
            Map<String, List<GroupStats.GroupsOfStorage>> groups = makeGroups1();
            List<BalanceAction> actions = balanceGroups(groups, BalanceOptions.withDefault());
            List<BalanceAction> expectedActions =
                Arrays.asList(
                    new ActionLockResource(
                        "db1",
                        Sets.newHashSet(ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, "db1"), "db1")
                    ),
                    new ActionMoveGroups("db1",
                        Arrays.asList(
                            new ActionMoveGroup("db1", Arrays.asList("g0"), "dn3"),
                            new ActionMoveGroup("db1", Arrays.asList("g4"), "dn3"),
                            new ActionMoveGroup("db1", Arrays.asList("g5"), "dn3")
                        ))
                );
            List<BalanceAction> actMoveAction = new ArrayList<>();

            for (BalanceAction action : actions) {
                if (action instanceof ActionMoveGroups) {
                    ActionMoveGroups mg = (ActionMoveGroups) action;
                    Collections.sort(mg.getActions());
                    actMoveAction.add(action);
                } else if (action instanceof ActionLockResource) {
                    actMoveAction.add(action);
                }
            }
            Assert.assertEquals(expectedActions, actMoveAction);
        }

        // two empty storage nodes
        {
            Map<String, List<GroupStats.GroupsOfStorage>> groups = makeGroups3();
            List<BalanceAction> actions = balanceGroups(groups, BalanceOptions.withDefault());
            List<BalanceAction> expectedActions =
                Arrays.asList(
                    new ActionLockResource(
                        "db1",
                        Sets.newHashSet(ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, "db1"), "db1")
                    ),
                    new ActionMoveGroups(
                        "db1",
                        Arrays.asList(
                            new ActionMoveGroup("db1", Arrays.asList("g4"), "dn3"),
                            new ActionMoveGroup("db1", Arrays.asList("g5"), "dn3"),
                            new ActionMoveGroup("db1", Arrays.asList("g0"), "dn4"),
                            new ActionMoveGroup("db1", Arrays.asList("g1"), "dn4")
                        )
                    )
                );
            List<BalanceAction> actMoveAction = new ArrayList<>();
            for (BalanceAction action : actions) {
                if (action instanceof ActionMoveGroups) {
                    ActionMoveGroups mg = (ActionMoveGroups) action;
                    Collections.sort(mg.getActions());
                    actMoveAction.add(action);
                } else if (action instanceof ActionLockResource) {
                    actMoveAction.add(action);
                }
            }
            Assert.assertEquals(expectedActions, actMoveAction);
        }
    }

    @Test
    public void testDrainNode() {
        // all empty
        {
            Map<String, List<GroupStats.GroupsOfStorage>> groups = makeGroups0();
            BalanceOptions options = BalanceOptions.withDefault().withDrainNode("dn1");
            List<BalanceAction> actions = drainNode(groups, options);
            List<BalanceAction> expectedActions = Collections.emptyList();
            Assert.assertEquals(expectedActions, actions);
        }

        // existed storage node
        {
            Map<String, List<GroupStats.GroupsOfStorage>> groups = makeGroups1();
            BalanceOptions options = BalanceOptions.withDefault().withDrainNode("dn1");
            List<BalanceAction> actions = drainNode(groups, options);
            List<BalanceAction> expectedActions =
                Arrays.asList(
                    new ActionLockResource(
                        "db1",
                        Sets.newHashSet(ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, "db1"), "db1")
                    ),
                    new ActionMoveGroups("db1",
                        Arrays.asList(
                            new ActionMoveGroup("db1", Arrays.asList("g0"), "dn2"),
                            new ActionMoveGroup("db1", Arrays.asList("g1"), "dn3"),
                            new ActionMoveGroup("db1", Arrays.asList("g2"), "dn2"),
                            new ActionMoveGroup("db1", Arrays.asList("g3"), "dn3")
                        ))
                );
            List<BalanceAction> actMoveAction = new ArrayList<>();
            for (BalanceAction action : actions) {
                if (action instanceof ActionMoveGroups || action instanceof ActionLockResource) {
                    actMoveAction.add(action);
                }
            }
            Assert.assertEquals(expectedActions, actMoveAction);
        }

        // non-existed storage node
        {
            Map<String, List<GroupStats.GroupsOfStorage>> groups = makeGroups1();
            BalanceOptions options = BalanceOptions.withDefault().withDrainNode("not-existed");
            List<BalanceAction> actions = drainNode(groups, options);
            List<BalanceAction> expectedActions = Collections.emptyList();
            Assert.assertEquals(expectedActions, actions);
        }
    }

    @Data
    @AllArgsConstructor
    class DiskInfoTestCase {
        String diskInfoStr;
        List<PolicyDrainNode.DnDiskInfo> expected;

        String exceptionMessage;

        DiskInfoTestCase(String diskInfoStr, List<PolicyDrainNode.DnDiskInfo> expected) {
            this.diskInfoStr = diskInfoStr;
            this.expected = expected;
        }
    }

    @Test
    public void testDiskInfoParse() {
        List<DiskInfoTestCase> testCases = Arrays.asList(
            /* empty */
            new DiskInfoTestCase(
                "[]",
                Collections.emptyList()),

            /* single */
            new DiskInfoTestCase(
                "[{\"node\":\"dn1\", \"free_space_byte\":100}]",
                Arrays.asList(new PolicyDrainNode.DnDiskInfo("dn1", 100))),

            /* multiple */
            new DiskInfoTestCase(
                "[{node:\"dn1\", free_space_byte:100}, {node:\"dn2\", free_space_byte:100}]",
                Arrays.asList(
                    new PolicyDrainNode.DnDiskInfo("dn1", 100),
                    new PolicyDrainNode.DnDiskInfo("dn2", 100)
                )),

            /* illegal */
            new DiskInfoTestCase(
                "[{\"node\":\"dn1]",
                Arrays.asList(new PolicyDrainNode.DnDiskInfo("dn1", 100)),
                "invalid disk_info: unclosed str"),

            new DiskInfoTestCase(
                "[{node:\"dn1\", free_space_byte: \"1TB\"}]",
                Arrays.asList(new PolicyDrainNode.DnDiskInfo("dn1", 100)),
                "invalid disk_info: parseLong error, field : free_space_byte")
        );

        for (DiskInfoTestCase testCase : testCases) {
            try {
                List<PolicyDrainNode.DnDiskInfo> diskInfo =
                    PolicyDrainNode.DnDiskInfo.parseToList(testCase.diskInfoStr);

                if (testCase.exceptionMessage == null) {
                    Assert.assertEquals(testCase.getExpected(), diskInfo);
                }
            } catch (Exception e) {
                Assert.assertEquals(testCase.exceptionMessage, e.getMessage());
            }
        }
    }

    @Data
    @AllArgsConstructor
    static class DrainNodeInfoTestCase {
        String input;
        PolicyDrainNode.DrainNodeInfo expected;
    }

    @Test
    public void testParseDrainNodeInfo() {
        List<DrainNodeInfoTestCase> testCases = Arrays.asList(
            // comma separated
            new DrainNodeInfoTestCase("dn1,dn2",
                new PolicyDrainNode.DrainNodeInfo(Arrays.asList("dn1", "dn2"))),
            new DrainNodeInfoTestCase("dn1",
                new PolicyDrainNode.DrainNodeInfo(Arrays.asList("dn1"))),

            // json format
            new DrainNodeInfoTestCase("{dn:[\"dn1\",\"dn2\"], cn_rw: ['cn1','cn2'], cn_ro: ['cn3','cn4']}",
                new PolicyDrainNode.DrainNodeInfo(Arrays.asList("dn1", "dn2"),
                    Arrays.asList("cn1", "cn2"),
                    Arrays.asList("cn3", "cn4"))),

            new DrainNodeInfoTestCase("{dn:[\"dn1\",\"dn2\"], cn_rw: [\"cn1\"] }",
                new PolicyDrainNode.DrainNodeInfo(Arrays.asList("dn1", "dn2"),
                    Arrays.asList("cn1"),
                    Arrays.asList()))

        );

        for (DrainNodeInfoTestCase testCase : testCases) {
            PolicyDrainNode.DrainNodeInfo result = PolicyDrainNode.DrainNodeInfo.parse(testCase.input);
            Assert.assertEquals(testCase.expected, result);
        }

    }

    @Data
    @AllArgsConstructor
    class DrainNodeDiskInfoTestCase {
        int freeByte;
        String exceptionMessage;
    }

    @Test
    public void testPolicyDrainNode_DiskInfo() {
        // Each group is 100 bytes
        // dn1 and dn2 both have 4 groups

        List<DrainNodeDiskInfoTestCase> testCaseList = Arrays.asList(
            new DrainNodeDiskInfoTestCase(10, "not enough free space"),
            new DrainNodeDiskInfoTestCase(100, "not enough free space"),
            new DrainNodeDiskInfoTestCase(200, "not enough free space"),
            new DrainNodeDiskInfoTestCase(399, "not enough free space"),
            new DrainNodeDiskInfoTestCase(400, ""),
            new DrainNodeDiskInfoTestCase(1000, "")
        );

        // dn not enough
        for (DrainNodeDiskInfoTestCase testCase : testCaseList) {
            String diskInfo = String.format("[{node: 'dn2', free_space_byte: %d}]", testCase.freeByte);
            Map<String, List<GroupStats.GroupsOfStorage>> groups = makeGroups1();
            BalanceOptions options = BalanceOptions.withDefault().withDrainNode("dn1").withDiskInfo(diskInfo);

            System.err.println("drain node with disk_info: " + diskInfo);
            try {
                drainNode(groups, options);
                if (TStringUtil.isNotBlank(testCase.exceptionMessage)) {
                    Assert.fail("should fail");
                }
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains(testCase.exceptionMessage));
            }
        }

    }

    @Test
    public void testSerializeSplitPartition() {
        ActionSplitPartition split =
            new ActionSplitPartition("schema", "tg", "pg",
                "alter tablegroup tg split partition pg into ()");

        String json = JSON.toJSONString(split);
        System.err.println(json);

        ActionSplitPartition result = JSON.parseObject(json, ActionSplitPartition.class);
        Assert.assertEquals(split, result);
    }

    @Test
    public void testSerializeMergePartition() {
        ActionMergePartition merge =
            new ActionMergePartition("schema", "tg", Arrays.asList("p1", "p2"), "p3");

        String json = JSON.toJSONString(merge);
        System.err.println(json);

        ActionMergePartition result = JSON.parseObject(json, ActionMergePartition.class);
        Assert.assertEquals(merge, result);
    }

    @Test
    public void testActionLockResourceEquals() {
        ActionLockResource ar1 = null;
        ActionLockResource ar2 = null;

        ar1 = new ActionLockResource("schema1", Sets.newHashSet("a"));
        ar2 = new ActionLockResource("schema1", Sets.newHashSet("a"));
        Assert.assertEquals(ar1, ar2);
        Assert.assertEquals(ar1.hashCode(), ar2.hashCode());

        ar1 = new ActionLockResource("schema1", Sets.newHashSet("a", "b"));
        ar2 = new ActionLockResource("schema1", Sets.newHashSet("a", "b"));
        Assert.assertEquals(ar1, ar2);
        Assert.assertEquals(ar1.hashCode(), ar2.hashCode());

        ar1 = new ActionLockResource("schema1", Sets.newHashSet("a"));
        ar2 = new ActionLockResource("schema1", Sets.newHashSet("b"));
        Assert.assertNotEquals(ar1, ar2);
        Assert.assertNotEquals(ar1.hashCode(), ar2.hashCode());
    }
}
