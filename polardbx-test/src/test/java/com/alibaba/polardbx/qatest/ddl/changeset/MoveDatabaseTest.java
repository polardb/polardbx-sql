package com.alibaba.polardbx.qatest.ddl.changeset;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ddl.sharding.movedatabase.MoveDatabaseBaseTest;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class MoveDatabaseTest extends MoveDatabaseBaseTest {
    private static final String dataBaseName = "tpcc_drds";

    public String hint = "";

    public MoveDatabaseTest() {
        super(dataBaseName);
    }

    @Before
    public void before() {
        initDatasourceInfomation(dataBaseName);
    }

    @Test
    public void moveDatabaseWithTPCCTest() {
        if (usingNewPartDb()) {
            return;
        }

        int times = 0;
        try {
            for (String groupName : groupNames) {
                doScaleOutTaskAndCheckForOneGroup(groupName);
                times++;
                if (times == 6) {
                    break;
                }
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    private void doScaleOutTaskAndCheckForOneGroup(String groupName) {
        if (groupNames.isEmpty()) {
            throw new RuntimeException(
                String.format("no find a new storage for group[%s] to move", groupName));
        }

        // Prepare scale out task sql
        if (groupName == null) {
            groupName = groupNames.stream().findFirst().get();
        }

        String storageIdOfGroup = groupToStorageIdMap.get(groupName);

        boolean findTargetSid = false;
        String targetStorageId = "";
        for (String sid : storageIDs) {
            if (storageIdOfGroup.equalsIgnoreCase(sid)) {
                continue;
            }
            findTargetSid = true;
            targetStorageId = sid;
            break;
        }
        if (!findTargetSid) {
            throw new RuntimeException(
                String.format("no find a new storage for group[%s] to move", groupName));
        }

        String scaleOutTaskSql =
            String.format("move database %s %s to '%s';", hint, groupName, targetStorageId);

        try {
            Connection conn = tddlConnection;
            String useDbSql = String.format("use %s;", dataBaseName);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(useDbSql);
            } catch (Throwable ex) {
                ex.printStackTrace();
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(scaleOutTaskSql);
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
        }

        System.out.print(scaleOutTaskSql);
    }
}
