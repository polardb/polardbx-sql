package com.alibaba.polardbx.optimizer.core.rel.util;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TargetTableInfoTest {

    private TargetTableInfo targetTableInfo;

    @Before
    public void setUp() {
        targetTableInfo = new TargetTableInfo();
    }

    @Test
    public void testGetTargetTableInfoList() {
        List<TargetTableInfoOneTable> expectedList = new ArrayList<>();
        expectedList.add(new TargetTableInfoOneTable());

        targetTableInfo.setTargetTableInfoList(expectedList);
        List<TargetTableInfoOneTable> actualList = targetTableInfo.getTargetTableInfoList();

        assertNotNull(actualList);
        assertEquals(expectedList, actualList);
    }

    @Test
    public void testSetTargetTableInfoList() {
        List<TargetTableInfoOneTable> expectedList = new ArrayList<>();
        expectedList.add(new TargetTableInfoOneTable());

        targetTableInfo.setTargetTableInfoList(expectedList);

        List<TargetTableInfoOneTable> actualList = targetTableInfo.getTargetTableInfoList();
        assertNotNull(actualList);
        assertEquals(expectedList, actualList);
    }

    @Test
    public void testGetTargetTables() {
        Map<String, List<List<String>>> expectedMap = new HashMap<>();
        expectedMap.put("table1", new ArrayList<>());

        targetTableInfo.setTargetTables(expectedMap);
        Map<String, List<List<String>>> actualMap = targetTableInfo.getTargetTables();

        assertNotNull(actualMap);
        assertEquals(expectedMap, actualMap);
    }

    @Test
    public void testSetTargetTables() {
        Map<String, List<List<String>>> expectedMap = new HashMap<>();
        expectedMap.put("table1", new ArrayList<>());

        targetTableInfo.setTargetTables(expectedMap);

        Map<String, List<List<String>>> actualMap = targetTableInfo.getTargetTables();
        assertNotNull(actualMap);
        assertEquals(expectedMap, actualMap);
    }
}
