package com.alibaba.polardbx.optimizer.core.rel.util;

import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TargetTableInfoOneTableTest {

    private TargetTableInfoOneTable targetTableInfoOneTable;

    @Before
    public void setUp() {
        targetTableInfoOneTable = new TargetTableInfoOneTable();
    }

    @Test
    public void testGetPrunedFirstLevelPartCount() {
        // Given
        int expectedCount = 3;

        // When
        targetTableInfoOneTable.setPrunedFirstLevelPartCount(expectedCount);

        // Then
        assertEquals(expectedCount, targetTableInfoOneTable.getPrunedFirstLevelPartCount());
    }

    @Test
    public void testSetPrunedFirstLevelPartCount() {
        // Given
        int expectedCount = 5;

        // When
        targetTableInfoOneTable.setPrunedFirstLevelPartCount(expectedCount);

        // Then
        assertEquals(expectedCount, targetTableInfoOneTable.getPrunedFirstLevelPartCount());
    }

    @Test
    public void testIsAllPartSorted() {
        // Given

        // When
        targetTableInfoOneTable.setAllPartSorted(true);

        // Then
        assertTrue(targetTableInfoOneTable.isAllPartSorted());
    }

    @Test
    public void testSetAllPartSorted() {
        // Given

        // When
        targetTableInfoOneTable.setAllPartSorted(false);

        // Then
        assertFalse(targetTableInfoOneTable.isAllPartSorted());
    }

    @Test
    public void testGetPartColList() {
        // Given
        List<String> expectedPartColList = new ArrayList<>();
        expectedPartColList.add("col1");
        expectedPartColList.add("col2");

        // When
        targetTableInfoOneTable.setPartColList(expectedPartColList);

        // Then
        assertEquals(expectedPartColList, targetTableInfoOneTable.getPartColList());
    }

    @Test
    public void testSetPartColList() {
        // Given
        List<String> expectedPartColList = new ArrayList<>();
        expectedPartColList.add("col1");
        expectedPartColList.add("col2");

        // When
        targetTableInfoOneTable.setPartColList(expectedPartColList);

        // Then
        assertEquals(expectedPartColList, targetTableInfoOneTable.getPartColList());
    }

    @Test
    public void testGetSubpartColList() {
        // Given
        List<String> expectedSubpartColList = new ArrayList<>();
        expectedSubpartColList.add("sub_col1");
        expectedSubpartColList.add("sub_col2");

        // When
        targetTableInfoOneTable.setSubpartColList(expectedSubpartColList);

        // Then
        assertEquals(expectedSubpartColList, targetTableInfoOneTable.getSubpartColList());
    }

    @Test
    public void testSetSubpartColList() {
        // Given
        List<String> expectedSubpartColList = new ArrayList<>();
        expectedSubpartColList.add("sub_col1");
        expectedSubpartColList.add("sub_col2");

        // When
        targetTableInfoOneTable.setSubpartColList(expectedSubpartColList);

        // Then
        assertEquals(expectedSubpartColList, targetTableInfoOneTable.getSubpartColList());
    }

    @Test
    public void testGetPartInfo() {
        // Given
        PartitionInfo expectedPartInfo = new PartitionInfo() {
        };

        // When
        targetTableInfoOneTable.setPartInfo(expectedPartInfo);

        // Then
        assertEquals(expectedPartInfo, targetTableInfoOneTable.getPartInfo());
    }

    @Test
    public void testSetPartInfo() {
        // Given
        PartitionInfo expectedPartInfo = new PartitionInfo() {
        };

        // When
        targetTableInfoOneTable.setPartInfo(expectedPartInfo);

        // Then
        assertEquals(expectedPartInfo, targetTableInfoOneTable.getPartInfo());
    }

    @Test
    public void testIsUseSubPart() {
        // Given

        // When
        targetTableInfoOneTable.setUseSubPart(true);

        // Then
        assertTrue(targetTableInfoOneTable.isUseSubPart());
    }

    @Test
    public void testSetUseSubPart() {
        // Given

        // When
        targetTableInfoOneTable.setUseSubPart(false);

        // Then
        assertFalse(targetTableInfoOneTable.isUseSubPart());
    }

    @Test
    public void testIsAllSubPartSorted() {
        // Given

        // When
        targetTableInfoOneTable.setAllSubPartSorted(true);

        // Then
        assertTrue(targetTableInfoOneTable.isAllSubPartSorted());
    }

    @Test
    public void testSetAllSubPartSorted() {
        // Given

        // When
        targetTableInfoOneTable.setAllSubPartSorted(false);

        // Then
        assertFalse(targetTableInfoOneTable.isAllSubPartSorted());
    }

    @Test
    public void testIsAllPrunedPartContainOnlyOneSubPart() {
        // Given

        // When
        targetTableInfoOneTable.setAllPrunedPartContainOnlyOneSubPart(true);

        // Then
        assertTrue(targetTableInfoOneTable.isAllPrunedPartContainOnlyOneSubPart());
    }

    @Test
    public void testSetAllPrunedPartContainOnlyOneSubPart() {
        // Given

        // When
        targetTableInfoOneTable.setAllPrunedPartContainOnlyOneSubPart(false);

        // Then
        assertFalse(targetTableInfoOneTable.isAllPrunedPartContainOnlyOneSubPart());
    }

    @Test
    public void testDefaultValues() {
        // Then
        assertNull(targetTableInfoOneTable.getPartInfo());
        assertFalse(targetTableInfoOneTable.isAllPartSorted());
        assertFalse(targetTableInfoOneTable.isUseSubPart());
        assertFalse(targetTableInfoOneTable.isAllSubPartSorted());
        assertFalse(targetTableInfoOneTable.isAllPrunedPartContainOnlyOneSubPart());
        assertNotNull(targetTableInfoOneTable.getPartColList());
        assertNotNull(targetTableInfoOneTable.getSubpartColList());
        assertEquals(0, targetTableInfoOneTable.getPrunedFirstLevelPartCount());
    }
}
