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

package com.alibaba.polardbx.qatest.dal.show;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author ximing.yd
 * @date 2022/3/10 2:14 下午
 */
public class ShowHeatmapTest extends ReadBaseTestCase {

    @Test
    public void testShowHeatmapRW() throws SQLException {
        String sql = "SHOW PARTITIONS HEATMAP LAST_SEVEN_DAYS READ_WRITTEN_ROWS;";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("HEATMAP", rs.getMetaData().getColumnName(1).toUpperCase());
    }

    @Test
    public void testShowHeatmapRwDn() throws SQLException {
        String sql = "SHOW PARTITIONS HEATMAP LAST_SEVEN_DAYS READ_WRITTEN_ROWS_WITH_DN;";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("HEATMAP", rs.getMetaData().getColumnName(1).toUpperCase());
    }

    @Test
    public void testShowHeatmapR() throws SQLException {
        String sql = "SHOW PARTITIONS HEATMAP LAST_SEVEN_DAYS READ_ROWS;";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("HEATMAP", rs.getMetaData().getColumnName(1).toUpperCase());
    }

    @Test
    public void testShowHeatmapW() throws SQLException {
        String sql = "SHOW PARTITIONS HEATMAP LAST_SEVEN_DAYS WRITTEN_ROWS;";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("HEATMAP", rs.getMetaData().getColumnName(1).toUpperCase());
    }

    @Test
    public void testShowHeatmapOneHours() throws SQLException {
        String sql = "SHOW PARTITIONS HEATMAP LAST_ONE_HOURS READ_WRITTEN_ROWS;";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("HEATMAP", rs.getMetaData().getColumnName(1).toUpperCase());
    }

    @Test
    public void testShowHeatmapSixHours() throws SQLException {
        String sql = "SHOW PARTITIONS HEATMAP LAST_SIX_HOURS READ_WRITTEN_ROWS;";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("HEATMAP", rs.getMetaData().getColumnName(1).toUpperCase());
    }

    @Test
    public void testShowHeatmapOneDays() throws SQLException {
        String sql = "SHOW PARTITIONS HEATMAP LAST_ONE_DAYS READ_WRITTEN_ROWS;";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("HEATMAP", rs.getMetaData().getColumnName(1).toUpperCase());
    }

    @Test
    public void testShowHeatmapThreeDays() throws SQLException {
        String sql = "SHOW PARTITIONS HEATMAP LAST_THREE_DAYS READ_WRITTEN_ROWS;";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("HEATMAP", rs.getMetaData().getColumnName(1).toUpperCase());
    }
}
