package com.alibaba.polardbx.gms.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class MetaDbUtilTest {

    private MockedStatic<MetaDbUtil> mockMetaDbUtil;

    @Before
    public void setUp() throws Exception {
        mockMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
        mockMetaDbUtil.when(MetaDbUtil::getGmsPolardbVersion).thenCallRealMethod();
    }

    @After
    public void cleanUp() {
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }
    }

    @Test
    public void testGmsPolardbVersion() throws SQLException {

        final String mockReleaseDate = "20240412";
        final String mockEngineVersion = "5.4.19";

        Connection mockConnection = mock(Connection.class);
        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenAnswer(i -> mockConnection);
        Statement statement = mock(Statement.class);
        Mockito.when(mockConnection.createStatement()).thenReturn(statement);
        ResultSet resultSet = mock(ResultSet.class);
        Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true);
        Mockito.when(resultSet.getString(1)).thenReturn(mockEngineVersion);
        Mockito.when(resultSet.getString(2)).thenReturn(mockReleaseDate);

        final String gmsPolardbVersion = MetaDbUtil.getGmsPolardbVersion();

        Assert.assertEquals(String.format("%s-%s", mockEngineVersion, mockReleaseDate), gmsPolardbVersion);
    }

}
