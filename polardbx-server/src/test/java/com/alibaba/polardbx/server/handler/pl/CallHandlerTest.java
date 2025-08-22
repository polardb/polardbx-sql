package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.pl.PlContext;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.net.ClusterAcceptIdGenerator;
import com.alibaba.polardbx.optimizer.parse.util.SpParameter;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.SyncPointExecutor;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class CallHandlerTest extends TestCase {

    @Test
    public void testProcedureRT() throws Exception {
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try(MockedStatic<CobarServer> mockedCobarServer = mockStatic(CobarServer.class);
            /*-------------*/
            MockedStatic<ClusterAcceptIdGenerator> mockedClusterAcceptIdGenerator = mockStatic(ClusterAcceptIdGenerator.class)){

            CobarServer cobarServer = mock(CobarServer.class);
            CobarConfig cobarConfig = mock(CobarConfig.class);
            ClusterAcceptIdGenerator clusterAcceptIdGenerator = mock(ClusterAcceptIdGenerator.class);

            mockedCobarServer.when(CobarServer::getInstance).thenReturn(cobarServer);
            mockedClusterAcceptIdGenerator.when(ClusterAcceptIdGenerator::getInstance).thenReturn(clusterAcceptIdGenerator);
            when(cobarServer.getConfig()).thenReturn(cobarConfig);
            when(cobarConfig.getSystem()).thenReturn(new SystemConfig());
            when(clusterAcceptIdGenerator.nextId()).thenReturn(10L);

            RuntimeProcedure runtimeProcedure = mock(RuntimeProcedure.class);
            SQLCallStatement callStatement = mock(SQLCallStatement.class);
            SocketChannel socketChannel = mock(SocketChannel.class);
            Socket socket = mock(Socket.class);
            InetAddress inetAddress = mock(InetAddress.class);


            when(socketChannel.socket()).thenReturn(socket);
            when(socket.getInetAddress()).thenReturn(inetAddress);
            when(socket.getPort()).thenReturn(3306);
            when(socket.getLocalPort()).thenReturn(3306);
            when(inetAddress.getHostAddress()).thenReturn("127.0.0.1");
            when(inetAddress.isLoopbackAddress()).thenReturn(false);

            ServerConnection serverConnection = new ServerConnection(socketChannel);

            when(runtimeProcedure.getPlContext()).thenReturn(new PlContext(1024));
            when(runtimeProcedure.getMemoryPool()).thenReturn(null);
            when(callStatement.getParameters()).thenReturn(new ArrayList<>());

            doAnswer(invocation -> {
                Thread.sleep(500);
                serverConnection.setLastActiveTime(System.nanoTime());
                System.out.println("running procedure : " + serverConnection.getLastActiveTime());
                return null;
            }).when(runtimeProcedure).run();

            /*-------------*/

            long lastActiveTime = System.nanoTime();
            serverConnection.setLastActiveTime(lastActiveTime);
            System.out.println("before run procedure : " + serverConnection.getLastActiveTime());
            CallHandler.handleProcedure(runtimeProcedure, callStatement, new HashMap<>(), serverConnection, null);
            System.out.println("after run procedure : " + serverConnection.getLastActiveTime());
            Assert.assertEquals(serverConnection.getLastActiveTime(), lastActiveTime);
        }
    }

}