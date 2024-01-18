package com.alibaba.polardbx.common;

import com.alibaba.polardbx.common.utils.AddressUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AddressUtilsTest {
    static final String MOCK_IP = "30.221.112.137";
    static final String SERIALIZED_INET_ADDRESS_PATH = "serialize/inet_address.txt";

    @Test
    public void testMatchPodIpFromLocalHost() {
        Assert.assertNull(AddressUtils.tryMatchFromLocalHost(MOCK_IP));
    }

    @Test
    public void testMatchPodIpFromAllNet()
        throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
        InstantiationException, IllegalAccessException {

        Assert.assertNull(AddressUtils.tryMatchFromAllNet(MOCK_IP, AddressUtils.getNetInterface()));

        // test mock interface
        InetAddress inetAddress = AddressUtils.tryMatchFromAllNet(
            MOCK_IP, Collections.enumeration(generateMockInterfaces()));
        Assert.assertEquals(inetAddress.getHostAddress(), MOCK_IP);
    }

    @Test
    public void checkNotMatchedAddress() {
        Assert.assertNull(AddressUtils.getMatchedAddress("127.0.0.1"));
        Assert.assertNull(AddressUtils.getMatchedAddress("0.0.0.0"));
        Assert.assertNull(AddressUtils.getMatchedAddress("123.xx.23.1"));
    }

    private List<NetworkInterface> generateMockInterfaces()
        throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException,
        IllegalAccessException, ClassNotFoundException {
        List<NetworkInterface> interfaceList =
            new ArrayList<>(Collections.list(NetworkInterface.getNetworkInterfaces()));
        NetworkInterface mockNetInterface = mockNetInterface();
        interfaceList.add(mockNetInterface);
        return interfaceList;
    }

    private NetworkInterface mockNetInterface()
        throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException,
        IOException, ClassNotFoundException {
        Class<?> clazz = NetworkInterface.class;
        Constructor<?> constructor = clazz.getDeclaredConstructor(String.class, int.class, InetAddress[].class);
        constructor.setAccessible(true);
        NetworkInterface mockInterface = (NetworkInterface) constructor.newInstance("mock_ens", 1001,
            new InetAddress[] {getMockInetAddress()});
        return mockInterface;
    }

    private InetAddress getMockInetAddress() throws IOException, ClassNotFoundException {
        String path = this.getClass().getClassLoader().getResource(SERIALIZED_INET_ADDRESS_PATH).getPath();
        try (FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream objIn = new ObjectInputStream(fileIn)) {

            InetAddress inetAddress = (InetAddress) objIn.readObject();
            return inetAddress;
        }
    }
}
