package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.net.packet.RowDataPacket;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.polardbx.server.response.SelectPolardbVersion.getFullProductionVersion;

public class SelectPolardbVersionTest {

    private static final String CHARSET = "utf8";

    private static RowDataPacket generatePacket() {
        RowDataPacket row = new RowDataPacket(3);
        return row;
    }

    @Test
    public void testParseVersion() {
        RowDataPacket packet = generatePacket();
        String type = "CN";
        final String cnVersion = "5.4.19";
        final String cnReleaseDate = "20240314";
        SelectPolardbVersion.addVersionWithReleaseDate(packet, type,
            cnVersion + "-" + cnReleaseDate, CHARSET);

        String typeInPacket = new String(packet.fieldValues.get(0));
        String versionInPacket = new String(packet.fieldValues.get(1));
        String releaseDateInPacket = new String(packet.fieldValues.get(2));
        Assert.assertEquals(type, typeInPacket);
        Assert.assertEquals(getFullProductionVersion(cnVersion), versionInPacket);
        Assert.assertEquals(cnReleaseDate, releaseDateInPacket);
    }

    /**
     * expect NULL for null version
     */
    @Test
    public void testParseNullVersion() {
        RowDataPacket packet = generatePacket();
        String type = "Columnar";
        SelectPolardbVersion.addVersionWithReleaseDate(packet, type, null, CHARSET);

        String typeInPacket = new String(packet.fieldValues.get(0));
        Assert.assertEquals(type, typeInPacket);
        Assert.assertNull(packet.fieldValues.get(1));
        Assert.assertNull(packet.fieldValues.get(2));
    }

    /**
     * test illegal version format
     * eg. without releaseDate
     */
    @Test
    public void testParseIllegalVersion() {
        RowDataPacket packet = generatePacket();
        String type = "Columnar";
        final String version = "5.4.19";
        SelectPolardbVersion.addVersionWithReleaseDate(packet, type, version, CHARSET);

        String typeInPacket = new String(packet.fieldValues.get(0));
        Assert.assertEquals(type, typeInPacket);
        String versionInPacket = new String(packet.fieldValues.get(1));
        Assert.assertEquals(getFullProductionVersion(version), versionInPacket);
        Assert.assertNull(packet.fieldValues.get(2));
    }
}
