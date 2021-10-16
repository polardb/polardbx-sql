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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author sanguan.tangsicheng on 16/5/13 上午9:11
 */
public class ShowGitCommit {

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    private static final Properties GIT_PROPERTIES = initGitProperties();

    private static Properties initGitProperties() {
        try {

            Properties pro = new Properties();
            InputStream in =
                ShowGitCommit.class.getClassLoader().getResourceAsStream("META-INF/polardbx/git.properties");
            pro.load(in);
            in.close();
            return pro;
        } catch (Exception ignore) {
            return new Properties();
        }

    }

    private static final String GIT_BRANCH = GIT_PROPERTIES.getProperty("git.branch", "master");
    private static final String GIT_COMMIT_ID = GIT_PROPERTIES.getProperty("git.commit.id", "id");
    private static final String GIT_TAGS = GIT_PROPERTIES.getProperty("git.tags", "release");
    private static final String BUILD_VERSION = GIT_PROPERTIES.getProperty("git.build.version", "1.0.0-realse");

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("BRANCH", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("COMMIT_ID", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TAGS", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BUILD_VERSION", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;

        eof.packetId = ++packetId;

    }

    private static RowDataPacket getGitCommitData(String charset) {
        RowDataPacket gitInfoData = new RowDataPacket(FIELD_COUNT);
        gitInfoData.add(StringUtil.encode(GIT_BRANCH, charset));
        gitInfoData.add(StringUtil.encode(GIT_COMMIT_ID, charset));
        gitInfoData.add(StringUtil.encode(GIT_TAGS, charset));
        gitInfoData.add(StringUtil.encode(BUILD_VERSION, charset));
        return gitInfoData;
    }

    public static void execute(ServerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = eof.write(proxy);

        byte packetId = eof.packetId;

        RowDataPacket gitCommitInfo = getGitCommitData(c.getCharset());

        gitCommitInfo.packetId = ++packetId;
        proxy = gitCommitInfo.write(proxy);

        /*proxy = GIT_COMMIT_INFO.write(proxy);
        GIT_COMMIT_INFO.packetId = ++packetId;*/

        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // post write
        proxy.packetEnd();
    }
}
