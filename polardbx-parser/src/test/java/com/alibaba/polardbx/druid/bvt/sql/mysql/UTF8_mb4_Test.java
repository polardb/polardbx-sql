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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import junit.framework.TestCase;

public class UTF8_mb4_Test extends TestCase {
    public void test_for_ut8() throws Exception {
        StringBuffer suffix = new StringBuffer();

        byte[] sctbyte=new byte[]{(byte)(0xf0),(byte)(0xf0),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0x00),(byte)(0xff)};
//                //String sctstr="tt";
        String prefix = "INSERT INTO wangshu2(id,name) VALUES ";
        String sctstr="";
//                int id=5;
        sctstr=new String(sctbyte,"utf-8");

        //String sct="mingwen";
        //String secname=secret(sct);
        suffix.append("('6','"+sctstr+"')");
        //suffix.append("('"+secname+"')");
        // 构建完整SQL
        String sql = prefix + suffix.substring(0, suffix.length());
        System.out.println("print sql..." + sql);

        SQLUtils.parseSingleMysqlStatement(sql);
    }
}
