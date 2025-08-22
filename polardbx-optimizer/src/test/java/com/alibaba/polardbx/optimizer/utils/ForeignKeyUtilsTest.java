/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ForeignKeyUtilsTest {

    @Test
    public void TestCreateFkTableWithoutName() {
        List<ForeignKeyData> fks = new ArrayList<>();
        ForeignKeyData fk1 =
            new ForeignKeyData("test", "device", "device_ibfk_1", "device_ibfk_1", Collections.singletonList("b"),
                "test", "user2", Collections.singletonList("b"), null, null);
        ForeignKeyData fk2 =
            new ForeignKeyData("test", "device", "device_ibfk_2", "fk", Collections.singletonList("b"),
                "test", "user2",
                Collections.singletonList("b"), null, null);
        ForeignKeyData fk3 =
            new ForeignKeyData("test", "device", "my_ibfk", "my_ibfk", Collections.singletonList("b"),
                "test", "user2",
                Collections.singletonList("c"), null, null);
        ForeignKeyData fk4 =
            new ForeignKeyData("test", "device", "my_ibfk_1", "my_ibfk_1", Collections.singletonList("c"),
                "test", "user2", Collections.singletonList("c"), null, null);
        ForeignKeyData fk5 =
            new ForeignKeyData("test", "device", "device_ibfk_3", "device_ibfk_3", Collections.singletonList("c"),
                "test", "user2",
                Collections.singletonList("c"), null, null);
        ForeignKeyData fk6 =
            new ForeignKeyData("test", "device", "device_ibfk_4", "device_ibfk_4", Collections.singletonList("c"),
                "test", "device",
                Collections.singletonList("c"), null, null);
        ForeignKeyData fk7 =
            new ForeignKeyData("test", "device", "fk_device_user", "fk_device_user", Arrays.asList("b", "c", "d"),
                "test", "user2",
                Arrays.asList("b", "c", "d"), null, null);
        ForeignKeyData fk8 =
            new ForeignKeyData("test", "device", "device_ibfk_5", "device_ibfk_5", Arrays.asList("b", "c", "d"),
                "test", "user2",
                Arrays.asList("b", "c", "d"), null, null);
        fks.add(fk1);
        fks.add(fk2);
        fks.add(fk3);
        fks.add(fk4);
        fks.add(fk5);
        fks.add(fk6);
        fks.add(fk7);
        fks.add(fk8);

        // fix bug: 特殊字符
        fks.add(new ForeignKeyData("test", "device", "fk~special!@#", "fk~special!@#",
            Arrays.asList("e"), "test", "user3",
            Arrays.asList("e"), null, null));
        fks.add(new ForeignKeyData("test", "device", "fk@特殊字符", "fk@特殊字符",
            Arrays.asList("f"), "test", "user4",
            Arrays.asList("f"), null, null));
        fks.add(new ForeignKeyData("test", "device", "fk-with-dash_and_underscore", "fk-with-dash_and_underscore",
            Arrays.asList("g"), "test", "user5",
            Arrays.asList("g"), null, null));
        fks.add(new ForeignKeyData("test", "device", "fk$#%^&*()", "fk$#%^&*()",
            Arrays.asList("h"), "test", "user6",
            Arrays.asList("h"), null, null));

        String createTableSql =
            "CREATE TABLE IF NOT EXISTS device (" +
                "  a INT PRIMARY KEY AUTO_INCREMENT," +
                "  b INT NOT NULL," +
                "  c INT NOT NULL," +
                "  d INT NOT NULL," +
                "  KEY `device_ibfk_1` (`b`)," +
                "  foreign key (`b`) REFERENCES `user2` (`a`),\n" +
                "  foreign key `fk` (`b`) REFERENCES `user2` (`b`),\n" +
                "  constraint `my_ibfk` foreign key (`b`) REFERENCES `user2` (`c`),\n" +
                "  constraint `my_ibfk_1` foreign key `fk1` (`c`) REFERENCES `user2` (`c`),\n" +
                "  foreign key (`c`) REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE,\n" +
                "  foreign key (`d`) REFERENCES `device` (`c`),\n" +
                "  constraint `fk_device_user` foreign key (`b` , `c` , `d`)\n" +
                "       REFERENCES `user2` (`b` , `c` , `d`)\n" +
                "  foreign key (`b` , `c` , `d`)\n" +
                "       REFERENCES `user2` (`b` , `c` , `d`)\n" +
                "  CONSTRAINT `fk~special!@#` foreign key (`e`) REFERENCES `user3` (`e`),\n" +
                "  CONSTRAINT `fk@特殊字符` foreign key (`f`) REFERENCES `user4` (`f`),\n" +
                "  CONSTRAINT `fk-with-dash_and_underscore` foreign key (`g`) REFERENCES `user5` (`g`),\n" +
                "  CONSTRAINT `fk$#%^&*()` foreign key (`h`) REFERENCES `user6` (`h`),\n" +
                "  KEY `c` (`c`)\n" +
                ") SINGLE ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

        String modifiedSql = ForeignKeyUtils.addForeignKeyConstraints(createTableSql, fks);
        Assert.assertEquals(modifiedSql, "CREATE TABLE IF NOT EXISTS device (" +
            "  a INT PRIMARY KEY AUTO_INCREMENT," +
            "  b INT NOT NULL," +
            "  c INT NOT NULL," +
            "  d INT NOT NULL," +
            "  KEY `device_ibfk_1` (`b`)," +
            "  CONSTRAINT `device_ibfk_1` foreign key (`b`) REFERENCES `user2` (`a`),\n" +
            "  CONSTRAINT `device_ibfk_2` foreign key `fk` (`b`) REFERENCES `user2` (`b`),\n" +
            "  constraint `my_ibfk` foreign key (`b`) REFERENCES `user2` (`c`),\n" +
            "  constraint `my_ibfk_1` foreign key `fk1` (`c`) REFERENCES `user2` (`c`),\n" +
            "  CONSTRAINT `device_ibfk_3` foreign key (`c`) REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE,\n"
            +
            "  CONSTRAINT `device_ibfk_4` foreign key (`d`) REFERENCES `device` (`c`),\n" +
            "  constraint `fk_device_user` foreign key (`b` , `c` , `d`)\n" +
            "       REFERENCES `user2` (`b` , `c` , `d`)\n" +
            "  CONSTRAINT `device_ibfk_5` foreign key (`b` , `c` , `d`)\n" +
            "       REFERENCES `user2` (`b` , `c` , `d`)\n" +
            "  CONSTRAINT `fk~special!@#` foreign key (`e`) REFERENCES `user3` (`e`),\n" +
            "  CONSTRAINT `fk@特殊字符` foreign key (`f`) REFERENCES `user4` (`f`),\n" +
            "  CONSTRAINT `fk-with-dash_and_underscore` foreign key (`g`) REFERENCES `user5` (`g`),\n" +
            "  CONSTRAINT `fk$#%^&*()` foreign key (`h`) REFERENCES `user6` (`h`),\n" +
            "  KEY `c` (`c`)\n" +
            ") SINGLE ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
    }

}
