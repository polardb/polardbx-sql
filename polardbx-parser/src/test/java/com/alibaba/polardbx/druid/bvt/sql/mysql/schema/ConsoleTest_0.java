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

package com.alibaba.polardbx.druid.bvt.sql.mysql.schema;

import com.alibaba.polardbx.druid.sql.repository.SchemaObject;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

public class ConsoleTest_0 extends TestCase {
    public void test_for_console() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "create table yushitai_test.card_record ( id bigint auto_increment) auto_increment=256 "
                + "; rename /* gh-ost */ table yushitai_test.card_record to yushitai_test._card_record_del;";
        repository.console(sql);
        assertNotNull(
                repository.findTable("_card_record_del")
        );

        assertNotNull(
                repository.findTable("yushitai_test._card_record_del")
        );
    }

    public void test_for_console_1() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "create table yushitai_test.card_record ( id bigint auto_increment) auto_increment=256 "
                + "; rename /* gh-ost */ table yushitai_test.card_record to _card_record_del;";
        repository.console(sql);
        assertNotNull(
                repository.findTable("_card_record_del")
        );

        assertNotNull(
                repository.findTable("yushitai_test._card_record_del")
        );

        SchemaObject table = repository.findTable("_card_record_del");
        assertEquals("_card_record_del", table.getName());
    }

    public void test_for_console_2() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "create table quniya4(name varchar(255) null,value varchar(255) null,id int not null,constraint quniya4_pk primary key (id));"
                + "alter table quniya4 modify id int not null first;";

        repository.console(sql);

        repository.setDefaultSchema("test");
        SchemaObject table = repository.findTable("quniya4");
        assertEquals("CREATE TABLE quniya4 (\n" +
                "\tid int NOT NULL,\n" +
                "\tname varchar(255) NULL,\n" +
                "\tvalue varchar(255) NULL,\n" +
                "\tPRIMARY KEY (id)\n" +
                ")", table.getStatement().toString());
    }
}
