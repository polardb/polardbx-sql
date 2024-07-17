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

package org.apache.calcite.sql.test;

import org.apache.calcite.sql.SqlCreateTable;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class BuildForeignKeyNameTest {

    @Test
    public void testBuildForeignKeyName() {
        String fkName1 = "tab_ibfk_1";
        String fkName2 = " tab_ibfk_2";
        String fkName3 = " a tab_ibfk_3";

        Assert.assertEquals("`tab_ibfk_1`", SqlCreateTable.buildForeignKeyName(fkName1, new HashSet<>(),""));
        Assert.assertEquals("` tab_ibfk_2`", SqlCreateTable.buildForeignKeyName(fkName2, new HashSet<>(),""));
        Assert.assertEquals("` a tab_ibfk_3`", SqlCreateTable.buildForeignKeyName(fkName3, new HashSet<>(),""));
    }
}
