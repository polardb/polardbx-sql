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

package org.apache.calcite.sql.validate;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ColumnarForbidTest {

    @Test(expected = TddlRuntimeException.class)
    public void ForbidCitableModifyUnsupportedType() {
        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        SqlColumnDeclaration sqlColumnDeclaration = mock(SqlColumnDeclaration.class);
        SqlDataTypeSpec sqlDataTypeSpec = mock(SqlDataTypeSpec.class);

        when(sqlColumnDeclaration.getDataType()).thenReturn(sqlDataTypeSpec);
        when(sqlDataTypeSpec.getTypeName()).thenReturn(sqlIdentifier);
        when(sqlIdentifier.getLastName()).thenReturn("binary");

        SqlValidatorImpl.validateUnsupportedTypeWithCciWhenModifyColumn(sqlColumnDeclaration);
    }

    @Test
    public void ForbidCitableModifySupportedType() {
        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        SqlColumnDeclaration sqlColumnDeclaration = mock(SqlColumnDeclaration.class);
        SqlDataTypeSpec sqlDataTypeSpec = mock(SqlDataTypeSpec.class);

        when(sqlColumnDeclaration.getDataType()).thenReturn(sqlDataTypeSpec);
        when(sqlDataTypeSpec.getTypeName()).thenReturn(sqlIdentifier);
        when(sqlIdentifier.getLastName()).thenReturn("char");

        SqlValidatorImpl.validateUnsupportedTypeWithCciWhenModifyColumn(sqlColumnDeclaration);
    }
}
