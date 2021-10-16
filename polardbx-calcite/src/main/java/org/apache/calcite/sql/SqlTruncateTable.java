/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Parse tree for {@code TRUNCATE TABLE} statement.
 */
public class SqlTruncateTable extends SqlDrop {
  private boolean purge = false;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("TRUNCATE TABLE", SqlKind.TRUNCATE_TABLE);

  /** Creates a SqlDropTable. */
  SqlTruncateTable(SqlParserPos pos, boolean ifExists, SqlIdentifier name, boolean purge) {
    super(OPERATOR, pos, ifExists);
    this.name = name;
    this.purge = purge;
  }

  public SqlNode getTargetTable() {
    return name;
  }

  public void setTargetTable(SqlNode targetTable) {
    this.name = targetTable;
  }

  public boolean isPurge() {
    return purge;
  }

  public void setPurge(boolean purge) {
    this.purge = purge;
  }
}

// End SqlDropTable.java
