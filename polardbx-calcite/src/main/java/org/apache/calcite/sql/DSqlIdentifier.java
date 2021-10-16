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

/**
 * Created by lingce.ldm on 16/9/22.
 */
public class DSqlIdentifier extends SqlIdentifier {
  private boolean isUserVar;
  private boolean isSysVar;
  private boolean isSessionVar;

  private DSqlIdentifier(
      SqlIdentifier identifier,
      boolean isUserVar,
      boolean isSysVar,
      boolean isSessionVar) {
    super(identifier.names, identifier.getParserPosition());
    this.isUserVar = isUserVar;
    this.isSysVar = isSysVar;
    this.isSessionVar = isSessionVar;
  }

  public static DSqlIdentifier sysVar(SqlIdentifier identifier, boolean isSessionVar) {
    return new DSqlIdentifier(identifier, false, true, isSessionVar);
  }

  public static DSqlIdentifier userVar(SqlIdentifier identifier) {
    return new DSqlIdentifier(identifier, true, false, false);
  }

  public boolean isUserVar() {
    return isUserVar;
  }

  public boolean isSysVar() {
    return isSysVar;
  }

  public boolean isSessionVar() {
    return isSessionVar;
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.IDENTIFIER);
    if (isSysVar) {
      writer.keyword("@@");
      String keyword = "global";
      if (isSessionVar) {
        keyword = "session";
      }
      writer.keyword(keyword);
      writer.sep(".");
    } else if (isUserVar) {
      writer.keyword("@");
    }
    for (String name : names) {
      writer.sep(".");
      if (name.equals("")) {
        writer.print("*");
      } else {
        writer.identifier(name);
      }
    }
    writer.endList(frame);
  }
}

// End DSqlIdentifier.java
