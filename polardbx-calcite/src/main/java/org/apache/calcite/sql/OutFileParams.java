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

package org.apache.calcite.sql;

import java.io.Serializable;

public class OutFileParams implements Serializable {
    private String fileName;
    private String charset;
    private String lineTerminatedBy;
    private String linesStartingBy;
    private String fieldTerminatedBy;
    private Byte fieldEnclose;
    private Byte fieldEscape;
    private boolean fieldEnclosedOptionally = false;
    private Object columnMeata = new Object();

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getLineTerminatedBy() {
        return lineTerminatedBy;
    }

    public void setLineTerminatedBy(String lineTerminatedBy) {
        this.lineTerminatedBy = lineTerminatedBy;
    }

    public String getLinesStartingBy() {
        return linesStartingBy;
    }

    public void setLinesStartingBy(String linesStartingBy) {
        this.linesStartingBy = linesStartingBy;
    }

    public String getFieldTerminatedBy() {
        return fieldTerminatedBy;
    }

    public void setFieldTerminatedBy(String fieldTerminatedBy) {
        this.fieldTerminatedBy = fieldTerminatedBy;
    }

    public Byte getFieldEnclose() {
        return fieldEnclose;
    }

    public void setFieldEnclose(Byte fieldEnclose) {
        this.fieldEnclose = fieldEnclose;
    }

    public Byte getFieldEscape() {
        return fieldEscape;
    }

    public void setFieldEscape(Byte fieldEscape) {
        this.fieldEscape = fieldEscape;
    }

    public boolean isFieldEnclosedOptionally() {
        return fieldEnclosedOptionally;
    }

    public void setFieldEnclosedOptionally(boolean fieldEnclosedOptionally) {
        this.fieldEnclosedOptionally = fieldEnclosedOptionally;
    }

    public Object getColumnMeata() {
        return columnMeata;
    }

    public void setColumnMeata(Object columnMeata) {
        this.columnMeata = columnMeata;
    }
}
