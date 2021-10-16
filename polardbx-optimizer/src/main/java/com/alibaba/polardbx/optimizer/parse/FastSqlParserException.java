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

package com.alibaba.polardbx.optimizer.parse;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 * @create 2018-03-27 19:12
 */
public class FastSqlParserException extends UnsupportedOperationException{

    private ExceptionType exceptionType;

    public FastSqlParserException(ExceptionType exceptionType, String message) {
        super(message);
    }

    public FastSqlParserException(ExceptionType exceptionType, Exception e) {
        super(e);
    }

    public FastSqlParserException(ExceptionType exceptionType, Exception e , String message) {
        super(message,e);
    }

    public ExceptionType getExceptionType() {
        return exceptionType;
    }

    public static enum ExceptionType {
        NOT_SUPPORT,NEED_IMPLEMENT,PARSER_ERROR;
    }
}
