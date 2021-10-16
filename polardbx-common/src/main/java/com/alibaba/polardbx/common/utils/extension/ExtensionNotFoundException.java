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

package com.alibaba.polardbx.common.utils.extension;

import org.apache.commons.lang.exception.NestableRuntimeException;

public class ExtensionNotFoundException extends NestableRuntimeException {

    private static final long serialVersionUID = -654893533794556357L;

    public ExtensionNotFoundException(String errorCode) {
        super(errorCode);
    }

    public ExtensionNotFoundException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public ExtensionNotFoundException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public ExtensionNotFoundException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public ExtensionNotFoundException(Throwable cause) {
        super(cause);
    }

    public Throwable fillInStackTrace() {
        return this;
    }

}
