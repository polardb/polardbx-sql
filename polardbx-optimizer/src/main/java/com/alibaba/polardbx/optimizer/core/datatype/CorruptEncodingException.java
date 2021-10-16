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

package com.alibaba.polardbx.optimizer.core.datatype;

import org.apache.commons.lang.exception.NestableRuntimeException;

public class CorruptEncodingException extends NestableRuntimeException {

    private static final long serialVersionUID = -7798002309588878953L;

    public CorruptEncodingException(String message) {
        super(message);
    }

    public CorruptEncodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public CorruptEncodingException(Throwable cause) {
        super(cause);
    }
}
