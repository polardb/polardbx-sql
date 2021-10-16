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

package com.alibaba.polardbx.executor.ddl.newengine.serializable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class DdlSerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlSerializer.class);

    public static String serializeToJSON(Object object) {
        try {
            return JSON.toJSONString(object, SerializerFeature.WriteClassName);
        } catch (Exception e) {
            throw DdlHelper.logAndThrowError(LOGGER, "Failed to serialize an object to a JSON string", e);
        }
    }

    public static Object deserializeJSON(String jsonString) {
        try {
            return JSON.parse(jsonString);
        } catch (Exception e) {
            throw DdlHelper.logAndThrowError(LOGGER, "Failed to deserialize a JSON string to an object ", e);
        }
    }

    public static byte[] serializeToBytes(Serializable object) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw DdlHelper.logAndThrowError(LOGGER, "Failed to serialize an object to a byte array", e);
        }
    }

    public static Serializable deserializeBytes(byte[] bytes) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (Serializable) ois.readObject();
        } catch (Exception e) {
            throw DdlHelper.logAndThrowError(LOGGER, "Failed to deserialize a byte array to an object", e);
        }
    }

}
