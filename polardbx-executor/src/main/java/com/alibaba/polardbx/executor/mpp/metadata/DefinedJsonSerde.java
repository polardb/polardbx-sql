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

package com.alibaba.polardbx.executor.mpp.metadata;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.alibaba.polardbx.common.jdbc.RawString.RawS;
import static com.alibaba.polardbx.executor.mpp.metadata.GenericJsonVal.GJVal;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.util.Objects.requireNonNull;

public class DefinedJsonSerde {

    private static final String TYPE_PROPERTY = "@type";
    private static final Cache<Class<?>, JsonSerializer<Object>> serializerCache =
        CacheBuilder.newBuilder().softValues().maximumSize(100).build();
    private static final Cache<String, JavaType> serializerTypeCache =
        CacheBuilder.newBuilder().softValues().maximumSize(100).build();
    private static final List<String> PACKAGES =
        ImmutableList.of(
            "com.alibaba.polardbx.common.datatype.",
            "java.sql.",
            "java.lang.",
            "java.math.",
            "com.alibaba.polardbx.common.utils.bloomfilter.",
            "com.alibaba.polardbx.executor.mpp.execution.");

    private static final Set<Class> supportJsonClasses = ImmutableSet.of(
        Byte.class, Short.class, Integer.class, Double.class, Float.class, Boolean.class,
        java.util.Date.class, java.sql.Date.class, Time.class, Timestamp.class, String.class, Long.class,
        BigInteger.class, BigDecimal.class, byte[].class, UInt64.class, BloomFilterInfo.class, RawString.class);

    public static class ParameterContextDeserializer extends StdDeserializer<ParameterContext> {

        private final TypeDeserializer typeDeserializer;

        public ParameterContextDeserializer() {
            super(ParameterContext.class);
            this.typeDeserializer = new AsPropertyTypeDeserializer(
                TypeFactory.defaultInstance().constructType(Object.class),
                new InternalTypeResolver(),
                TYPE_PROPERTY,
                false,
                null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public ParameterContext deserialize(JsonParser jsonParser,
                                            DeserializationContext deserializationContext)
            throws IOException {

            int ordinal = -1;
            JavaType intType =
                deserializationContext.getTypeFactory().constructType(int.class);
            JsonDeserializer<Object> intDeser = deserializationContext.findRootValueDeserializer(intType);

            List<Object> objects = new ArrayList<>();
            JsonToken t;
            int index = 0;
            //start array
            while ((t = jsonParser.nextToken()) != JsonToken.END_ARRAY) {
                try {
                    //1. deSerialize the method
                    if (index == 0) {
                        ordinal = (int) intDeser.deserialize(jsonParser, deserializationContext);
                    } else if (index == 1) {
                        // 2. deSerialize objects
                        int indexParam = (int) intDeser.deserialize(jsonParser, deserializationContext);
                        objects.add(indexParam);
                    } else {
                        if (t == JsonToken.VALUE_NULL) {
                            objects.add(null);
                        } else {
                            Object ret = typeDeserializer.deserializeTypedFromAny(jsonParser, deserializationContext);
                            if (ret instanceof GenericJsonVal) {
                                ret = ((GenericJsonVal) ret).getObject();
                            }
                            objects.add(ret);
                        }
                    }
                    index++;
                } catch (Exception var9) {
                    throw new IOException(var9);
                }
            }
            ParameterMethod[] methods = ParameterMethod.values();
            return new ParameterContext(methods[ordinal], objects.toArray());
        }
    }

    public static class ParameterContextSerializer
        extends StdSerializer<ParameterContext> {

        private final TypeSerializer typeSerializer;

        public ParameterContextSerializer() {
            super(ParameterContext.class);
            this.typeSerializer = new AsPropertyTypeSerializer(new InternalTypeResolver(), null, TYPE_PROPERTY);
        }

        @Override
        public void serialize(ParameterContext value, JsonGenerator generator, SerializerProvider provider)
            throws IOException {
            if (value == null) {
                provider.defaultSerializeNull(generator);
                return;
            }

            try {
                ParameterMethod method = value.getParameterMethod();
                int ordinal = method.ordinal();
                JsonSerializer<Object> intSerializer =
                    serializerCache.get(Integer.class, () -> createSerializer(provider, Integer.TYPE));
                Object[] objects = value.getArgs();
                int objectLen = objects.length;

                generator.writeStartArray(value, objectLen + 1);
                // 1. serialize the method
                intSerializer.serialize(ordinal, generator, provider);
                //2. serialize the objects
                if (objectLen > 0) {
                    for (int i = 0; i < objects.length; i++) {
                        Object object = objects[i];
                        if (object == null) {
                            provider.defaultSerializeNull(generator);
                            continue;
                        }
                        if (i == 0) {
                            //the first values is integer
                            intSerializer.serialize(object, generator, provider);
                        } else {
                            Class<?> objectType = object.getClass();
                            if (!DynamicConfig.getInstance().useJdkDefaultSer() ||
                                supportJsonClasses.contains(objectType)) {
                                //use json
                                JsonSerializer<Object> objectSerializer =
                                    serializerCache.get(objectType, () -> createSerializer(provider, objectType));
                                objectSerializer.serializeWithType(object, generator, provider, typeSerializer);
                            } else {
                                //use jdk which is low efficiency!
                                GenericJsonVal genericParameterVal = new GenericJsonVal(object);
                                JsonSerializer<Object> objectSerializer =
                                    serializerCache.get(
                                        objectType, () -> createSerializer(provider, genericParameterVal.getClass()));
                                objectSerializer.serializeWithType(
                                    genericParameterVal, generator, provider, typeSerializer);
                            }

                        }
                    }
                }
                generator.writeEndArray();
            } catch (ExecutionException e) {
                propagateIfInstanceOf(e.getCause(), IOException.class);
                throw Throwables.propagate(e.getCause());
            }
        }
    }

    public static class RawStringDeserializer extends StdDeserializer<RawString> {

        private final TypeDeserializer typeDeserializer;

        public RawStringDeserializer() {
            super(ParameterContext.class);
            this.typeDeserializer = new AsPropertyTypeDeserializer(
                TypeFactory.defaultInstance().constructType(Object.class),
                new InternalTypeResolver(),
                TYPE_PROPERTY,
                false,
                null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public RawString deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {

            // 1. deSerialize lists
            List<Object> objects = new ArrayList<>();
            JsonToken t;
            boolean startWithFieldName = false;
            if (jsonParser.currentToken() == JsonToken.FIELD_NAME) {
                jsonParser.nextToken();
                startWithFieldName = true;
            }
            //start array
            while ((t = jsonParser.nextToken()) != JsonToken.END_ARRAY) {
                try {
                    if (t == JsonToken.VALUE_NULL) {
                        objects.add(null);
                    } else {
                        Object ret = typeDeserializer.deserializeTypedFromAny(jsonParser, deserializationContext);
                        if (ret instanceof GenericJsonVal) {
                            ret = ((GenericJsonVal) ret).getObject();
                        }
                        objects.add(ret);
                    }
                } catch (Exception var9) {
                    throw new IOException(var9);
                }
            }
            if (startWithFieldName) {
                jsonParser.nextToken();
            }
            return new RawString(objects);
        }
    }

    public static class RawStringSerializer extends StdSerializer<RawString> {

        private final TypeSerializer typeSerializer;

        public RawStringSerializer() {
            super(RawString.class);
            this.typeSerializer = new AsPropertyTypeSerializer(new InternalTypeResolver(), null, TYPE_PROPERTY);
        }

        @Override
        public void serializeWithType(
            RawString value, JsonGenerator g, SerializerProvider provider, TypeSerializer typeSer) throws IOException {
            WritableTypeId typeIdDef = typeSer.writeTypePrefix(g, typeSer.typeId(value, JsonToken.VALUE_STRING));
            this.serialize(value, g, provider);
            typeSer.writeTypeSuffix(g, typeIdDef);
        }

        @Override
        public void serialize(RawString value, JsonGenerator generator, SerializerProvider provider)
            throws IOException {
            if (value == null) {
                provider.defaultSerializeNull(generator);
                return;
            }

            try {
                //1. serialize the lists
                List objects = value.getObjList();
                int len = objects.size();
                generator.writeStartArray(value, len);
                if (len > 0) {
                    for (int i = 0; i < objects.size(); i++) {
                        Object object = objects.get(i);
                        if (object == null) {
                            provider.defaultSerializeNull(generator);
                            continue;
                        }
                        Class<?> objectType = object.getClass();
                        if (!DynamicConfig.getInstance().useJdkDefaultSer() ||
                            supportJsonClasses.contains(objectType)) {
                            //use json
                            JsonSerializer<Object> objectSerializer =
                                serializerCache.get(objectType, () -> createSerializer(provider, objectType));
                            objectSerializer.serializeWithType(object, generator, provider, typeSerializer);
                        } else {
                            //use jdk which is low efficiency!
                            GenericJsonVal genericParameterVal = new GenericJsonVal(object);
                            JsonSerializer<Object> objectSerializer =
                                serializerCache.get(
                                    objectType, () -> createSerializer(provider, genericParameterVal.getClass()));
                            objectSerializer.serializeWithType(
                                genericParameterVal, generator, provider, typeSerializer);
                        }
                    }
                }
                generator.writeEndArray();
            } catch (ExecutionException e) {
                propagateIfInstanceOf(e.getCause(), IOException.class);
                throw Throwables.propagate(e.getCause());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> JsonSerializer<T> createSerializer(SerializerProvider provider, Class<?> type)
        throws JsonMappingException {
        JsonSerializer<Object> ser = provider.findTypedValueSerializer(type, true, (BeanProperty) null);
        return (JsonSerializer<T>) ser;
    }

    private static class InternalTypeResolver extends TypeIdResolverBase {

        public InternalTypeResolver() {
        }

        @Override
        public String idFromValue(Object value) {
            return idFromValueAndType(value, value.getClass());
        }

        @SuppressWarnings("unchecked")
        @Override
        public String idFromValueAndType(Object value, Class<?> suggestedType) {
            requireNonNull(value, "value is null");
            String type;
            if (value instanceof GenericJsonVal) {
                type = GJVal;
            } else if (value instanceof RawString) {
                type = RawS;
            } else {
                type = value.getClass().getName();
                for (String package_ : PACKAGES) {
                    if (type.startsWith(package_)) {
                        String remaining = type.substring(package_.length());
                        if (remaining.indexOf('.') < 0) {
                            type = remaining;
                            break;
                        }
                    }
                }
            }
            checkArgument(type != null, "Unknown class: %s", suggestedType.getSimpleName());
            return type;
        }

        @Override
        public JavaType typeFromId(DatabindContext context, String id) {
            requireNonNull(id, "id is null");
            try {
                JavaType javaType = serializerTypeCache.get(id, () -> {
                    Class<?> typeClass = null;
                    if (GJVal.equals(id)) {
                        typeClass = GenericJsonVal.class;
                    } else if (RawS.equals(id)) {
                        typeClass = RawString.class;
                    } else {
                        if (!id.contains(".")) {
                            for (String package_ : PACKAGES) {
                                try {
                                    typeClass = Class.forName(package_ + id);
                                    break;
                                } catch (ClassNotFoundException e) {
                                    // ignore
                                }
                            }
                        }
                        if (typeClass == null) {
                            typeClass = Class.forName(id);
                        }
                    }
                    checkArgument(typeClass != null, "Unknown type ID: %s", id);
                    return context.getTypeFactory().constructType(typeClass);
                });
                return javaType;
            } catch (Throwable t) {
                return null;
            }
        }

        @Override
        public JsonTypeInfo.Id getMechanism() {
            return JsonTypeInfo.Id.NAME;
        }
    }
}
