package com.alibaba.polardbx.executor.operator.scan;

import org.junit.Test;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

import java.nio.charset.StandardCharsets;

public class ArrowVectorTest {
    @Test
    public void test1() {
        try (
            BufferAllocator allocator = new RootAllocator();
            IntVector intVector = new IntVector("intVector", allocator)
        ) {
            intVector.allocateNew(3);
            intVector.set(0, 1);
            intVector.set(1, 2);
            intVector.set(2, 3);
            intVector.setValueCount(3);

            System.out.print(intVector);
        }
    }

    @Test
    public void test2() {
        try (
            BufferAllocator allocator = new RootAllocator();
            VarCharVector varCharVector = new VarCharVector("varCharVector", allocator);
        ) {
            varCharVector.allocateNew(3);
            varCharVector.set(0, "one".getBytes());
            varCharVector.set(1, "two".getBytes());
            varCharVector.set(2, "three".getBytes());
            varCharVector.setValueCount(3);

            System.out.print(varCharVector);
        }
    }

    @Test
    public void test3() {
        try (BufferAllocator root = new RootAllocator();
            VarCharVector countries = new VarCharVector("country-dict", root);
            VarCharVector appUserCountriesUnencoded = new VarCharVector("app-use-country-dict", root)
        ) {
            countries.allocateNew(10);
            countries.set(0, "Andorra".getBytes(StandardCharsets.UTF_8));
            countries.set(1, "Cuba".getBytes(StandardCharsets.UTF_8));
            countries.set(2, "Grecia".getBytes(StandardCharsets.UTF_8));
            countries.set(3, "Guinea".getBytes(StandardCharsets.UTF_8));
            countries.set(4, "Islandia".getBytes(StandardCharsets.UTF_8));
            countries.set(5, "Malta".getBytes(StandardCharsets.UTF_8));
            countries.set(6, "Tailandia".getBytes(StandardCharsets.UTF_8));
            countries.set(7, "Uganda".getBytes(StandardCharsets.UTF_8));
            countries.set(8, "Yemen".getBytes(StandardCharsets.UTF_8));
            countries.set(9, "Zambia".getBytes(StandardCharsets.UTF_8));
            countries.setValueCount(10);

            Dictionary countriesDictionary = new Dictionary(countries,
                new DictionaryEncoding(/*id=*/1L, /*ordered=*/false, /*indexType=*/new ArrowType.Int(8, true)));
            System.out.println("Dictionary: " + countriesDictionary);

            appUserCountriesUnencoded.allocateNew(5);
            appUserCountriesUnencoded.set(0, "Andorra".getBytes(StandardCharsets.UTF_8));
            appUserCountriesUnencoded.set(1, "Guinea".getBytes(StandardCharsets.UTF_8));
            appUserCountriesUnencoded.set(2, "Islandia".getBytes(StandardCharsets.UTF_8));
            appUserCountriesUnencoded.set(3, "Malta".getBytes(StandardCharsets.UTF_8));
            appUserCountriesUnencoded.set(4, "Uganda".getBytes(StandardCharsets.UTF_8));
            appUserCountriesUnencoded.setValueCount(5);
            System.out.println("Unencoded data: " + appUserCountriesUnencoded);

            try (FieldVector appUserCountriesDictionaryEncoded = (FieldVector) DictionaryEncoder
                .encode(appUserCountriesUnencoded, countriesDictionary)) {
                System.out.println("Dictionary-encoded data: " + appUserCountriesDictionaryEncoded);
            }
        }
    }
}
