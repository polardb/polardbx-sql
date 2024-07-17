package com.alibaba.polardbx.common;

import org.junit.Assert;
import org.junit.Test;

import java.security.SecureRandom;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RevisableOrderInvariantHashTest {
    Random random = new SecureRandom();
    NumberFormat numberFormat = NumberFormat.getNumberInstance();

    /**
     * Basic usage of DynamicHash.
     */
    @Test
    public void usageExamples() {
        {
            // order invariant.
            long a = new RevisableOrderInvariantHash().add(1).add(2).add(3).getResult();
            long b = new RevisableOrderInvariantHash().add(3).add(2).add(1).getResult();
            Assert.assertEquals(a, b);
        }

        {
            // Revisable case 1.
            long a = new RevisableOrderInvariantHash().add(2).add(3).getResult();
            long b = new RevisableOrderInvariantHash().add(1).add(2).add(3).remove(1).getResult();
            Assert.assertEquals(a, b);
        }

        {
            // Revisable case 2.
            // Suppose you have 2 arrays: a = {1, 2} and b = {3}
            RevisableOrderInvariantHash a = new RevisableOrderInvariantHash().add(1).add(2);
            RevisableOrderInvariantHash b = new RevisableOrderInvariantHash().add(3);
            // And you have another array c = {1, 2, 3}
            RevisableOrderInvariantHash c = new RevisableOrderInvariantHash().add(1).add(2).add(3);
            // You expect hash(a, b) = hash (c)
            // Add one result of Dynamic hash, remember to remove one zero.
            // Remove one result of Dynamic hash, remember to add one zero back (See the next case).
            RevisableOrderInvariantHash ab = new RevisableOrderInvariantHash()
                .add(a.getResult()).remove(0)
                .add(b.getResult()).remove(0);
            Assert.assertEquals(c.getResult(), ab.getResult());
        }

        {
            // Revisable case 3.
            // Suppose data in innodb are: a = {1, 2}
            RevisableOrderInvariantHash a = new RevisableOrderInvariantHash().add(1).add(2);
            // And data in orc files (containing deleted data): b = {1, 2, 3, 4}
            RevisableOrderInvariantHash b = new RevisableOrderInvariantHash().add(1).add(2).add(3).add(4);
            // And data in deleted-bitmap: c = {3, 4}
            // We want to prove innodb = orc - deleted-bitmap: a = (b - c)

            // One way to do that is proving b + (-c) = a
            RevisableOrderInvariantHash removeC = new RevisableOrderInvariantHash().remove(3).remove(4);
            RevisableOrderInvariantHash bRemoveC = new RevisableOrderInvariantHash()
                .add(b.getResult()).remove(0)
                .add(removeC.getResult()).remove(0);
            Assert.assertEquals(a.getResult(), bRemoveC.getResult());

            // Another way is proving b - c = a
            // Remove one result of Dynamic hash, remember to add one zero back.
            RevisableOrderInvariantHash c = new RevisableOrderInvariantHash().add(3).add(4);
            RevisableOrderInvariantHash bRemoveC2 = new RevisableOrderInvariantHash()
                .add(b.getResult()).remove(0)
                .remove(c.getResult()).add(0);
            Assert.assertEquals(a.getResult(), bRemoveC2.getResult());
        }
    }

    private long mod(long x) {
        return x & ((1L << 63) - 1);
    }

    private long mod2(long x) {
        return x & ((1L << 31) - 1);
    }

    private long mod(long a, long b) {
        long result = a % b;
        if (result < 0) {
            result += b;
        }
        return result;
    }

    @Test
    public void orderInvariantTest() {
        {
            // Simple test.
            List<Long> arr0 = Arrays.asList(0L, 100L, 200L, 1L << 30, 1L << 31, 1L << 32, 1L << 33, 1L << 63 - 1, 100L);
            Long hash0 = verifyRemoval(arr0);
            Collections.shuffle(arr0, random);
            Long hash1 = verifyRemoval(arr0);
            Assert.assertEquals(hash0, hash1);
            long before = System.nanoTime();
            RevisableOrderInvariantHash hash = new RevisableOrderInvariantHash();
            for (Long x : arr0) {
                hash.add(x);
            }
            long duration = System.nanoTime() - before;
            System.out.println("Simple test cost " + numberFormat.format(duration) + " ns, hash: " + hash.getResult());
        }

        {
            // Random generate 100w positive long integers.
            List<Long> arr0 = Stream
                .generate(() -> mod(random.nextLong()))
                .limit(1000000)
                .collect(Collectors.toList());
            Long hash0 = verifyRemoval(arr0);
            Collections.shuffle(arr0, random);
            Long hash1 = verifyRemoval(arr0);
            Assert.assertEquals(hash0, hash1);
            RevisableOrderInvariantHash hash = new RevisableOrderInvariantHash();
            long before = System.nanoTime();
            for (Long x : arr0) {
                hash.add(x);
            }
            long duration = System.nanoTime() - before;
            System.out.println(
                "Hashing 100w elements cost " + numberFormat.format(duration) + " ns, hash: " + hash.getResult());

            hash.reset();
            before = System.nanoTime();
            for (Long x : arr0) {
                hash.addNoMod(x);
            }
            duration = System.nanoTime() - before;
            System.out.println(
                "Hashing 100w elements (allow overflow) cost " + numberFormat.format(duration) + " ns, hash: "
                    + hash.getResult());
        }
    }

    @Test
    public void removalTest() {
        {
            // Simple test.
            List<Long> arr0 = Arrays.asList(0L, 100L, 200L, 1L << 30, 1L << 31, 1L << 32, 1L << 33, 1L << 63 - 1, 100L);
            // Hash code after removing the i-th element.
            List<Long> expected = new ArrayList<>();
            RevisableOrderInvariantHash hash = new RevisableOrderInvariantHash();
            for (int i = 0; i < arr0.size(); i++) {
                hash.reset();
                for (int j = 0; j < arr0.size(); j++) {
                    if (i == j) {
                        // Skip i-th element.
                        continue;
                    }
                    hash.add(arr0.get(j));
                }
                expected.add(hash.getResult());
            }

            hash.reset();
            for (Long x : arr0) {
                hash.add(x);
            }

            Long all = hash.getResult();
            for (int i = 0; i < arr0.size(); i++) {
                Assert.assertEquals(hash.getResult(), all);
                long x = arr0.get(i);
                // remove x
                Assert.assertEquals(hash.remove(x).getResult(), expected.get(i));
                // add it back
                Assert.assertEquals(hash.add(x).getResult(), all);
            }
        }

        {
            // Random generate 1w positive long integers.
            List<Long> arr0 = Stream
                .generate(() -> mod(random.nextLong()))
                .limit(10000)
                .collect(Collectors.toList());
            // Hash code after removing the i-th element.
            List<Long> expected = new ArrayList<>();
            RevisableOrderInvariantHash hash = new RevisableOrderInvariantHash();
            for (int i = 0; i < arr0.size(); i++) {
                hash.reset();
                for (int j = 0; j < arr0.size(); j++) {
                    if (i == j) {
                        // Skip i-th element.
                        continue;
                    }
                    hash.add(arr0.get(j));
                }
                expected.add(hash.getResult());
            }

            hash.reset();
            for (Long x : arr0) {
                hash.add(x);
            }

            Long all = hash.getResult();
            for (int i = 0; i < arr0.size(); i++) {
                Assert.assertEquals(hash.getResult(), all);
                long x = arr0.get(i);
                // remove x
                Assert.assertEquals(hash.remove(x).getResult(), expected.get(i));
                // add it back
                Assert.assertEquals(hash.add(x).getResult(), all);
            }
        }

        {
            // Random generate 100w positive long integers.
            List<Long> arr0 = Stream
                .generate(() -> mod(random.nextLong()))
                .limit(1000000)
                .collect(Collectors.toList());
            List<Long> extra = Stream
                .generate(() -> mod(random.nextLong()))
                .limit(10000)
                .collect(Collectors.toList());
            // hash0 = arr0 + extra - extra
            RevisableOrderInvariantHash hash0 = new RevisableOrderInvariantHash();
            int i = 0;
            for (Long x : arr0) {
                hash0.add(x);
                if (i < extra.size()) {
                    hash0.add(extra.get(i));
                    i++;
                }
            }
            Collections.shuffle(extra);
            for (Long x : extra) {
                hash0.remove(x);
            }
            // hash1 = arr0
            RevisableOrderInvariantHash hash1 = new RevisableOrderInvariantHash();
            Collections.shuffle(arr0);
            for (Long x : arr0) {
                hash1.add(x);
            }
            Assert.assertEquals(hash0.getResult(), hash1.getResult());
        }
    }

    @Test
    public void testFastMod() {
        long x = 100;
        long m = 1L << 31;
        Assert.assertEquals(x % m, RevisableOrderInvariantHash.mod(x));
        x = -100;
        Assert.assertEquals((x % m) + m, RevisableOrderInvariantHash.mod(x));
        x = (1L << 32) + 333;
        Assert.assertEquals(x % m, RevisableOrderInvariantHash.mod(x));
        x = (-1L << 32) - 333;
        Assert.assertEquals((x % m) + m, RevisableOrderInvariantHash.mod(x));
        x = (1L << 62) + 333;
        Assert.assertEquals(x % m, RevisableOrderInvariantHash.mod(x));
        x = (-1L << 62) - 333;
        Assert.assertEquals((x % m) + m, RevisableOrderInvariantHash.mod(x));
        x = -1;
        Assert.assertEquals((x % m) + m, RevisableOrderInvariantHash.mod(x));
        x = (1L << 32);
        Assert.assertEquals(x % m, RevisableOrderInvariantHash.mod(x));
        x = (1L << 31);
        Assert.assertEquals(x % m, RevisableOrderInvariantHash.mod(x));

        int negative = 0;
        for (int i = 0; i < 10_000_000; i++) {
            x = random.nextLong();
            long result = (x % m);
            if (result >= 0) {
                Assert.assertEquals(result, RevisableOrderInvariantHash.mod(x));
            } else {
                negative++;
                Assert.assertEquals(result + m, RevisableOrderInvariantHash.mod(x));
            }
        }
        System.out.println("Total negative: " + negative);
    }

    @Test
    public void modularInverseTest() {
        RevisableOrderInvariantHash.ModularInverseSolver solver =
            new RevisableOrderInvariantHash.ModularInverseSolver();
        // gcd(a, b) = 1 -> (a * algorithm.i) mod b = 1
        long a = 101, b = 8;
        long inverse = solver.solve(a, b);
        Assert.assertEquals(mod(a * inverse, b), 1L);
        b = 1L << 31;
        for (int i = 0; i < 10_000_000; i++) {
            a = mod(random.nextLong(), b);
            if ((a & 1) == 0) {
                a++;
            }
            inverse = solver.solve(a, b);
            if (mod(a * inverse, b) != 1) {
                System.out.println("a: " + a + ", inverse: " + inverse);
                throw new RuntimeException();
            }
        }
    }

    @Test
    public void collisionTest() {
        RevisableOrderInvariantHash hash = new RevisableOrderInvariantHash();
        long m = 1L << 31;
        for (int i = 0; i < 100_000; i++) {
            for (int j = 0; j < 100; j++) {
                // large x
                long x = mod(random.nextLong(), m) + m;
                hash.add(x);
            }
            long r0 = hash.getResult();

            hash.reset();
            for (int j = 0; j < 100; j++) {
                // large x
                long x = mod(random.nextLong(), m) + m;
                hash.add(x);
            }
            long r1 = hash.getResult();
            Assert.assertNotEquals(r0, r1);
        }
    }

    @Test
    public void accumulateAddAndRemoveTest() {
        {
            // Simple test.
            long a = 1, b = (1L << 31) + 433, c = (1L << 32) + 233, d = 2077;
            long ha = new RevisableOrderInvariantHash().add(a).getResult();
            long hb = new RevisableOrderInvariantHash().add(b).getResult();
            long hc = new RevisableOrderInvariantHash().add(c).getResult();
            long hd = new RevisableOrderInvariantHash().add(d).getResult();
            // a + b
            long hab0 = new RevisableOrderInvariantHash().reset(ha).add(hb).remove(0).getResult();
            long hab1 = new RevisableOrderInvariantHash().add(a).add(b).getResult();
            Assert.assertEquals(hab0, hab1);
            // a + b + c
            long habc0 = new RevisableOrderInvariantHash().reset(ha).add(hb).remove(0).add(hc).remove(0).getResult();
            long habc1 = new RevisableOrderInvariantHash().reset(hab0).add(hc).remove(0).getResult();
            long habc2 = new RevisableOrderInvariantHash().reset(hc).add(hab0).remove(0).getResult();
            long habc3 = new RevisableOrderInvariantHash().add(a).add(b).add(c).getResult();
            Assert.assertEquals(habc0, habc1);
            Assert.assertEquals(habc0, habc2);
            Assert.assertEquals(habc0, habc3);
            // a + b + c + d
            long habcd0 = new RevisableOrderInvariantHash().add(a).add(b).add(c).add(d).getResult();
            long hcd0 = new RevisableOrderInvariantHash().add(c).add(d).getResult();
            long habcd1 = new RevisableOrderInvariantHash().reset(hcd0).add(hab0).remove(0).getResult();
            long habcd2 = new RevisableOrderInvariantHash().reset(hab0).add(c).add(d).getResult();
            long habcd3 = new RevisableOrderInvariantHash().add(ha).remove(0).add(hb).remove(0)
                .add(hc).remove(0).add(hd).remove(0).getResult();
            Assert.assertEquals(habcd0, habcd1);
            Assert.assertEquals(habcd0, habcd2);
            Assert.assertEquals(habcd0, habcd3);
        }

        {
            // Accumulate add.
            List<Long> arr0 = Stream
                .generate(() -> mod(random.nextLong()))
                .limit(1_000_000)
                .collect(Collectors.toList());

            // the i-th element in this array represents the hash of [0, i) in arr0.
            List<Long> result0 = new ArrayList<>(Collections.nCopies(arr0.size(), 0L));
            // the i-the element in this array represents the hash of [n-1-i, n-1] in arr0.
            List<Long> result1 = new ArrayList<>(Collections.nCopies(arr0.size(), 0L));
            // the hash code of all elements in arr0, hash calculated from first to last.
            RevisableOrderInvariantHash hash0 = new RevisableOrderInvariantHash();
            // the hash code of all elements in arr0, hash calculated from last to first.
            RevisableOrderInvariantHash hash1 = new RevisableOrderInvariantHash();

            for (int i = 0; i < arr0.size(); i++) {
                result0.set(i, hash0.getResult());
                hash0.add(arr0.get(i));
            }

            for (int i = arr0.size() - 1; i >= 0; i--) {
                hash1.add(arr0.get(i));
                result1.set(i, hash1.getResult());
            }

            Assert.assertEquals(hash0.getResult(), hash1.getResult());

            for (int i = 0; i < arr0.size(); i++) {
                RevisableOrderInvariantHash hashTmp = new RevisableOrderInvariantHash();
                hashTmp.reset(result0.get(i)).add(result1.get(i)).remove(0);
                Assert.assertEquals(hashTmp.getResult(), hash0.getResult());
            }
        }

        {
            // Accumulate remove.
            List<Long> arr0 = Stream
                .generate(() -> mod(random.nextLong()))
                .limit(1_000_000)
                .collect(Collectors.toList());

            List<Long> extra = Stream
                .generate(() -> mod(random.nextLong()))
                .limit(1_000_000)
                .collect(Collectors.toList());

            // hash code of arr0 + elements [0, i] from extra
            List<Long> result0 = new ArrayList<>(extra.size());
            // hash code of removing elements [0, i] from extra
            List<Long> result1 = new ArrayList<>(extra.size());
            // hash code of adding elements [0, i] from extra
            List<Long> result2 = new ArrayList<>(extra.size());
            RevisableOrderInvariantHash hash0 = new RevisableOrderInvariantHash();

            for (Long x : arr0) {
                hash0.add(x);
            }

            long expected = hash0.getResult();

            RevisableOrderInvariantHash hash1 = new RevisableOrderInvariantHash();
            RevisableOrderInvariantHash hash2 = new RevisableOrderInvariantHash();
            for (Long x : extra) {
                hash0.add(x);
                result0.add(hash0.getResult());
                hash1.remove(x);
                result1.add(hash1.getResult());
                hash2.add(x);
                result2.add(hash2.getResult());
            }

            for (int i = 0; i < extra.size(); i++) {
                // result0 + result1 should equals expected
                long acc0 =
                    new RevisableOrderInvariantHash().reset(result0.get(i)).add(result1.get(i)).remove(0).getResult();
                Assert.assertEquals(expected, acc0);
                // result0 - result2 should equals expected
                long acc1 =
                    new RevisableOrderInvariantHash().reset(result0.get(i)).remove(result2.get(i)).add(0).getResult();
                Assert.assertEquals(expected, acc1);
            }
        }
    }

    private Long verifyRemoval(Collection<Long> arr) {
        RevisableOrderInvariantHash hash = new RevisableOrderInvariantHash();
        for (Long x : arr) {
            Long before = hash.getResult();
            Long middle = hash.add(x).getResult();
            Assert.assertEquals(before, hash.remove(x).getResult());
            Assert.assertEquals(middle, hash.add(x).getResult());
            Assert.assertNotEquals(before, middle);
        }
        return hash.getResult();
    }
}
