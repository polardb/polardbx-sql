drop java function if exists mymurmurhash64var|
CREATE JAVA FUNCTION mymurmurhash64var
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(255)
CODE
  public class Mymurmurhash64var extends UserDefinedJavaFunction {
    public Object compute(Object[] args) {
        String str = (String) args[0];
        return doMurmurHash(str);
    }

    public long doMurmurHash(String str) {
        return zeroSeedMurmurHash3Hash64(str.getBytes());
    }

    public long zeroSeedMurmurHash3Hash64(byte[] data) {
        return hash64(data, 0);
    }

    public long hash64(byte[] data, int seed) {
        int len = data.length;
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        long h = (seed & 0xffffffffl) ^ (len * m);
        int i = 0;
        while (len >= 8) {
            long k = ((long) data[i] & 0xff) + (((long) data[i + 1] & 0xff) << 8) + (((long) data[i + 2] & 0xff) << 16) + (((long) data[i + 3] & 0xff) << 24) + (((long) data[i + 4] & 0xff) << 32) + (((long) data[i + 5] & 0xff) << 40) + (((long) data[i + 6] & 0xff) << 48) + (((long) data[i + 7] & 0xff) << 56);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;
            i += 8;
            len -= 8;
        }
        switch (len) {
        case 7:
            h ^= (long) (data[i + 6] & 0xff) << 48;
        case 6:
            h ^= (long) (data[i + 5] & 0xff) << 40;
        case 5:
            h ^= (long) (data[i + 4] & 0xff) << 32;
        case 4:
            h ^= (long) (data[i + 3] & 0xff) << 24;
        case 3:
            h ^= (long) (data[i + 2] & 0xff) << 16;
        case 2:
            h ^= (long) (data[i + 1] & 0xff) << 8;
        case 1:
            h ^= (long) (data[i] & 0xff);
            h *= m;
        }
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;
    }
};
END_CODE|


drop java function if exists mymurmurhash64bint|
CREATE JAVA FUNCTION mymurmurhash64bint
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE
  public class Mymurmurhash64bint extends UserDefinedJavaFunction {
    public Object compute(Object[] args) {
        Long val = (Long) args[0];
        return doMurmurHash(val+"");
    }

    public long doMurmurHash(String str) {
        return zeroSeedMurmurHash3Hash64(str.getBytes());
    }

    public long zeroSeedMurmurHash3Hash64(byte[] data) {
        return hash64(data, 0);
    }

    public long hash64(byte[] data, int seed) {
        int len = data.length;
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        long h = (seed & 0xffffffffl) ^ (len * m);
        int i = 0;
        while (len >= 8) {
            long k = ((long) data[i] & 0xff) + (((long) data[i + 1] & 0xff) << 8) + (((long) data[i + 2] & 0xff) << 16) + (((long) data[i + 3] & 0xff) << 24) + (((long) data[i + 4] & 0xff) << 32) + (((long) data[i + 5] & 0xff) << 40) + (((long) data[i + 6] & 0xff) << 48) + (((long) data[i + 7] & 0xff) << 56);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;
            i += 8;
            len -= 8;
        }
        switch (len) {
        case 7:
            h ^= (long) (data[i + 6] & 0xff) << 48;
        case 6:
            h ^= (long) (data[i + 5] & 0xff) << 40;
        case 5:
            h ^= (long) (data[i + 4] & 0xff) << 32;
        case 4:
            h ^= (long) (data[i + 3] & 0xff) << 24;
        case 3:
            h ^= (long) (data[i + 2] & 0xff) << 16;
        case 2:
            h ^= (long) (data[i + 1] & 0xff) << 8;
        case 1:
            h ^= (long) (data[i] & 0xff);
            h *= m;
        }
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;
    }
};
END_CODE|
