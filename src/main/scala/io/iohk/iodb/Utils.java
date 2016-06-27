package io.iohk.iodb;

import java.util.Comparator;

/**
 * Java utilities
 */
class Utils {

    public static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            if (o1 == o2) return 0;
            final int len = Math.min(o1.length, o2.length);
            for (int i = 0; i < len; i++) {
                int b1 = o1[i] & 0xFF;
                int b2 = o2[i] & 0xFF;
                if (b1 != b2)
                    return b1 - b2;
            }
            return o1.length - o2.length;
        }
    };

    public static int byteArrayHashCode(byte[] data) {
        //do not use Arrays.hashCode, it generates too many collisions (31 is too low)
        int h = 1;
        for (byte b : data) {
            h = h * (-1640531527) + b;
        }
        return h;
    }
}
