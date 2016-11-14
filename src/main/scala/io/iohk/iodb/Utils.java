package io.iohk.iodb;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Java utilities
 */
class Utils {


    static final String shardInfoFileExt = "shardinfo";

    static final int checksumOffset = 8 + 8;
    static final int fileSizeOffset = 8 + 8;
    static final int keyCountOffset = 8 + 8 + 8;
    static final int keySizeOffset = keyCountOffset + 4;

    static final long baseKeyOffset = 8 + 8 + 8 + 4 + 4;
    static final long baseValueOffset = 8 + 8 + 8;


    static final Logger LOG = Logger.getLogger(Utils.class.getName());

    static final sun.misc.Unsafe UNSAFE = getUnsafe();

    @SuppressWarnings("restriction")
    private static sun.misc.Unsafe getUnsafe() {
        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
            LOG.log(Level.WARNING, "This is not Little Endian platform. Unsafe optimizations are disabled.");
            return null;
        }
        try {
            java.lang.reflect.Field singleoneInstanceField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            singleoneInstanceField.setAccessible(true);
            sun.misc.Unsafe ret = (sun.misc.Unsafe) singleoneInstanceField.get(null);
            return ret;
        } catch (Throwable e) {
            LOG.log(Level.WARNING, "Could not instantiate sun.misc.Unsafe. Fall back to DirectByteBuffer and other alternatives.", e);
            return null;
        }
    }
    public static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            return Utils.compare(o1, o2);
        }
    };

    public static int compare(byte[] o1, byte[] o2) {
//            if (o1 == o2) return 0;
        final int len = Math.min(o1.length, o2.length);
        for (int i = 0; i < len; i++) {
            int b1 = o1[i] & 0xFF;
            int b2 = o2[i] & 0xFF;
            if (b1 != b2)
                return b1 - b2;
        }
        return o1.length - o2.length;
    }


    public static int byteArrayHashCode(byte[] data) {
        //do not use Arrays.hashCode, it generates too many collisions (31 is too low)
        int h = 1;
        for (byte b : data) {
            h = h * (-1640531527) + b;
        }
        return h;
    }

    /**  returns byte array, which is greater than any other array of given size */
    public static byte[] greatest(int size){
        byte[] ret = new byte[size];
        for(int i=0;i<size;i++){
            ret[i] = (byte) 0xFF;
        }
        return ret;
    }

    private static long getLong7(byte[] buf, int pos) {
        return
                ((long) (buf[pos++] & 0xff) << 48) |
                        ((long) (buf[pos++] & 0xff) << 40) |
                        ((long) (buf[pos++] & 0xff) << 32) |
                        ((long) (buf[pos++] & 0xff) << 24) |
                        ((long) (buf[pos++] & 0xff) << 16) |
                        ((long) (buf[pos++] & 0xff) << 8) |
                        ((long) (buf[pos] & 0xff));
    }

    private static long[] parseKey(byte[] bb) {
        int size = (int) Math.ceil(1.0 * bb.length / 7);
        long[] r = new long[size];
        for (int i = 0; i < size; i++) {
            int offset = Math.min(bb.length - 7, i * 7);
            r[i] = getLong7(bb, offset);
        }
        return r;
    }

    static boolean unsafeSupported() {
        return UNSAFE != null;
    }


    private static long unsafeGetLong(long address) {
        long l = UNSAFE.getLong(address);
        return Long.reverseBytes(l);
    }

    static int unsafeBinarySearch(ByteBuffer keys, byte[] key) {
        long bufAddress = ((sun.nio.ch.DirectBuffer) keys).address() + baseKeyOffset;

        long[] keyParsed = parseKey(key);
        int keySize = key.length;

        int lo = 0;
        int hi = keys.getInt(keyCountOffset); // key count offset
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            int comp = unsafeCompare(bufAddress, mid, keySize, keyParsed);
            if (comp < 0)
                lo = mid + 1;
            else if (comp > 0)
                hi = mid - 1;
            else
                return mid;
        }
        return -1;
    }

    private static int unsafeCompare(long bufAddress, int mid, int keySize, long[] keyParsed) {
        bufAddress = bufAddress + mid * (keySize + 4 + 8);
        int offset = -1;
        for (long keyPart : keyParsed) {
            long v = unsafeGetLong(bufAddress + offset) & 0xFFFFFFFFFFFFFFL;
            if (v < keyPart)
                return -1;
            else if (v > keyPart)
                return 1;
            offset = Math.min(keySize - 8, offset + 7); //TODO this does not work with small keys
        }
        return 0;
    }


    /**
     * Hack to unmap MappedByteBuffer.
     * Unmap is necessary on Windows, otherwise file is locked until JVM exits or BB is GCed.
     * There is no public JVM API to unmap buffer, so this tries to use SUN proprietary API for unmap.
     * Any error is silently ignored (for example SUN API does not exist on Android).
     */
    protected static boolean unmap(ByteBuffer b) {
        if (!(b instanceof DirectBuffer))
            return false;

        // need to dispose old direct buffer, see bug
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
        DirectBuffer bb = (DirectBuffer) b;
        Cleaner c = bb.cleaner();
        if (c != null) {
            c.clean();
            return true;
        }
        Object attachment = bb.attachment();
        return attachment != null && attachment instanceof DirectBuffer && unmap(b);

    }

    protected static File[] listFiles(File dir, String extension) {
        return dir.listFiles(pathname -> pathname.isFile() && pathname.getName().endsWith("." + extension));
    }
}
