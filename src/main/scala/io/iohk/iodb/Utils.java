package io.iohk.iodb;

import net.jpountz.xxhash.*;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.*;

/**
 * Java utilities
 */
public class Utils {


    static final String shardInfoFileExt = "shardinfo";

    static final int checksumOffset = 8;
    static final int fileSizeOffset = 8 + 8;
    static final int keyCountOffset = 8 + 8 + 8;
    static final int keySizeOffset = keyCountOffset + 4;


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

    /**
     * Compares primitive Byte Arrays.
     * It uses unsigned binary comparation; so the byte with negative value is always higher than byte with non-negative value.
     */
    public static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = (o1, o2) -> compare(o1, o2);

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

    static int unsafeBinarySearch(ByteBuffer keys, byte[] key, int baseKeyOffset, int keyCount) {
        long bufAddress = ((sun.nio.ch.DirectBuffer) keys).address() + baseKeyOffset;

        long[] keyParsed = parseKey(key);
        int keySize = key.length;

        int lo = 0;
        int hi = keyCount - 1; // key count offset
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
        bufAddress = bufAddress + mid * (keySize);
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


    static void writeFully(FileChannel channel, long offset, ByteBuffer buf) throws IOException {
        int remaining = buf.limit() - buf.position();

        while (remaining > 0) {
            int written = channel.write(buf, offset);
            if (written < 0)
                throw new EOFException();
            remaining -= written;
        }
    }


    static void writeFully(FileChannel channel, ByteBuffer buf) throws IOException {
        int remaining = buf.limit() - buf.position();

        while (remaining > 0) {
            int written = channel.write(buf);
            if (written < 0)
                throw new EOFException();
            remaining -= written;
        }
    }

    static void readFully(FileChannel channel, long offset, ByteBuffer buf) throws IOException {
        int remaining = buf.limit() - buf.position();

        long csize = channel.size();
        while (remaining > 0) {
            int read = channel.read(buf, offset);
            if (read < 0)
                throw new EOFException(channel.size() + " - " + csize + " - " + offset);
            remaining -= read;
        }
    }

    public static int getInt(byte[] buf, int pos) {
        return
                (((int) buf[pos++]) << 24) |
                        (((int) buf[pos++] & 0xFF) << 16) |
                        (((int) buf[pos++] & 0xFF) << 8) |
                        (((int) buf[pos] & 0xFF));
    }

    public static void putInt(byte[] buf, int pos, int v) {
        buf[pos++] = (byte) (0xff & (v >> 24));  //TODO PERF is >>> faster here? Also invert 0xFF &?
        buf[pos++] = (byte) (0xff & (v >> 16));
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos] = (byte) (0xff & (v));
    }


    public static long getLong(byte[] buf, int pos) {
        return
                ((((long) buf[pos++]) << 56) |
                        (((long) buf[pos++] & 0xFF) << 48) |
                        (((long) buf[pos++] & 0xFF) << 40) |
                        (((long) buf[pos++] & 0xFF) << 32) |
                        (((long) buf[pos++] & 0xFF) << 24) |
                        (((long) buf[pos++] & 0xFF) << 16) |
                        (((long) buf[pos++] & 0xFF) << 8) |
                        (((long) buf[pos] & 0xFF)));

    }

    public static void putLong(byte[] buf, int pos, long v) {
        buf[pos++] = (byte) (0xff & (v >> 56));
        buf[pos++] = (byte) (0xff & (v >> 48));
        buf[pos++] = (byte) (0xff & (v >> 40));
        buf[pos++] = (byte) (0xff & (v >> 32));
        buf[pos++] = (byte) (0xff & (v >> 24));
        buf[pos++] = (byte) (0xff & (v >> 16));
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos] = (byte) (0xff & (v));
    }

    public static void putLong(byte[] buf, int pos, long v, int vSize) {
        for (int i = vSize - 1; i >= 0; i--) {
            buf[i + pos] = (byte) (0xff & v);
            v >>>= 8;
        }
    }

    private static final XXHash64 hash64 = XXHashFactory.fastestJavaInstance().hash64();

    public static long checksum(byte[] data) {
        return checksum(data, 0, data.length);
    }


    public static long checksum(byte[] data, int startOffset, int size) {
        int seed = 0x3289989d;
        return hash64.hash(data, startOffset, size, seed);
    }

    public static void fileReaderIncrement(ConcurrentMap<Long, Long> readers, Long fileNum) {
        readers.compute(fileNum, (key, value) -> value == null ? 1L : value + 1L);
    }


    public static void fileReaderDecrement(ConcurrentMap<Long, Long> readers, Long fileNum) {
        readers.compute(fileNum, (key, value) -> {
            if (value == null)
                throw new IllegalMonitorStateException("file not locked: " + fileNum);
            if (value.longValue() == 1L) {
                return null;
            } else {
                return value - 1L;
            }
        });

    }


}
