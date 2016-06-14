package io.iohk.iodb;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Wraps byte array and provides hashCode, equals and compare methods.
 */
//NOTE: is written in Java because Scala compiler might produce slower code for hashing and compare.
public class ByteArrayWrapper implements Serializable, Comparable<ByteArrayWrapper>{

    protected final byte[] data;

    public ByteArrayWrapper(byte[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ByteArrayWrapper that = (ByteArrayWrapper) o;

        return Arrays.equals(data, that.data);

    }

    @Override
    public int hashCode() {
        //do not use Arrays.hashCode, it generates too many collisions (31 is too low)
        int h = 1;
        for(byte b: data){
            h = h * (-1640531527)  + b;
        }
        return h;
    }

    @Override
    public int compareTo(ByteArrayWrapper o) {
        return ByteArrayComparator.INSTANCE.compare(this.data, o.data);
    }

    public byte[] getData(){
        return data;
    }
}
