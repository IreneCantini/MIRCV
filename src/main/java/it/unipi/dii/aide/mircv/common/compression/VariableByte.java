package it.unipi.dii.aide.mircv.common.compression;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static java.lang.Math.log;

public class VariableByte {

    /**
     * This function converts an integer in his own Variable-Byte representation
     * @param docId the posting's docID
     * @return the Variable-Byte representation
     */
    public static byte[] fromLongToVariableByte(long docId){

        if (docId == 0)
            return new byte[]{0};

        int i = (int) (log(docId) / log(128)) + 1;
        byte[] rv = new byte[i];
        int j = i - 1;

        do {
            rv[j--] = (byte) (docId % 128);
            docId /= 128;
        } while (j >= 0);

        rv[i - 1] += 128;

        return rv;
    }

    /**
     * Converts an array of Long to them representation of Variable-Byte
     * @param numbers arrayList of docID to convert
     * @return an array of Variable-Byte
     */
    public static byte[] fromArrayLongToVariableByte(ArrayList<Long> numbers) {

        ByteBuffer buf = ByteBuffer.allocate(numbers.size() * (Long.SIZE / Byte.SIZE));

        for (Long number : numbers)
            buf.put(fromLongToVariableByte(number));

        buf.flip();
        byte[] rv = new byte[buf.limit()];
        buf.get(rv);

        return rv;
    }

    /**
     * Taken an array of represented bytes the variable byte encoding of long
     * return an array containing the corresponding longs
     * @param byteStream the array of byte to convert
     * @return an arrayList of Long numbers converted
     */
    public static ArrayList<Long> fromVariableByteToLong(byte[] byteStream) {

        ArrayList<Long> numbers = new ArrayList<>();
        long n = 0;

        for (byte b : byteStream) {
            if ((b & 0xff) < 128) {
                n = 128 * n + b;
            } else {
                long num = (128 * n + ((b - 128) & 0xff));
                numbers.add(num);
                n = 0;
            }
        }

        return numbers;
    }
}

