package it.unipi.dii.aide.mircv.common.compression;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;

public class Unary {
    /**
     * This function converts an integer in his own Unary representation
     * @param valuesToCompress contains the numbers to convert
     * @return the byte-array of the Unary representation
     */
    public static byte[] fromIntToUnary(ArrayList<Integer> valuesToCompress){
        BitSet bs = new BitSet();

        int i = 0;
        while (i < valuesToCompress.size()) {
            int n = valuesToCompress.get(i);

            if (i == 0){
                bs.set(0, n);
                bs.set(n+1, false);
            }else {
                bs.set(bs.length() + 1, bs.length() + 1 + n);
                bs.set(bs.length() + 1, false);
            }

            i++;
        }

        /* If the last bit is equal to 1 insert a byte of all 0s */
        if (bs.get(bs.length() - 1))
            bs.set(bs.length() + 1, false);

        if (bs.length() % 8 == 0){

            byte[] zero= new byte[1];

            ByteBuffer bytebuffer = ByteBuffer.allocate(zero.length + bs.toByteArray().length);
            bytebuffer.put(bs.toByteArray());
            bytebuffer.put(zero);

            return bytebuffer.array();
        }

        return bs.toByteArray();
    }

    /**
     * This function convert a Unary byte representation to his num,ber equivalent
     * @param byteA the Unary representatin to convert
     * @return the integer converted
     */
    public static ArrayList<Integer> fromUnaryToInt(byte[] byteA) {

        BitSet b = BitSet.valueOf(byteA);
        ArrayList<Integer> arrayInt = new ArrayList<>();

        int i_clear;
        int i_set = 0;

        while (i_set >= 0) {
            i_clear = b.nextClearBit(i_set);
            arrayInt.add(i_clear - i_set);
            i_set = b.nextSetBit(i_clear);
        }

        return arrayInt;
    }
}

