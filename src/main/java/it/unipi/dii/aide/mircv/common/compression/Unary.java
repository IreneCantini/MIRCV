package it.unipi.dii.aide.mircv.common.compression;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

import static java.lang.Math.log;
public class Unary {
    // converte da un intero alla sua rappresentazione in Unary
    public static byte[] fromIntToUnary(ArrayList<Integer> valuesToCompress){
        BitSet bs = new BitSet();

        int i = 0;
        while(i<valuesToCompress.size()) {
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

        if(bs.get(bs.length()-1)) {
            // if the last bit is equal to 1 insert a byte of all 0s    for (i=0; i<8; i++){
            bs.set(bs.length() + 1, false);
        }

       /* StringBuilder s = new StringBuilder();
        for( int j = 0; j < bs.length()+1;  j++ )
        {
            s.append( bs.get( j ) == true ? 1: 0 );
        }

        System.out.println( "BitSet: " + s ); */

        if(bs.length()%8==0){
            byte[] zero= new byte[1];

            ByteBuffer bytebuffer = ByteBuffer.allocate(zero.length + bs.toByteArray().length);
            bytebuffer.put(bs.toByteArray());
            bytebuffer.put(zero);

            return bytebuffer.array();
        }

        //System.out.println(bs.toByteArray().length);
        return bs.toByteArray();
    }

    // converte un array di byte contenente numeri rappresentati in unary in interi
    public static ArrayList<Integer> fromUnaryToInt(byte[] byteA) {
        BitSet b = BitSet.valueOf(byteA);
        ArrayList<Integer> arrayInt=new ArrayList<>();

        int i_clear;
        int i_set=0;

        while(i_set>=0){
            i_clear=b.nextClearBit(i_set);
            arrayInt.add(i_clear-i_set);
            i_set=b.nextSetBit(i_clear);
        }

        return arrayInt;
    }
}

