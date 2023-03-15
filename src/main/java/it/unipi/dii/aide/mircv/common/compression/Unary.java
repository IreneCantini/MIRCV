package it.unipi.dii.aide.mircv.common.compression;

import java.util.ArrayList;
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

