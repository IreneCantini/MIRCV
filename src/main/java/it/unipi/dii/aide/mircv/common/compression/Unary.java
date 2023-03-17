package it.unipi.dii.aide.mircv.common.compression;

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

    //Unary compression of tfs
    public static byte[] compressListOfTFs(ArrayList<Integer> array){
        // use unary compression
        int numOfBitsNecessary = 0;
        for (int tf : array) { // Here we are looking for the number of bytes we will need for our compressed numbers
            int numOfByteNecessary = (int) (Math.floor(tf/8) + 1); // qui si può anche fare tutto con una variabile sola
            numOfBitsNecessary += (numOfByteNecessary * 8); // però diventa illeggibile quindi facciamolo alla fine
        }
        boolean[] result = new boolean[numOfBitsNecessary];
        int j = 0;
        for(int tf : array){
            long zerosToAdd = 8 - (tf % 8); //number of zeros to be added to allign to byte each TF
            for(int i=0; i<tf -1; i++){
                result[j] = true;
                j++;
            }
            // add zeros to allign to byte+
            for (int i=0; i<zerosToAdd; i++){
                j++; // instead of adding zeros we skip positions which are inizialized to false
            }
            j++;
        }
        System.out.println(Arrays.toString(result));
        return fromBooleanArrToByteArr(result);
    }

    private static byte[] fromBooleanArrToByteArr(boolean[] boolArr){
        BitSet bits = new BitSet(boolArr.length);
        for (int i = 0; i < boolArr.length; i++) {
            if (boolArr[i]) {
                bits.set(i);
            }
        }
        byte[] bytes = bits.toByteArray();
        if (bytes.length * 8 >= boolArr.length) {
            System.out.println(Arrays.toString(bytes));
            return bytes;
        } else {
            return Arrays.copyOf(bytes, boolArr.length / 8 + (boolArr.length % 8 == 0 ? 0 : 1));
        }
    }

}

