package it.unipi.dii.aide.mircv.common.compression;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static java.lang.Math.log;

public class VariableByte {
    // converte un long nella sua codifica variable byte
    public static byte[] fromLongToVariableByte(long n){
        if (n == 0) {
            return new byte[]{0};
        }
        int i = (int) (log(n) / log(128)) + 1;
        byte[] rv = new byte[i];
        int j = i - 1;
        do {
            rv[j--] = (byte) (n % 128);
            n /= 128;
        } while (j >= 0);
        rv[i - 1] += 128;
        return rv;
    }

    public byte[] fromIntToVariableByte(int n){
        if (n == 0) {
            return new byte[]{0};
        }
        int i = (int) (log(n) / log(128)) + 1;
        byte[] rv = new byte[i];
        int j = i - 1;
        do {
            rv[j--] = (byte) (n % 128);
            n /= 128;
        } while (j >= 0);
        rv[i - 1] += 128;
        return rv;
    }

    // preso una lista di variabili long restituisce un array contenente la loro rappesentazione in codifica variable byte
    public static byte[] fromArrayLongToVariableByte(ArrayList<Long> numbers) {
        ByteBuffer buf = ByteBuffer.allocate(numbers.size() * (Long.SIZE / Byte.SIZE));
        for (Long number : numbers) {
            buf.put(fromLongToVariableByte(number));
        }
        buf.flip();
        byte[] rv = new byte[buf.limit()];
        buf.get(rv);
        return rv;
    }

    // preso un array di byte rappresentati la codifica variable byte di long restituisce un array contenente i long corrispondenti
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
