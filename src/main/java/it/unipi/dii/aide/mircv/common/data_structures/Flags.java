package it.unipi.dii.aide.mircv.common.data_structures;

import it.unipi.dii.aide.mircv.common.file_management.FileUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.Flags_raf;
import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.PATH_TO_FLAGS_FILE;

public class Flags {
    //flag for checking if the compression is enabled
    private static boolean compression_flag;

    //flag for checking if stopwords removal and stemming are enabled
    private static boolean filter_flag;

    //flag for checking if the max score algorithm is enabled
    private static boolean maxScore_flag;

    //flag for checking if the debug mode is enabled
    private static boolean debug_flag;

    public static void setCompression_flag(boolean compression_flag) {
        Flags.compression_flag = compression_flag;
    }

    public static void setFilter_flag(boolean filter_flag) {
        Flags.filter_flag = filter_flag;
    }

    public static void setMaxScore_flag(boolean maxScore_flag) {
        Flags.maxScore_flag = maxScore_flag;
    }

    public static void setDebug_flag(boolean debug_flag) {
        Flags.debug_flag = debug_flag;
    }

    public static boolean isCompression_flag() {
        return !compression_flag;
    }

    public static boolean isFilter_flag() {
        return filter_flag;
    }

    public static boolean isMaxScore_flag() {
        return maxScore_flag;
    }

    public static boolean isDebug_flag() {
        return debug_flag;
    }

    public static void writeFlagToDisk() throws IOException {
        ByteBuffer FlagsBuffer = ByteBuffer.allocate(16);
        Flags_raf.getChannel().position(0);


        //write the Flags into file
        if(compression_flag)
            FlagsBuffer.putInt(1);
        else
            FlagsBuffer.putInt(0);

        if(filter_flag)
            FlagsBuffer.putInt(1);
        else
            FlagsBuffer.putInt(0);

        if(maxScore_flag)
            FlagsBuffer.putInt(1);
        else
            FlagsBuffer.putInt(0);

        if(debug_flag)
            FlagsBuffer.putInt(1);
        else
            FlagsBuffer.putInt(0);


        FlagsBuffer = ByteBuffer.wrap(FlagsBuffer.array());

        while(FlagsBuffer.hasRemaining()) {
            Flags_raf.getChannel().write(FlagsBuffer);
        }
    }

    public static void writeFlagDebugModeToDisk() throws IOException {
        String flag_string = "Compression flag: " + compression_flag +
                ", Filter flag: " + filter_flag +
                ", MaxScore flag: " + maxScore_flag +
                ", Debug flag: " + debug_flag;

        BufferedWriter disk_writer = new BufferedWriter(new FileWriter("src/main/resources/Debug/flags_debug.txt", true));
        disk_writer.write(flag_string);
        disk_writer.close();
    }


    public static void readFlagsFromDisk() throws IOException {
        //retrieve file channel
        Flags_raf = new RandomAccessFile(PATH_TO_FLAGS_FILE, "r");

        ByteBuffer FlagsBuffer = ByteBuffer.allocate(16);

        Flags_raf.getChannel().position(0);


        while (FlagsBuffer.hasRemaining()){
            Flags_raf.getChannel().read(FlagsBuffer);
        }

        FlagsBuffer.rewind();

        compression_flag = FlagsBuffer.getInt() == 1;

        filter_flag = FlagsBuffer.getInt() == 1;

        maxScore_flag = FlagsBuffer.getInt() == 1;

        debug_flag = FlagsBuffer.getInt() == 1;
    }

    public static void printFlag() throws IOException {

        readFlagsFromDisk();

        System.out.println("Compression flag: " + compression_flag +
                            "\nFilter flag: " + filter_flag +
                            "\nMaxScore flag: " + maxScore_flag +
                            "\nDebug flag: " + debug_flag);
    }
}
