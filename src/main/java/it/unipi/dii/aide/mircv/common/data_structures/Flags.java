package it.unipi.dii.aide.mircv.common.data_structures;

import it.unipi.dii.aide.mircv.cli.utils.UploadDataStructures;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.Flags_raf;

public class Flags {

    /* Flag for checking if the Compression is enabled */
    private static boolean compression_flag;

    /* Flag for checking if Stopword Removal and Stemming are enabled */
    private static boolean filter_flag;

    /* Flag for checking if the MaxScore algorithm is enabled
       TRUE: execute MaxScore
       FALSE: not execute
    */
    private static boolean maxScore_flag;

    /* Flag for checking if the Debug Mode is enabled */
    private static boolean debug_flag;

    /* TRUE: BM25  FALSE: TFIDF */
    private static final boolean scoreMode = true;

    /* TRUE: Conjunctive Query  FALSE: Disjunctive Query */
    private static boolean queryMode;

    /**
     * Function that write the Flags into disk
     * @throws IOException if the channel is not found
     */
    public static void writeFlagToDisk() throws IOException {

        ByteBuffer FlagsBuffer = ByteBuffer.allocate(16);
        Flags_raf.getChannel().position(0);

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

        while (FlagsBuffer.hasRemaining())
            Flags_raf.getChannel().write(FlagsBuffer);
    }

    /**
     * Function that write the Flags into disk with DEBUG-MODE
     * @throws IOException if the channel is not found
     */
    public static void writeFlagDebugModeToDisk() throws IOException {

        String flag_string = "Compression flag: " + compression_flag +
                ", Filter flag: " + filter_flag +
                ", MaxScore flag: " + maxScore_flag +
                ", Debug flag: " + debug_flag;

        BufferedWriter disk_writer = new BufferedWriter(new FileWriter("src/main/resources/Debug/flags_debug.txt", true));
        disk_writer.write(flag_string);
        disk_writer.close();
    }

    /**
     * Function that print flags
     * @throws IOException if the channel is not found
     */
    public static void printFlag() throws IOException {

        UploadDataStructures.readFlagsFromDisk();

        System.out.println("Compression flag: " + compression_flag +
                            "\nFilter flag: " + filter_flag +
                            "\nMaxScore flag: " + maxScore_flag +
                            "\nDebug flag: " + debug_flag);
    }

    /* SETTER AND GETTER SECTION */

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

    public static void setQueryMode(boolean queryMode) {Flags.queryMode = queryMode;    }

    public static boolean isCompression_flag() {
        return !compression_flag;
    }

    public static boolean isScoreMode() {return scoreMode;}

    public static boolean isFilter_flag() {
        return filter_flag;
    }

    public static boolean isMaxScore_flag() {
        return maxScore_flag;
    }

    public static boolean isDebug_flag() {
        return debug_flag;
    }

    public static boolean isQueryMode() {return queryMode;}
}
