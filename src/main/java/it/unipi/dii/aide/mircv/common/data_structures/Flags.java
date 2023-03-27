package it.unipi.dii.aide.mircv.common.data_structures;

public class Flags {
    //flag for checking if the compression is enabled
    public static boolean compression_flag;

    //flag for checking if stopwords removal and stemming are enabled
    public static boolean filter_flag;

    //flag for checking if the max score algorithm is enabled
    public static boolean maxScore_flag;

    public static void setCompression_flag(boolean compression_flag) {
        Flags.compression_flag = compression_flag;
    }

    public static void setFilter_flag(boolean filter_flag) {
        Flags.filter_flag = filter_flag;
    }

    public static void setMaxScore_flag(boolean maxScore_flag) {
        Flags.maxScore_flag = maxScore_flag;
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
}
