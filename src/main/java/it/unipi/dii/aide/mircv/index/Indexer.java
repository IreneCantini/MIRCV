package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.common.data_structures.*;
import java.io.IOException;
import java.util.Scanner;

public class Indexer {
    public static void main(String[] args) throws IOException, InterruptedException {

        Scanner sc;
        String type;

        /* Select if you want to construct the Inverted Index with compression or not */
        do {
            System.out.println("| SELECT ONE OF THIS POSSIBLES CHOICE:\n1: To build Inverted Index with compression\n2: To build Inverted Index without compression");
            sc = new Scanner(System.in);
            type = sc.nextLine();
        } while (!type.equals("1") && !type.equals("2"));

        Flags.setCompression_flag(type.equals("1"));

        /* Select if you want to construct the Inverted Index filtered or not */
        do {
            System.out.println("| SELECT ONE OF THIS POSSIBLES CHOICE:\n1: To build filtered Inverted Index\n2: To build unfiltered Inverted Index");
            sc = new Scanner(System.in);
            type = sc.nextLine();
        } while (!type.equals("1") && !type.equals("2"));

        Flags.setFilter_flag(type.equals("1"));

        /* Select if you want to construct the Inverted Index with DEBUG information or not */
        do {
            System.out.println("| SELECT ONE OF THIS POSSIBLES CHOICE:\n1: To build Inverted Index with DEBUG Information\n2: To build Inverted Index without DEBUG Information");
            sc = new Scanner(System.in);
            type = sc.nextLine();
        } while (!type.equals("1") && !type.equals("2"));

        Flags.setDebug_flag(type.equals("1"));

        System.out.println("\n\n*** STARTED INVERTED INDEX CONSTRUCTION OF " +
                ((!Flags.isCompression_flag()) ? "COMPRESSED, " : "UNCOMPRESSED, ") +
                ((Flags.isFilter_flag()) ? "FILTERED " : "UNFILTERED ") +
                ((Flags.isDebug_flag()) ? "WITH DEBUG INFORMATIONS" : "WITHOUT DEBUG INFORMATIONS ***"));

        long start = System.currentTimeMillis();
        SPIMI.executeSPIMI("src/main/resources/collection.tsv");
        System.out.println("\n*** INVERTED INDEX CONSTRUCTION COMPLETED ***\n");

        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;

        System.out.println("(INFO) Inverted Index built in: " + time + " minutes");

        time = (SPIMI.timeTextPreprocessing/1000)/60;
        System.out.println("(INFO) Total time to execute text preprocesser: " + time + " minutes");
    }
}
