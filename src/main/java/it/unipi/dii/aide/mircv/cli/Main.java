package it.unipi.dii.aide.mircv.cli;

import it.unipi.dii.aide.mircv.cli.utils.UploadDataStructures;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.text_preprocessing.TextPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("*** ACADEMIC SEARCH ENGINE ***");

        /* Retrieve Collection info */
        System.out.println("\nLoading Collection information... ");
        UploadDataStructures.readCollectionInfoFromDisk();

        /* Retrieve Flags from disk */
        System.out.println("\nLoading Flag information... ");
        UploadDataStructures.readFlagsFromDisk();

        /* Retrieve Document Index */
        System.out.println("\nLoading Document index... ");
        UploadDataStructures.readDocumentIndexFromDisk();

        /* Retrieve Dictionary */
        System.out.println("\nLoading Dictionary... ");
        UploadDataStructures.readDictionaryFromDisk();

        /* Retrieve Stopwords list */
        TextPreprocesser.stopwords_global= Files.readAllLines(Paths.get("src/main/resources/stopwords.txt"));

        /* Query inserted by the user */
        String query;

        /* Tokens that compose the query */
        ArrayList<String> tokens;

        /* Type of the query requested by the user */
        String type;
        int k = -1;
        boolean goOn = false;
        
        while (true) {

            /* Insert the query */
            System.out.println("\nWrite Query or 'exit' command to terminate: ");
            Scanner sc = new Scanner(System.in);
            query = sc.nextLine();

            /* If the query is null or empty, returns error and go on */
            if (query == null || query.isEmpty()){
                System.out.println("(ERROR) Insert a correct query.");
                continue;
            }

            /* If the query's text is equals to 'exit' terminate the program */
            if (query.equals("exit"))
                break;

            /* Select from Conjunctive Queries and Disjunctive Queries */
            do {
                System.out.println("SELECT ONE OF THIS POSSIBLES CHOICE:\n1: To execute Conjunctive Query\n2: To execute Disjunctive Query");
                sc = new Scanner(System.in);
                type = sc.nextLine();
            } while (!type.equals("1") && !type.equals("2"));

            if (type.equals("1"))
                Flags.setQueryMode(true);
            else {
                Flags.setQueryMode(false);

                do {
                    System.out.println("SELECT ONE OF THIS POSSIBLES CHOICE:\n1: To execute DAAT\n2: To execute MaxScore");
                    sc=new Scanner(System.in);
                    type = sc.nextLine();
                } while (!type.equals("1") && !type.equals("2"));
            }

            Flags.setMaxScore_flag(type.equals("2"));

            do {
                System.out.println("Write the number of document you want to retrieve: ");
                sc = new Scanner(System.in);
                type = sc.nextLine();

                try {
                    k = Integer.parseInt(type);
                    goOn = true;
                } catch (NumberFormatException ex){
                    System.out.println("(ERROR) Number not valid.");
                }
            } while (!goOn);

            /* Pre-process the query */
            tokens = TextPreprocesser.executeTextPreprocessing(query);

            if (tokens.size() == 0) {
                System.out.println("(ERROR) Query not valid!");
                continue;
            }

            long start = System.currentTimeMillis();
            QueryPreprocesser.executeQueryProcesser(tokens, k);
            long end = System.currentTimeMillis() - start;
            System.out.println("\n(INFO) Query executed in: " + end + " ms");
            tokens.clear();
        }
    }
}
