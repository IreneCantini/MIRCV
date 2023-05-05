package it.unipi.dii.aide.mircv.query_processing.test_performance;

import it.unipi.dii.aide.mircv.cli.utils.UploadDataStructures;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.text_preprocessing.TextPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TestPerformanceMain {

    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("*** Welcome! ***");

        /* Retrieve Collection info */
        System.out.println("\nLoading Collection information ... ");
        UploadDataStructures.readCollectionInfoFromDisk();

        /* Retrieve Flags from disk */
        System.out.println("\nLoading Flag information... ");
        UploadDataStructures.readFlagsFromDisk();

        /* Retrieve Document Index from disk */
        System.out.println("\nLoading Document index... ");
        UploadDataStructures.readDocumentIndexFromDisk();

        /* Retrieve Dictionary from disk */
        System.out.println("\nLoading Dictionary... ");
        UploadDataStructures.readDictionaryFromDisk();

        while (true) {
            String query;
            ArrayList<String> tokens;
            String type;
            Scanner sc;
            ArrayList<Long> timeQueries;
            long avgTimes = 0;

            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/main/resources/test.tsv"), StandardCharsets.UTF_8));
            query = reader.readLine();

            do {
                System.out.println("\nSELECT ONE OF THIS POSSIBLES CHOICE:\n1 -> To execute Conjunctive Query\n2 -> To execute Disjunctive Query\n3 -> to exit");
                sc = new Scanner(System.in);
                type = sc.nextLine();
            } while (!type.equals("1") && !type.equals("2") && !type.equals("3"));

            if (type.equals("3"))
                break;

            if (type.equals("1"))
                Flags.setQueryMode(true);
            else {

                do {
                    System.out.println("\nSELECT ONE OF THIS POSSIBLES CHOICE:\n1 -> To execute DAAT algorithm\n2 -> To execute MaxScore algorithm");
                    sc = new Scanner(System.in);
                    type = sc.nextLine();
                } while (!type.equals("1") && !type.equals("2"));
            }

            Flags.setMaxScore_flag(type.equals("2"));

            do {
                System.out.println("\nSELECT ONE OF THIS POSSIBLES CHOICE:\n1 -> To use TFIDF\n2 -> To use BM25");
                sc = new Scanner(System.in);
                type = sc.nextLine();
            } while (!type.equals("1") && !type.equals("2"));

            Flags.setScoreMode(type.equals("2"));

            System.out.println("\n\n *** START TESTING QUERIES ***\n\n");

            timeQueries = new ArrayList<>();

            while (query != null) {

                String[] queryTokens = query.split("\t");

                System.out.println("\n*** QUERY " + queryTokens[0] + " ***\n");

                List<String> queryList = new ArrayList<>(Arrays.asList(queryTokens));
                queryList.remove(0);

                /* Convert to document without queryID */
                String queryWithoutDocID = String.join(",", queryList);

                long start = System.currentTimeMillis();
                tokens = TextPreprocesser.executeTextPreprocessing(queryWithoutDocID);

                QueryPreprocesser.executeQueryProcesser(tokens, 5);
                long end = System.currentTimeMillis() - start;
                System.out.println("Query executed in: " + end + " ms");

                /* Add time to array */
                timeQueries.add(end);
                query = reader.readLine();
                tokens.clear();
            }

            for (Long l : timeQueries)
                avgTimes += l;

            System.out.println("\n\n(INFO) The total time to process 200 query is: -> " + avgTimes / 1000 + " sec");

            avgTimes /= 200;

            System.out.println("The average time to process 200 query is: -> " + avgTimes + " ms");
        }
    }
}
