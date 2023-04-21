package it.unipi.dii.aide.mircv.query_processing.test_performance;

import it.unipi.dii.aide.mircv.cli.utils.UploadDataStructures;
import it.unipi.dii.aide.mircv.common.data_structures.CollectionInfo;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.text_preprocessing.TextPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;
import org.apache.lucene.analysis.util.FilesystemResourceLoader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TestPerformanceMain {

    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("*** Welcome! ***");

        //retrieve Collection info
        System.out.println("\nLoading Collection information ... ");
        UploadDataStructures.readCollectionInfoFromDisk();

        //retrieve data structures from disk
        System.out.println("\nLoading Flag information... ");
        UploadDataStructures.readFlagsFromDisk();
        System.out.println("\nLoading Document index... ");
        UploadDataStructures.readDocumentIndexFromDisk();
        System.out.println("\nLoading Dictionary... ");
        UploadDataStructures.readDictionaryFromDisk();

        String query;
        ArrayList<String> tokens;
        String type;
        Scanner sc;
        ArrayList<Long> timeQueries = null;
        long avgTimes = 0;

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/main/resources/test.tsv"), StandardCharsets.UTF_8));
        query = reader.readLine();

        do{
            System.out.println("Write: 1 -> to execute conjunctive query , 2 -> to execute disjunctive query");
            sc=new Scanner(System.in);
            type = sc.nextLine();
        }while (!type.equals("1") && !type.equals("2"));

        if(type.equals("1"))
            Flags.setQueryMode(true);
        else {
            do{
                System.out.println("Write: 1 -> to execute DAAT, 2 -> to execute MAxScore  ");
                sc=new Scanner(System.in);
                type = sc.nextLine();
            }while (!type.equals("1") && !type.equals("2"));
        }

        Flags.setMaxScore_flag(type.equals("2"));

        System.out.println("\n\n *** START TESTING QUERIES ***\n\n");

        timeQueries = new ArrayList<>();

        while (query != null) {

            String[] queryTokens = query.split("\t");

            System.out.println("\n*** QUERY " + queryTokens[0] + " ***\n");

            List<String> queryList = new ArrayList<>(Arrays.asList(queryTokens));
            queryList.remove(0);

            // Convert to document without queryID
            String queryWithoutDocID = String.join(",", queryList);

            long start = System.currentTimeMillis();
            tokens = TextPreprocesser.executeTextPreprocessing(queryWithoutDocID);

            QueryPreprocesser.executeQueryProcesser(tokens);
            long end = System.currentTimeMillis() - start;
            System.out.println("Query executed in: " + end + " ms");

            // Add time to array
            timeQueries.add(end);
            query = reader.readLine();
            tokens.clear();
        }

        for(Long l : timeQueries)
            avgTimes += l;

        System.out.println("\n\nThe totale time to process 200 query is: -> " + avgTimes / 1000 + " sec");

        avgTimes /= 200;

        System.out.println("\n\nThe average time to process 200 query is: -> " + avgTimes + " ms");

    }
}
