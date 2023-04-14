package it.unipi.dii.aide.mircv.cli;

import it.unipi.dii.aide.mircv.cli.utils.UploadDataStructures;
import it.unipi.dii.aide.mircv.common.data_structures.CollectionInfo;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.text_preprocessing.TextPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {


        //retrieve Collection info
        CollectionInfo.readCollectionInfoToDisk();

        //retrieve data structures from disk
        UploadDataStructures.readFlagsFromDisk();
        UploadDataStructures.readDocumentIndexFromDisk();
        UploadDataStructures.readDictionaryFromDisk();

        System.out.println("Last element: " + UploadDataStructures.Dictionary.get("project"));

        System.out.println("Write Query or 'exit' command to terminate: ");
        Scanner sc=new Scanner(System.in);
        String query;
        ArrayList<String> tokens;
        
        while(true) {
            query = sc.nextLine();

            if(query==null || query.isEmpty()){
                System.out.println("Error: Insert a correct query.");
                continue;
            }

            if(query.equals("exit"))
                break;

            //flag = true to execute stemming
            //flag = false to not execute stemming
            long start = System.currentTimeMillis();
            tokens = TextPreprocesser.executeTextPreprocessing(query);

            QueryPreprocesser.executeQueryProcesser(tokens);
            long end = System.currentTimeMillis() - start;
            System.out.println("Query executed in: " + end + " ms");
        }
    }
}
