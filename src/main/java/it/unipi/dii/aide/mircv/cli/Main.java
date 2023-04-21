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
        int k=-1;
        boolean goon=false;
        
        while(true) {
            System.out.println("\nWrite Query or 'exit' command to terminate: ");
            Scanner sc=new Scanner(System.in);
            query = sc.nextLine();

            if(query==null || query.isEmpty()){
                System.out.println("Error: Insert a correct query.");
                continue;
            }

            if(query.equals("exit"))
                break;

            do{
                System.out.println("Write: 1 -> to execute conjunctive query , 2 -> to execute disjunctive query");
                sc=new Scanner(System.in);
                type = sc.nextLine();
            }while (!type.equals("1") && !type.equals("2"));

            if(type.equals("1"))
                Flags.setQueryMode(true);
            else {
                Flags.setQueryMode(false);
                do{
                    System.out.println("Write: 1 -> to execute DAAT, 2 -> to execute MaxScore  ");
                    sc=new Scanner(System.in);
                    type = sc.nextLine();
                }while (!type.equals("1") && !type.equals("2"));
            }

            Flags.setMaxScore_flag(type.equals("2"));

            do{
                System.out.println("Write the number of document you want to retrive: ");
                sc=new Scanner(System.in);
                type = sc.nextLine();
                try{
                    k = Integer.parseInt(type);
                    goon=true;
                }
                catch (NumberFormatException ex){
                    System.out.println("Error: Number not valid.");
                }
            }while (!goon);

            //flag = true to execute stemming
            //flag = false to not execute stemming
            long start = System.currentTimeMillis();
            tokens = TextPreprocesser.executeTextPreprocessing(query);

            if (tokens.size() == 0) {
                System.out.println("Query not valid!");
                continue;
            }

            QueryPreprocesser.executeQueryProcesser(tokens, k);
            long end = System.currentTimeMillis() - start;
            System.out.println("Query executed in: " + end + " ms");
            tokens.clear();
        }
    }
}
