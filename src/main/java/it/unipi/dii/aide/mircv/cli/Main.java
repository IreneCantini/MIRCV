package it.unipi.dii.aide.mircv.cli;

import it.unipi.dii.aide.mircv.cli.utils.UploadDataStructures;
import it.unipi.dii.aide.mircv.common.data_structures.CollectionInfo;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.text_preprocessing.TextPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;

import java.io.IOException;
import java.util.*;

public class Main {

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        /*
        //retrieve Collection info
        CollectionInfo.readCollectionInfoToDisk();

        //retrieve data structures from disk
        UploadDataStructures.readFlagsFromDisk();
        UploadDataStructures.readDocumentIndexFromDisk();
        UploadDataStructures.readDictionaryFromDisk();

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
            tokens = TextPreprocesser.executeTextPreprocessing(query);

            QueryPreprocesser.executeQueryProcesser(tokens);

        }

         */

        ArrayList<Double> val;
        HashMap<Integer, Double> prova=new HashMap<>();
        prova.put(0, 2.4);
        prova.put(1, 1.0);
        prova.put(3, 2.4);
        prova.put(4, 1.5);
        prova.put(5, 1.7);
        prova.put(6, 2.0);

        prova= (HashMap<Integer, Double>) sortByValue(prova);
        val=new ArrayList<>(prova.values());

        for (Map.Entry<Integer, Double> entry: prova.entrySet()){
            System.out.println("chiave: "+ entry.getKey() + ", valore: "+entry.getValue());
        }

        for(Double d:val){
            System.out.println(d);
        }
    }
}
