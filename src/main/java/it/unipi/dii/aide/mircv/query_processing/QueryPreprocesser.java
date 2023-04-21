package it.unipi.dii.aide.mircv.query_processing;

import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.query_processing.Algorithms.ConjunctiveQuery;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;

import java.io.IOException;
import java.util.*;

import static it.unipi.dii.aide.mircv.query_processing.Algorithms.DAAT.*;
import static it.unipi.dii.aide.mircv.query_processing.Algorithms.MaxScore.*;



public class QueryPreprocesser {

    public static ArrayList<PostingList> plQueryTerm = new ArrayList<>();
    public static ArrayList<PostingList> orderedPlQueryTerm = new ArrayList<>();
    public static HashMap<Integer, Double> hm_PosScore = new HashMap<>();
    public static ArrayList<Double> orderedMaxScore;
    // variable for conjunctive query
    public static HashMap<Integer, Integer> hm_PosLen = new HashMap<>();

    // ordinamento ascendente
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    /**
     * Per provare il MaxScore mettere ! agli if segnati con TODO
     * @param tokens
     * @throws IOException
     * @throws InterruptedException
     */

    public static void executeQueryProcesser(ArrayList<String> tokens, int k) throws IOException, InterruptedException {

        int pos=0;
        for(String t: tokens){
            PostingList pl=new PostingList();
            pl.getPl().clear();
            pl.setTerm(t);

            pl.obtainPostingList(t);

            if(pl.getPl().size() == 0 || pl.getPl()==null){
                // the term is not present in the dictionary
                continue;
            }

            plQueryTerm.add(pl);

            if (Flags.isMaxScore_flag()) {
                if (Flags.isScoreMode())
                    hm_PosScore.put(pos, pl.getMaxBM25());
                else
                    hm_PosScore.put(pos, pl.getMaxTFIDF());
            }

            hm_PosLen.put(pos, pl.getPl().size());

            pos++;
        }

        PriorityQueue<DocumentScore> pQueueResult;

        if(plQueryTerm == null || plQueryTerm.size()==0){
            System.out.println("All the query words are not present in the dictionary");
            return;
        }

        if(Flags.isQueryMode()){
            pQueueResult = ConjunctiveQuery.executeConjunctiveQuery(k);
            System.out.println("Conjunctive query results: ");
        }else {
            if (Flags.isMaxScore_flag()) {
                // ordinamento posting list in base allo score
                hm_PosScore = (HashMap<Integer, Double>) sortByValue(hm_PosScore);

                for (Map.Entry<Integer, Double> entry : hm_PosScore.entrySet()) {
                    orderedPlQueryTerm.add(plQueryTerm.get(entry.getKey()));
                }

                orderedMaxScore = new ArrayList<>(hm_PosScore.values());

                pQueueResult = executeMaxScore(k);
                System.out.println("MaxScore results: ");

            } else {
                pQueueResult = executeDAAT(k);
                System.out.println("DAAT results: ");
            }
        }

        int rank=1;
        System.out.println("size queue: "+ pQueueResult.size());
        while (pQueueResult.size()!=0 && rank!=k+1) {
            DocumentScore d = pQueueResult.poll();
            System.out.println(rank + ": " + d.getDocid() + ", score: "+d.getScore());
            rank++;
        }

        if(orderedMaxScore!=null)
            orderedMaxScore.clear();

        if(orderedPlQueryTerm!=null)
            orderedPlQueryTerm.clear();

        if(plQueryTerm!=null)
            plQueryTerm.clear();

        if(hm_PosScore!=null)
            hm_PosScore.clear();

        if(hm_PosLen!=null)
            hm_PosLen.clear();

        pQueueResult.clear();
    }
}
