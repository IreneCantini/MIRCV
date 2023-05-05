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
    public static HashMap<Integer, Integer> hm_PosLen = new HashMap<>();

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {

        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());
        Map<K, V> result = new LinkedHashMap<>();

        for (Map.Entry<K, V> entry : list)
            result.put(entry.getKey(), entry.getValue());

        return result;
    }

    /**
     * Function that allows to execute the processing of the query
     * @param tokens the tokens of the query
     * @throws IOException if the channel is not found
     */

    public static void executeQueryProcesser(ArrayList<String> tokens, int k) throws IOException{

        int pos = 0;

        for (String t: tokens) {

            PostingList pl = new PostingList();
            pl.getPl().clear();
            pl.setTerm(t);

            pl.obtainPostingList(t);

            if (pl.getPl().size() == 0 || pl.getPl()==null){
                /* The term is not present in the dictionary */
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

        if(plQueryTerm == null || plQueryTerm.size() == 0){
            System.out.println("(INFO) All the query words are not present in the Dictionary");
            return;
        }

        if (Flags.isQueryMode()){

            pQueueResult = ConjunctiveQuery.executeConjunctiveQuery(k);
        }else {
            if (Flags.isMaxScore_flag()) {

                /* Sorting posting list by score */
                hm_PosScore = (HashMap<Integer, Double>) sortByValue(hm_PosScore);

                for (Map.Entry<Integer, Double> entry : hm_PosScore.entrySet())
                    orderedPlQueryTerm.add(plQueryTerm.get(entry.getKey()));

                orderedMaxScore = new ArrayList<>(hm_PosScore.values());

                pQueueResult = executeMaxScore(k);
            }else {
                pQueueResult = executeDAAT(k);
            }
        }

        int rank = 1;

        System.out.println("\n*** TOP " + k + " DOCUMENTS RETRIEVED ***\n");

        String leftAlignFormat = "\t| %-15d | %-4s |%n";

        System.out.format("\t+-----------------+-------+%n");
        System.out.format("\t|  Document       | Score |%n");
        System.out.format("\t+-----------------+-------+%n");

        while (pQueueResult.size() != 0 && rank != (k + 1)) {
            DocumentScore d = pQueueResult.poll();
            if (d.getScore() != 0)
                System.out.format(leftAlignFormat, d.getDocid(), String.format("%.3f", d.getScore()));
            rank++;
        }
        System.out.format("\t+-----------------+-------+%n");

        if (orderedMaxScore != null)
            orderedMaxScore.clear();

        if (orderedPlQueryTerm != null)
            orderedPlQueryTerm.clear();

        if (plQueryTerm != null)
            plQueryTerm.clear();

        if (hm_PosScore != null)
            hm_PosScore.clear();

        if (hm_PosLen != null)
            hm_PosLen.clear();

        pQueueResult.clear();
    }
}
