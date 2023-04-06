package it.unipi.dii.aide.mircv.query_processing;

import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.query_processing.Algorithms.DAAT;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.*;

import static it.unipi.dii.aide.mircv.query_processing.Algorithms.DAAT.*;
import static it.unipi.dii.aide.mircv.query_processing.Algorithms.MaxScore.*;



public class QueryPreprocesser {

    public static ArrayList<PostingList> plQueryTerm = new ArrayList<>();
    public static ArrayList<PostingList> orderedPlQueryTerm = new ArrayList<>();
    public static HashMap<Integer, Double> hm_PosScore = new HashMap<>();
    public static ArrayList<Double> orderedMaxScore;

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

    public static void executeQueryProcesser(ArrayList<String> tokens) throws IOException, InterruptedException {

        int pos=0;
        double score=0;
        for(String t: tokens){
            PostingList pl=new PostingList();
            pl.getPl().clear();
            pl.setTerm(t);
            pl.obtainPostingList(t, score);
            if(!Flags.isMaxScore_flag())
                plQueryTerm.add(pl);
            else
                hm_PosScore.put(pos, score);

            pos++;
        }

        if(Flags.isMaxScore_flag())
        {
            // ordinamento posting list in base allo score
            hm_PosScore = (HashMap<Integer, Double>) sortByValue(hm_PosScore);

            for (Map.Entry<Integer, Double> entry: hm_PosScore.entrySet()){
                orderedPlQueryTerm.add(plQueryTerm.get(entry.getKey()));
            }

            orderedMaxScore=new ArrayList<>(hm_PosScore.values());

            executeMaxScore(10);
        }

        else
            executeDAAT(10);

        plQueryTerm.clear();
        //score_hm.clear();
    }


}
