package it.unipi.dii.aide.mircv.query_processing.Algorithms;

import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.document_score.ComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

public class MaxScore {

    public static void executeMaxScore(int k){

        ArrayList<Double> sortedMaxScore = new ArrayList<>(QueryPreprocesser.score_hm.keySet());
        Collections.sort(sortedMaxScore);

        PriorityQueue<DocumentScore> pQueue
                = new PriorityQueue<>(k, new ComparatorScore());

        ArrayList<Double> ub= new ArrayList<>();
    }

}
