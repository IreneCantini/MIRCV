package it.unipi.dii.aide.mircv.query_processing;

import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.common.data_structures.Posting;
import it.unipi.dii.aide.mircv.query_processing.Algorithms.DAAT;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;

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

    public static void executeQueryProcesser(ArrayList<String> tokens) throws IOException, InterruptedException {

        int pos=0;
        for(String t: tokens){
            PostingList pl=new PostingList();
            pl.getPl().clear();
            pl.setTerm(t);

            // TODO: per provare il MaxScore aggiungere !
            if(!Flags.isMaxScore_flag())
                pl.obtainPostingListMaxScore(t);
            else
                pl.obtainPostingListDAAT(t);

            plQueryTerm.add(pl);
            // TODO: per provare il MaxScore aggiungere !
            if(!Flags.isMaxScore_flag()) {
                if (Flags.isScoreMode())
                    hm_PosScore.put(pos, pl.getMaxBM25());
                else
                    hm_PosScore.put(pos, pl.getMaxTFIDF());
            }

            hm_PosLen.put(pos, pl.getPl().size());

            pos++;
        }

        // TODO: per provare il MaxScore aggiungere !
        if(!Flags.isMaxScore_flag()) {
            // ordinamento posting list in base allo score
            hm_PosScore = (HashMap<Integer, Double>) sortByValue(hm_PosScore);

            for (Map.Entry<Integer, Double> entry: hm_PosScore.entrySet()){
                orderedPlQueryTerm.add(plQueryTerm.get(entry.getKey()));
            }

            orderedMaxScore=new ArrayList<>(hm_PosScore.values());

            PriorityQueue<DocumentScore> pQueueResult = executeMaxScore(10);
            DocumentScore d = pQueueResult.poll();
            System.out.println("MaxScore: 1st Document Score is: <" + d.getDocid() + ", " + d.getScore() + ">");

        } else {
            PriorityQueue<DocumentScore> pQueueResult = executeDAAT(10);
            DocumentScore d = pQueueResult.poll();
            System.out.println("DAAT: 1st Document Score is: <" + d.getDocid() + ", " + d.getScore() + ">");
        }

        plQueryTerm.clear();
        if(!Flags.isMaxScore_flag())
        {
            orderedMaxScore.clear();
            orderedPlQueryTerm.clear();
        }
        hm_PosScore.clear();
        hm_PosLen.clear();
    }

/*
    public static ArrayList<Long> executeConjunctiveQuery(){
        // Order the posting list in increasing order of length
        hm_PosLen = (HashMap<Integer, Integer>) sortByValue(hm_PosLen);

        for (Map.Entry<Integer, Double> entry: hm_PosScore.entrySet()){
            orderedPlQueryTerm.add(plQueryTerm.get(entry.getKey()));
        }

        // Array to maintain for each posting list the position of the docID to analyze
        // At the beginning they are all zero because we start from the first position
        //ArrayList<Integer> pos=new ArrayList<>(Collections.nCopies(QueryPreprocesser.orderedPlQueryTerm.size(), 0));
        Long current;
        int posFinal;
        int currentPos;
        ArrayList<Long> finalDocIdList = new ArrayList<>();
        ArrayList<Long> temp = new ArrayList<>();

        for (Posting p: orderedPlQueryTerm.get(0).getPl())
            finalDocIdList.add(p.getDocID());

        //binary merge boolean conjunctive algorithm
        for(int i=1; i<orderedPlQueryTerm.size(); i++){
            posFinal=0;
            temp.clear();
            current=finalDocIdList.get(posFinal);
            while (true){
                currentPos=orderedPlQueryTerm.get(i).next(current, 0);
                if(currentPos>0){
                    if(orderedPlQueryTerm.get(i).getPl().get(currentPos).getDocID()==current){
                        // docid found
                        temp.add(current);
                    }
                }

                posFinal++;
                if(posFinal==finalDocIdList.size() || currentPos<0)
                    break;

                current=finalDocIdList.get(posFinal);
            }

            finalDocIdList.clear();
            finalDocIdList.addAll(temp);
            if(finalDocIdList.size()==0){
                // there aren't common docId
                // the query doesn't produce results
                return null;
            }
        }

        return finalDocIdList;
    }

*/
}
