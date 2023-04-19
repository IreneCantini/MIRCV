package it.unipi.dii.aide.mircv.query_processing.Algorithms;

import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.document_score.DecComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;
import it.unipi.dii.aide.mircv.query_processing.utils.Score;

import java.io.IOException;
import java.util.*;

import static it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser.orderedPlQueryTerm;
import static it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser.plQueryTerm;

public class ConjunctiveQuery {
    /*
    public static PriorityQueue<DocumentScore> executeConjunctiveQuery(int k) throws IOException {

        // Priority queue maintaining docid and score
        PriorityQueue<DocumentScore> pQueue
                = new PriorityQueue<>(k, new DecComparatorScore());

        // Arraylist maintaining, for each posting list, the position at which read the next docid
        ArrayList<Integer> current_pos_pl = new ArrayList<>(Collections.nCopies(plQueryTerm.size(),0));

        // Order the posting list in increasing order of length
        QueryPreprocesser.hm_PosLen = (HashMap<Integer, Integer>) QueryPreprocesser.sortByValue(QueryPreprocesser.hm_PosLen);

        for (Map.Entry<Integer, Double> entry: QueryPreprocesser.hm_PosScore.entrySet()){
            QueryPreprocesser.orderedPlQueryTerm.add(plQueryTerm.get(entry.getKey()));
        }

        double score; // Temporary score

        // temporary variable
        DocumentScore ds;
        long current_docid = 0;

        //Loop until all the docId of the first postinglist have been processed
        while(true) {

            // fetch the first docid to analyze
            // it is the shortest one presents in all the posting list
            do{
                if((current_pos_pl.get(0)==orderedPlQueryTerm.get(0).getPl().size()))
                    return pQueue; // there isn't any other docid in the shortest posting list

                // Keep the candidate docid from the shortest posting list
                current_docid = orderedPlQueryTerm.get(0).getPl().get(current_pos_pl.get(0)).getDocID();
                current_pos_pl.set(0, current_pos_pl.get(0)+1);
            }while (!checkAllPostingList(current_docid, current_pos_pl));

            score = 0;

            for (int i = 0; i < orderedPlQueryTerm.size(); i++) {

                if(Flags.isScoreMode())
                    score += Score.BM25(orderedPlQueryTerm.get(i).getTerm(), orderedPlQueryTerm.get(i).getPl().get(current_pos_pl.get(i)), 1.2, 0.75);
                else
                    score += Score.TFIDF(orderedPlQueryTerm.get(i).getTerm(), orderedPlQueryTerm.get(i).getPl().get(current_pos_pl.get(i)));
            }

            ds = new DocumentScore(current_docid, score);
            pQueue.add(ds);
        }
    }

    private static boolean checkAllPostingList(Long docid, ArrayList<Integer> positions){
        int tempPos;
        int i=0;
        for(PostingList pl: orderedPlQueryTerm){
            tempPos=pl.next(docid,0);
            if(tempPos<0 || (pl.getPl().get(tempPos).getDocID()>docid))
                return false; // docid not present in one posting list

            positions.set(i, tempPos);
            i++;
        }
        return true;
    }
    */
}
