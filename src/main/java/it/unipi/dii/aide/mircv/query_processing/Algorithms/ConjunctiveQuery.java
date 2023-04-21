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

    public static PriorityQueue<DocumentScore> executeConjunctiveQuery(int k) throws IOException {

        // Priority queue maintaining docid and score
        PriorityQueue<DocumentScore> pQueue
                = new PriorityQueue<>(k, new DecComparatorScore());

        // Arraylist maintaining, for each posting list, the position at which read the next docid
        //ArrayList<Integer> current_pos_pl = new ArrayList<>(Collections.nCopies(plQueryTerm.size(),0));

        // Order the posting list in increasing order of length
        QueryPreprocesser.hm_PosLen = (HashMap<Integer, Integer>) QueryPreprocesser.sortByValue(QueryPreprocesser.hm_PosLen);

        for (Map.Entry<Integer, Integer> entry: QueryPreprocesser.hm_PosLen.entrySet()){
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
                // Keep the candidate docid from the shortest posting list
                current_docid = orderedPlQueryTerm.get(0).getActualPosting().getDocID();

                if(!orderedPlQueryTerm.get(0).postingIterator.hasNext())
                   break; // there isn't any other docid in the shortest posting list

                orderedPlQueryTerm.get(0).nextPosting();

            }while (!checkAllPostingList(current_docid));

            score = 0;

            for (PostingList postingList : orderedPlQueryTerm) {

                if (Flags.isScoreMode())
                    score += Score.BM25(postingList.getTerm(), postingList.getActualPosting(), 1.2, 0.75);
                else
                    score += Score.TFIDF(postingList.getTerm(), postingList.getActualPosting());
            }

            ds = new DocumentScore(current_docid, score);
            pQueue.add(ds);

            // there isn't any other docid in the shortest posting list
            if(!orderedPlQueryTerm.get(0).postingIterator.hasNext())
                return pQueue;
        }
    }

    private static boolean checkAllPostingList(Long docid) throws IOException {
        for(PostingList pl: orderedPlQueryTerm){
            pl.nextGEQ(docid);
            if(pl.getActualPosting() == null)
                return false;
        }
        return true;
    }

}
