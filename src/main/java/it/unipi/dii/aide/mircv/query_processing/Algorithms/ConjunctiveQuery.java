package it.unipi.dii.aide.mircv.query_processing.Algorithms;

import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.document_score.DecComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.IncComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.utils.Score;

import java.io.IOException;
import java.util.*;

import static it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser.orderedPlQueryTerm;
import static it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser.plQueryTerm;

public class ConjunctiveQuery {

    /**
     * Function that perform the conjunctive query
     * @param k number of docs returned
     * @return a priority queue containing the top k docs
     * @throws IOException if the channel is not found
     */
    public static PriorityQueue<DocumentScore> executeConjunctiveQuery(int k) throws IOException {

        /* Temporary score */
        double score;

        /* Temporary variables */
        long current_docid;
        boolean firstTime=true;

        /* Priority queue to maintain the docIds with the highest score in decreasing order */
        PriorityQueue<DocumentScore> decPQueue
                = new PriorityQueue<>(k, new DecComparatorScore());

        /* Priority queue to maintain the docIds with the highest score in increasing order
           (it is used to maintain the smallest score)
        */
        PriorityQueue<DocumentScore> incPQueue
                = new PriorityQueue<>(k, new IncComparatorScore());

        /* Order the posting list in increasing order of length */
        QueryPreprocesser.hm_PosLen = (HashMap<Integer, Integer>) QueryPreprocesser.sortByValue(QueryPreprocesser.hm_PosLen);

        for (Map.Entry<Integer, Integer> entry: QueryPreprocesser.hm_PosLen.entrySet())
            QueryPreprocesser.orderedPlQueryTerm.add(plQueryTerm.get(entry.getKey()));

        /* Loop until all the docIDs of the first posting list have been processed */
        while (true) {

            /* fetch the first docid, present in all the posting list, among those not yet processed */
            do {
                if (!firstTime)
                    orderedPlQueryTerm.get(0).nextPosting();

                firstTime = false;
                if (orderedPlQueryTerm.get(0).getActualPosting() == null)
                    return decPQueue;

                /* Keep the candidate docID from the shortest posting list */
                current_docid = orderedPlQueryTerm.get(0).getActualPosting().getDocID();

            } while(!checkAllPostingList(current_docid));

            score = 0;

            for (PostingList postingList : orderedPlQueryTerm) {

                if (Flags.isScoreMode())
                    score += Score.BM25(postingList.getTerm(), postingList.getActualPosting(), 1.2, 0.75);
                else
                    score += Score.TFIDF(postingList.getTerm(), postingList.getActualPosting());
            }

            if (decPQueue.size() < k) {

                decPQueue.add(new DocumentScore(current_docid, score));
                incPQueue.add(new DocumentScore(current_docid, score));
            }else {

                if (incPQueue.peek().getScore() < score) {
                    decPQueue.add(new DocumentScore(current_docid, score));
                    incPQueue.add(new DocumentScore(current_docid, score));

                    /* Remove from decPQueue e incPQueue the element with the smallest score */
                    decPQueue.remove(incPQueue.poll());
                }
            }

            /* There isn't any other docID in the shortest posting list */
            if (!orderedPlQueryTerm.get(0).postingIterator.hasNext())
                return decPQueue;
        }
    }

    private static boolean checkAllPostingList(Long docid) throws IOException {

        for (PostingList pl: orderedPlQueryTerm) {
            pl.nextGEQ(docid);

            if (pl.getActualPosting() == null)
                return false;
        }
        return true;
    }
}
