package it.unipi.dii.aide.mircv.query_processing.Algorithms;

import it.unipi.dii.aide.mircv.common.data_structures.CollectionInfo;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.document_score.DecComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.IncComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.utils.Score;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

public class MaxScore {

    /**
     *
     * @return the minimum docId present in the posting list of query term
     */
    private static long getMinimumDocId(){

        long minDocid = CollectionInfo.getDocid_counter() + 1;

        for (int i = 0; i < QueryPreprocesser.orderedPlQueryTerm.size(); i++) {
            if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID() < minDocid)
                minDocid = QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID();
        }

        return minDocid;
    }

    /**
     * Function that perform the MaxScore algorithm with skipping
     * @param k the number of top documents to return
     * @return a priority queue of docs
     * @throws IOException if the channel is not found
     */
    public static PriorityQueue<DocumentScore> executeMaxScore(int k) throws IOException {

        /* Priority queue to maintain the docIds with the highest score in decreasing order */
        PriorityQueue<DocumentScore> decPQueue
                = new PriorityQueue<>(k, new DecComparatorScore());

        /* Priority queue to maintain the docIds with the highest score in increasing order
           (it is used to maintain the smallest score)
         */
        PriorityQueue<DocumentScore> incPQueue
                = new PriorityQueue<>(k, new IncComparatorScore());

        ArrayList<Double> ub = new ArrayList<>();

        ub.add(QueryPreprocesser.orderedMaxScore.get(0));

        for (int i = 1; i<QueryPreprocesser.orderedPlQueryTerm.size(); i++)
            ub.add(ub.get(i-1)+QueryPreprocesser.orderedMaxScore.get(i));

        // Set initial threshold, pivot and the first docID to analyze */
        double threshold = 0;
        double score;
        long next;
        int pivot = 0;

        long current = getMinimumDocId();

        /* While there is at least an essential list and there are documents to process */
        while (pivot < QueryPreprocesser.orderedPlQueryTerm.size() && current != -1){
            score = 0;

            /* next <- +infinite */
            next = CollectionInfo.getDocid_counter()+1;

            /* PROCESS THE ESSENTIAL LISTS */

            for (int i = pivot; i < QueryPreprocesser.orderedPlQueryTerm.size(); i++){

                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting() == null)
                    continue;

                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID() == current){
                    score = updateScore(score, i);

                    QueryPreprocesser.orderedPlQueryTerm.get(i).nextPosting();

                    if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting() == null)
                        continue;
                }

                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID() < next){
                    next = QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID();
                }
            }

            /* PROCESS THE NON-ESSENTIAL LISTS */

            for (int i = pivot - 1; i > 0; i--) {

                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting() == null)
                    continue;

                if (score + ub.get(i) <= threshold)
                    /* We are sure that the candidate docID cannot be in the final top k documents, and the remaining
                        posting lists can be skipped completely
                    */
                    break;

                /* Return -1 if the posting list doesn't have a docID grater or equal 'current' */
                QueryPreprocesser.orderedPlQueryTerm.get(i).nextGEQ(current);

                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting() == null)
                    continue;

                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID() == current)
                    score = updateScore(score, i);
            }

            /* LIST PIVOT UPDATE */

            if (decPQueue.size() < k){

                decPQueue.add(new DocumentScore(current, score));
                incPQueue.add(new DocumentScore(current, score));

            }else {

                if (incPQueue.peek().getScore() < score) {
                    decPQueue.add(new DocumentScore(current, score));
                    incPQueue.add(new DocumentScore(current, score));

                    /* Remove from decPQueue e incPQueue the element with the smallest score */
                    decPQueue.remove(incPQueue.poll());
                }

                threshold = incPQueue.peek().getScore();

                /* Update pivot */
                while (pivot < QueryPreprocesser.orderedPlQueryTerm.size() && ub.get(pivot) <= threshold){
                    pivot++;
                }
            }

            if (next == CollectionInfo.getDocid_counter()+1)
                break;
            else
                current = next;
        }

        return decPQueue;
    }

    /**
     * Function that takes the partial score and updates it
     * @param score is the partial score
     * @param i to access to the right posting list
     * @return a new score
     */
    private static double updateScore(double score, int i) {

        if (Flags.isScoreMode())
            score+= Score.BM25(QueryPreprocesser.orderedPlQueryTerm.get(i).getTerm(), QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting(), 1.2, 0.75);
        else
            score+= Score.TFIDF(QueryPreprocesser.orderedPlQueryTerm.get(i).getTerm(), QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting());

        return score;
    }
}
