package it.unipi.dii.aide.mircv.query_processing.Algorithms;

import it.unipi.dii.aide.mircv.common.data_structures.CollectionInfo;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.query_processing.document_score.DecComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.IncComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.utils.Score;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser.plQueryTerm;

public class DAAT {

    /**
     * Function that performs the DAAT algorithm
     * @param k the number of how many top documents will be returned
     * @return a priority queue containing the docs
     * @throws IOException if the channel is not found
     */
    public static PriorityQueue<DocumentScore> executeDAAT(int k) throws IOException {

        /* Priority queue to maintain the docIds with the highest score in decreasing order */
        PriorityQueue<DocumentScore> decPQueue
                = new PriorityQueue<>(k, new DecComparatorScore());

        /* Priority queue to maintain the docIds with the highest score in increasing order
           (it is used to maintain the smallest score)
        */
        PriorityQueue<DocumentScore> incPQueue
                = new PriorityQueue<>(k, new IncComparatorScore());

        /* Temporary score */
        double score;

        /* Next docID to analyze */
        long next;

        /* Take the smallest docId among those contained within the query term posting lists */
        long current_docid = minimumDocID();

        /* Cycle until I have finished analyzing all the documents on the posting lists */
        while(true) {

            score = 0;
            next = CollectionInfo.getDocid_counter() + 1;

            for (PostingList postingList : plQueryTerm) {

                if (postingList.getActualPosting() == null)
                    continue;

                if (postingList.getActualPosting().getDocID() == current_docid) {
                    if (Flags.isScoreMode())
                        score += Score.BM25(postingList.getTerm(), postingList.getActualPosting(), 1.2, 0.75);
                    else
                        score += Score.TFIDF(postingList.getTerm(), postingList.getActualPosting());

                    postingList.nextPosting();
                }

                /* If run out of postings within the posting list, must continue without scrolling */
                if (postingList.getActualPosting() == null)
                    continue;

                /* Otherwise scroll and compare it with the value of next */
                if (postingList.getActualPosting().getDocID() < next) {
                    next = postingList.getActualPosting().getDocID();
                }
            }

            if (decPQueue.size() < k){

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

            if (current_docid == next)
                break;

            current_docid = next;
        }
        return decPQueue;
    }

    public static long minimumDocID(){
        ArrayList<Long> docid_candidate = new ArrayList<>();

        for(PostingList pl: plQueryTerm){
            docid_candidate.add(pl.getPl().get(0).getDocID());
        }

        return Collections.min(docid_candidate);
    }
}
