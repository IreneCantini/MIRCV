package it.unipi.dii.aide.mircv.query_processing.Algorithms;

import it.unipi.dii.aide.mircv.common.data_structures.CollectionInfo;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.query_processing.document_score.DecComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.IncComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.utils.Score;

import java.io.IOException;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser.plQueryTerm;
import static it.unipi.dii.aide.mircv.query_processing.utils.QueryUtils.minimumDocID;

public class DAAT {

    public static PriorityQueue<DocumentScore> executeDAAT(int k) throws IOException {

        // Priority queue to maintain the docIds with the highest score in decreasing order
        PriorityQueue<DocumentScore> decPQueue
                = new PriorityQueue<>(k, new DecComparatorScore());

        // Priority queue to maintain the docIds with the highest score in increasing order
        // it is used to maintain the smallest score
        PriorityQueue<DocumentScore> incPQueue
                = new PriorityQueue<>(k, new IncComparatorScore());

        double score; // Variabili contenente lo score temporaneo
        long next;  // Variabile utile per salvarsi il prossimo docId da dover analizzare

        long current_docid = minimumDocID(); // Prendo il docId più piccolo tra quelli contenuti all'interno delle
        // posting list dei termini della query
        //Ciclo fino a quando non ho finito di analizzare tutti i documenti delle posting list
        while(true) {

            score = 0;
            next = CollectionInfo.getDocid_counter()+1;


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
                // Se ho finito i posting all'interno della posting list devo proseguire senza scorrere
                if (postingList.getActualPosting() == null)
                    continue;

                // Altrimenti scorro e lo confronto con il valore di next
                if (postingList.getActualPosting().getDocID() < next) {
                    next = postingList.getActualPosting().getDocID();
                }
            }

            if(decPQueue.size()<k){

                decPQueue.add(new DocumentScore(current_docid, score));
                incPQueue.add(new DocumentScore(current_docid, score));
            } else {

                if (incPQueue.peek().getScore() < score) {
                    decPQueue.add(new DocumentScore(current_docid, score));
                    incPQueue.add(new DocumentScore(current_docid, score));
                    //remove from decPQueue e incPQueue the element with the smallest score
                    decPQueue.remove(incPQueue.poll());
                }
            }

            if (current_docid == next)
                break;
            current_docid = next;

        }

        return decPQueue;
    }
}
