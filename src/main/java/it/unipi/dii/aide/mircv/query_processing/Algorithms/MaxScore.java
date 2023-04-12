package it.unipi.dii.aide.mircv.query_processing.Algorithms;

import it.unipi.dii.aide.mircv.common.data_structures.CollectionInfo;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.document_score.DecComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.IncComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.utils.Score;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

public class MaxScore {

    // This function returns the minimum docId present in the posting list of query term
    private static long getMinimumDocId(){
        long minDocid=0;
        for (PostingList p: QueryPreprocesser.orderedPlQueryTerm){
            if(p.getPl().get(0).getDocID()<minDocid)
                minDocid=p.getPl().get(0).getDocID();
        }
        return minDocid;
    }

    public static void executeMaxScore(int k) throws IOException {

        // Priority queue to maintain the docIds with the highest score in decreasing order
        PriorityQueue<DocumentScore> decPQueue
                = new PriorityQueue<>(k, new DecComparatorScore());

        // Priority queue to maintain the docIds with the highest score in increasing order
        // it is used to maintain the smallest score
        PriorityQueue<DocumentScore> incPQueue
                = new PriorityQueue<>(k, new IncComparatorScore());


        ArrayList<Double> ub= new ArrayList<>();

        // Array to maintain for each posting list the position of the docID to analyze
        // At the beginning they are all zero because we start from the first position
        ArrayList<Integer> pos=new ArrayList<>(Collections.nCopies(QueryPreprocesser.orderedPlQueryTerm.size(), 0));

        ub.add(QueryPreprocesser.orderedMaxScore.get(0));

        for(int i=1; i<QueryPreprocesser.orderedPlQueryTerm.size(); i++){
            ub.add(ub.get(i-1)+QueryPreprocesser.orderedMaxScore.get(i));
        }

        // set initial threshold, pivot and the first docid to analyze
        double threshold=0;
        int pivot=0;
        long current = getMinimumDocId();

        double score;
        long next;

        // While there is at least an essential list and there are documents to process
        while (pivot<QueryPreprocesser.orderedPlQueryTerm.size() && current!=-1){
            score=0;
            // next <- +infinite
            next= CollectionInfo.getDocid_counter()+1;

            // process the essential lists
            for(int i =pivot; i<QueryPreprocesser.orderedPlQueryTerm.size(); i++){
                if(pos.get(i)<0) // This posting list is finished
                    continue;
                if(QueryPreprocesser.orderedPlQueryTerm.get(i).getPl().get(pos.get(i)).getDocID()==current){
                    score = updateScore(pos, score, i);
                    pos.set(i, pos.get(i)+1);
                    // check if I finished the posting list (in this case pass to the next posting list)
                    if(pos.get(i)>=QueryPreprocesser.orderedPlQueryTerm.get(i).getPl().size()){
                        pos.set(i, -1);
                        continue;
                    }
                }
                if(QueryPreprocesser.orderedPlQueryTerm.get(i).getPl().get(pos.get(i)).getDocID()<next){
                    next=QueryPreprocesser.orderedPlQueryTerm.get(i).getPl().get(pos.get(i)).getDocID();
                }
            }

            // process non essential list by skipping to the candidate docid
            for(int i = pivot-1; i>=0; i--){
                if(pos.get(i)<0) //This posting list is finished
                    continue;

                if(score+ub.get(i)<=threshold)
                    // We are sure that the candidate docid cannot be in the final top k documents, and the remaining
                    // posting lists can be skipped completely
                    break;

                // return -1 if the posting list doesn't have a docid grater or equal 'current'.
                pos.set(i, QueryPreprocesser.orderedPlQueryTerm.get(i).next(current, pos.get(i)));

                if(pos.get(i)<0)
                    continue;

                if(QueryPreprocesser.orderedPlQueryTerm.get(i).getPl().get(pos.get(i)).getDocID()==current){
                    score = updateScore(pos, score, i);
                }
            }

            // List pivot update
            if(decPQueue.size()<k){
                decPQueue.add(new DocumentScore(current, score));
                incPQueue.add(new DocumentScore(current, score));
            }
            else
            {
                if(incPQueue.peek().getScore()<score){
                    decPQueue.add(new DocumentScore(current, score));
                    incPQueue.add(new DocumentScore(current, score));
                    //remove from decPQueue e incPQueue the element with the smallest score
                    decPQueue.remove(incPQueue.poll());
                }

                threshold=incPQueue.peek().getScore();

                //update pivot
                while (pivot<QueryPreprocesser.orderedPlQueryTerm.size() && ub.get(pivot)<=threshold){
                    pivot++;
                }
            }

            current=next;
            if(Collections.frequency(pos, -1)==pos.size()){ // I finished scanning all the posting list
                current=-1;
            }
        }
   }

    private static double updateScore(ArrayList<Integer> pos, double score, int i) throws IOException {
        if(Flags.isScoreMode()){
            score+= Score.BM25(QueryPreprocesser.orderedPlQueryTerm.get(i).getTerm(), QueryPreprocesser.orderedPlQueryTerm.get(i).getPl().get(pos.get(i)), 1.2, 0.75);
        }
        else {
            score+= Score.TFIDF(QueryPreprocesser.orderedPlQueryTerm.get(i).getTerm(), QueryPreprocesser.orderedPlQueryTerm.get(i).getPl().get(pos.get(i)));
        }
        return score;
    }

}
