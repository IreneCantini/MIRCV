package it.unipi.dii.aide.mircv.query_processing.Algorithms;

import it.unipi.dii.aide.mircv.common.data_structures.CollectionInfo;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.data_structures.Posting;
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
    /*
    private static int sizeQuery;

    private static ArrayList<Integer> pos;

    private static ArrayList<Double> ub;

    // Threshold usata per tener traccia del più piccolo document score durante l'esecuzione dell'algoritmo
    private static double threshold = 0;

    // Pivot usato per dividere in essential list e non essential list
    private static int pivot = 0;

    private static long current;

    private static double score;

    private static double scoreNonEssentialList;

    private static long next;

    */

    // This function returns the minimum docId present in the posting list of query term
    private static long getMinimumDocId(){

        long minDocid = CollectionInfo.getDocid_counter() + 1;

        for (int i=0; i < QueryPreprocesser.orderedPlQueryTerm.size(); i++) {
            if(QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID() < minDocid)
                minDocid = QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID();
        }

        return minDocid;

        /*
        for (PostingList p: QueryPreprocesser.orderedPlQueryTerm){
            if(p.getActualPosting().getDocID() < minDocid)
                minDocid = p.getActualPosting().getDocID();
        }

         */
    }
    /*
    public static PriorityQueue<DocumentScore> executeMaxScore(int k) throws IOException {

        sizeQuery = QueryPreprocesser.orderedPlQueryTerm.size();

        // Array usato per tenere traccia dell'avanzamento dei vari puntatori alle posting lists
        pos = new ArrayList<>(Collections.nCopies(QueryPreprocesser.orderedPlQueryTerm.size(), 0));

        // Priority queue to maintain the docIds with the lowest score in ascending order
        PriorityQueue<DocumentScore> pQueue
                = new PriorityQueue<>(k, new IncComparatorScore());

        // Priority queue to maintain the docIds with the highest score in decreasing order
        PriorityQueue<DocumentScore> decPQueue
                = new PriorityQueue<>(k, new DecComparatorScore());

        // Array of document upper bounds, one per posting lists, initialized all to zero
        ub = new ArrayList<>(sizeQuery);
        Collections.fill(ub, 0.0);

        // Setto il primo upper bound uguale al primo term upper bound della prima posting list
        ub.add(QueryPreprocesser.orderedMaxScore.get(0));

        for (int i = 1; i < sizeQuery; i++) {
            // ub[i] = u[i - 1] + termUpperBound[i]
            ub.add( ub.get(i - 1) + QueryPreprocesser.orderedMaxScore.get(i));
        }

        // Divido le posting lists in essential e non-essential e mi ricavo il pivot iniziale
        pivot = 0;

        if (pivot == -1) return null;

        // Questa variabile è la somma dello score totale dell'essential list più il term upper bound più grande della
        // non essential list
        double documentUB = 0;

        // Prendo il più piccolo docID delle posting list che verrà elaborato
        current = getMinimumDocId();

        while (pivot < sizeQuery && current != -1) {

            score = 0;
            scoreNonEssentialList = 0;
            next = CollectionInfo.getDocid_counter() + 1;

            // Essential Lists
            score = calculateScoreEssentialLists();

            if (pivot == 0)
                documentUB = score;
            else
                documentUB = score + ub.get(pivot - 1);


            // Non-Essential Lists
            if (documentUB > threshold)
            scoreNonEssentialList = calculateScoreNonEssentialLists();

            score += scoreNonEssentialList;

            // List Pivot Update
            DocumentScore d = new DocumentScore(current, score);

            if (pQueue.add(d)) {

                // Serve per il risultato finale
                decPQueue.add(d);

                // Aggiorno la soglia con il minimo score della priority queue
                threshold = pQueue.peek().getScore();

                while ((pivot < sizeQuery) && (ub.get(pivot) <= threshold)) {
                    pivot += 1;
                }
            }

            current = next;
        }
        return decPQueue;
    }
    */
/*
    private static double calculateScoreNonEssentialLists() throws IOException {

        double scoreNonEssentialList = 0;

        for (int i = pivot; i > 0; i--) {
            if ((score + ub.get(i)) < threshold)
                    break;

            if ((QueryPreprocesser.orderedPlQueryTerm.get(i).getPl().get(pos.get(i)).getDocID()) == current) {
                if (Flags.isScoreMode())
                    scoreNonEssentialList += QueryPreprocesser.orderedPlQueryTerm.get(i).getMaxBM25();
                else
                    scoreNonEssentialList += QueryPreprocesser.orderedPlQueryTerm.get(i).getMaxTFIDF();

                // Questa funzione next() permette di avanzare di una posizione avanti nella posting list corrente
                QueryPreprocesser.orderedPlQueryTerm.get(i).next();

                continue;
            }

            // Altrimenti se nella posting list che sto elaborando ho che il docID è diverso da quello corrente, salto,
            // con la nextGeq al posting con docID uguale a quello di current e verifico che sia uguale a quello corrente:
            // se lo è calcolo lo score altrimenti non faccio niente e proseguo alla prossima posting list
            Posting p = QueryPreprocesser.orderedPlQueryTerm.get(i).nextGEQ(current);
            if (p.getDocID() == current) {

                if (Flags.isScoreMode())
                    scoreNonEssentialList += QueryPreprocesser.orderedPlQueryTerm.get(i).getMaxBM25();
                else
                    scoreNonEssentialList += QueryPreprocesser.orderedPlQueryTerm.get(i).getMaxTFIDF();

                // Avanzo di una posizione
                QueryPreprocesser.orderedPlQueryTerm.get(i).next();
            }
        }

        return scoreNonEssentialList;
    }

 */
/*
    private static double calculateScoreEssentialLists() throws IOException {

        double scoreEssentialList = 0;

        for (int i=pivot; i < sizeQuery; i++) {

            if ((QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID()) == current) {
                if (Flags.isScoreMode())
                    scoreEssentialList += QueryPreprocesser.orderedPlQueryTerm.get(i).getMaxBM25();
                else
                    scoreEssentialList += QueryPreprocesser.orderedPlQueryTerm.get(i).getMaxTFIDF();
            }

            // Aggiorno l'iteratore della posting list i-esima (es. pos[0] = 1) (questo era senza skipping)
            //pos.set(i, i+1);

            // Questa funzione next() permette di avanzare di una posizione avanti nella posting list corrente
            Posting p = QueryPreprocesser.orderedPlQueryTerm.get(i).next();

            if (p == null)
                continue;

            if (p.getDocID() < next)
                next = QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID();
        }

        return scoreEssentialList;
    }


 */


    public static PriorityQueue<DocumentScore> executeMaxScore(int k) throws IOException {

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
        // ArrayList<Integer> pos=new ArrayList<>(Collections.nCopies(QueryPreprocesser.orderedPlQueryTerm.size(), 0));

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
            next = CollectionInfo.getDocid_counter()+1;

            // process the essential lists
            for(int i = pivot; i < QueryPreprocesser.orderedPlQueryTerm.size(); i++){
                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting() == null)
                    continue;
                /*
                if(pos.get(i)<0) // This posting list is finished
                    continue;
                 */
                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID() == current){
                    score = updateScore(score, i);

                    QueryPreprocesser.orderedPlQueryTerm.get(i).nextPosting();
                    //pos.set(i, pos.get(i)+1);

                    if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting() == null)
                        continue;
                    /*
                    // check if I finished the posting list (in this case pass to the next posting list)
                    if(QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID() >= QueryPreprocesser.orderedPlQueryTerm.get(i).getPl().size()){
                        pos.set(i, -1);
                        continue;
                    }

                     */
                }

                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID() < next){
                    next = QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID();
                }
            }

            // process non essential list by skipping to the candidate docid
            for(int i = pivot-1; i>=0; i--){

                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting() == null)
                    continue;

                if (score + ub.get(i) <= threshold)
                    // We are sure that the candidate docid cannot be in the final top k documents, and the remaining
                    // posting lists can be skipped completely
                    break;

                // return -1 if the posting list doesn't have a docid grater or equal 'current'.
                QueryPreprocesser.orderedPlQueryTerm.get(i).nextGEQ(current);

                //pos.set(i, QueryPreprocesser.orderedPlQueryTerm.get(i).next(current, pos.get(i)));

                if (QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting() == null)
                    continue;

                if(QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting().getDocID() == current){
                    score = updateScore(score, i);
                }
            }

            // List pivot update
            if(decPQueue.size()<k){
                decPQueue.add(new DocumentScore(current, score));
                incPQueue.add(new DocumentScore(current, score));
            }
            else
            {
                if(incPQueue.peek().getScore() < score){
                    decPQueue.add(new DocumentScore(current, score));
                    incPQueue.add(new DocumentScore(current, score));
                    //remove from decPQueue e incPQueue the element with the smallest score
                    decPQueue.remove(incPQueue.poll());
                }

                threshold = incPQueue.peek().getScore();

                //update pivot
                while (pivot < QueryPreprocesser.orderedPlQueryTerm.size() && ub.get(pivot)<=threshold){
                    pivot++;
                }
            }

            if (next == current)
                current = -1;
            else
                current = next;
            /*
            if(Collections.frequency(pos, -1)==pos.size()){ // I finished scanning all the posting list
                current=-1;
            }

             */
        }
        return decPQueue;
    }


    private static double updateScore(double score, int i) throws IOException {
        if(Flags.isScoreMode()){
            score+= Score.BM25(QueryPreprocesser.orderedPlQueryTerm.get(i).getTerm(), QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting(), 1.2, 0.75);
        }
        else {
            score+= Score.TFIDF(QueryPreprocesser.orderedPlQueryTerm.get(i).getTerm(), QueryPreprocesser.orderedPlQueryTerm.get(i).getActualPosting());
        }
        return score;
    }

}
