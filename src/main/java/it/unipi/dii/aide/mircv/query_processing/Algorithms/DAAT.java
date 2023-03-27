package it.unipi.dii.aide.mircv.query_processing.Algorithms;

import it.unipi.dii.aide.mircv.query_processing.document_score.ComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

public class DAAT {
    /*
    private PriorityQueue<DocumentScore> DAAT(int k) throws IOException {
        // Priority queue contenente docId e score
        PriorityQueue<DocumentScore> pQueue
                = new PriorityQueue<>(k, new ComparatorScore());

        // Arraylist contenente la posizione a cui sono arrivata per ogni posting list
        ArrayList<Integer> current_pos_pl = new ArrayList<Integer>(Collections.nCopies(listTermQuery.size(),0));


        double score; // Variabili contenente lo score temporaneo
        long next;  // Variabile utile per salvarsi il prossimo docId da dover analizzare

        // Variabili di appoggio
        DocumentScore ds;

        long current_docid = minimumDocID(); // Prendo il docId più piccolo tra quelli contenuti all'interno delle
        // posting list dei termini della query
        //Ciclo fino a quando non ho finito di analizzare tutti i documenti delle posting list
        while(true){

            long lengthCurrentDocid=getDL(current_docid);
            score = 0;
            next = (fileChannelDict.size()/68) + 1;


            for(int i=0; i<listTermQuery.size(); i++){
                if(listTermQuery.get(i).getPl().get(current_pos_pl.get(i)).getDocID() == current_docid){
                    score+=BM25(listTermQuery.get(i).getPl().get(current_pos_pl.get(i)).getTermFrequency(), lengthCurrentDocid, this.averageLengthDoc, (double)(fileChannelDocIndex.size()-8)/36, listTermQuery.get(i).getPl().size());
                    current_pos_pl.set(i, current_pos_pl.get(i)+1);
                }
                // Se ho finito i posting all'interno della posting list devo proseguire senza scorrere
                if(listTermQuery.get(i).getPl().size()-1<current_pos_pl.get(i))
                    continue;

                // Altrimenti scorro e lo confronto con il valore di next
                if(listTermQuery.get(i).getPl().get(current_pos_pl.get(i)).getDocID() < next){
                    next=listTermQuery.get(i).getPl().get(current_pos_pl.get(i)).getDocID();
                }
            }

            ds=new Document_Score(current_docid, score);
            pQueue.add(ds);
            if(current_docid==next)
                break;
            current_docid=next;
        }

        return pQueue;
    }

     */
}
