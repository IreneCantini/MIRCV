package it.unipi.dii.aide.mircv.query_processing.Algorithms;

import it.unipi.dii.aide.mircv.cli.utils.UploadDataStructures;
import it.unipi.dii.aide.mircv.common.data_structures.DocumentIndexElem;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.query_processing.document_score.DecComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.DocumentScore;
import it.unipi.dii.aide.mircv.query_processing.document_score.IncComparatorScore;
import it.unipi.dii.aide.mircv.query_processing.utils.QueryUtils;
import it.unipi.dii.aide.mircv.query_processing.utils.Score;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.*;
import static it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser.plQueryTerm;
import static it.unipi.dii.aide.mircv.query_processing.utils.QueryUtils.documentBinarySearch;
import static it.unipi.dii.aide.mircv.query_processing.utils.QueryUtils.minimumDocID;

public class DAAT {

    public static PriorityQueue<DocumentScore> executeDAAT(int k) throws IOException {

        // Priority queue contenente docId e score
        PriorityQueue<DocumentScore> pQueue
                = new PriorityQueue<>(k, new DecComparatorScore());

        // Arraylist contenente la posizione a cui sono arrivata per ogni posting list
        ArrayList<Integer> current_pos_pl = new ArrayList<Integer>(Collections.nCopies(plQueryTerm.size(),0));


        double score; // Variabili contenente lo score temporaneo
        long next;  // Variabile utile per salvarsi il prossimo docId da dover analizzare

        // Variabili di appoggio
        DocumentScore ds;

        //DocumentIndexElem docElem=new DocumentIndexElem();

        long current_docid = minimumDocID(); // Prendo il docId più piccolo tra quelli contenuti all'interno delle
        // posting list dei termini della query
        //Ciclo fino a quando non ho finito di analizzare tutti i documenti delle posting list
        while(true) {
            //docElem = documentBinarySearch(current_docid);

            score = 0;
            next = (UploadDataStructures.Document_Index.size()) + 1;


            for (int i = 0; i < plQueryTerm.size(); i++) {
                if (plQueryTerm.get(i).getPl().get(current_pos_pl.get(i)).getDocID() == current_docid) {

                    if(Flags.isScoreMode())
                        score += Score.BM25(plQueryTerm.get(i).getTerm(), plQueryTerm.get(i).getPl().get(i), 1.2, 0.75);
                    else
                        score += Score.TFIDF(plQueryTerm.get(i).getTerm(), plQueryTerm.get(i).getPl().get(i));
                    current_pos_pl.set(i, current_pos_pl.get(i) + 1);
                }
                // Se ho finito i posting all'interno della posting list devo proseguire senza scorrere
                if (plQueryTerm.get(i).getPl().size() - 1 < current_pos_pl.get(i))
                    continue;

                // Altrimenti scorro e lo confronto con il valore di next
                if (plQueryTerm.get(i).getPl().get(current_pos_pl.get(i)).getDocID() < next) {
                    next = plQueryTerm.get(i).getPl().get(current_pos_pl.get(i)).getDocID();
                }
            }

            ds = new DocumentScore(current_docid, score);
            pQueue.add(ds);
            if (current_docid == next)
                break;
            current_docid = next;


        }

        return pQueue;
    }
}
