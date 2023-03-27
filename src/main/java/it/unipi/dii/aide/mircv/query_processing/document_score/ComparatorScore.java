package it.unipi.dii.aide.mircv.query_processing.document_score;

import java.util.Comparator;

public class ComparatorScore implements java.util.Comparator<DocumentScore>{
    // Overriding compare()method of Comparator
    // for decreasing order of score
    @Override
    public int compare(DocumentScore o1, DocumentScore o2) {
        if(o1.getScore() == o2.getScore()) {
            if (o1.getDocid() < o2.getDocid())
                return -1;
            else
                return 1;
        }else
            return ((o1.getScore() - o2.getScore()) > 0) ? -1 : 1;
    }
}
