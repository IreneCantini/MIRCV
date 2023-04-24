package it.unipi.dii.aide.mircv.query_processing.document_score;

public class IncComparatorScore implements java.util.Comparator<DocumentScore>{
    @Override
    public int compare(DocumentScore o1, DocumentScore o2) {
        if(o1.getScore() == o2.getScore()) {
            if (o1.getDocid() < o2.getDocid())
                return -1;
            else
                return 1;
        }else
            return ((o1.getScore() - o2.getScore()) < 0) ? -1 : 1;
    }
}
