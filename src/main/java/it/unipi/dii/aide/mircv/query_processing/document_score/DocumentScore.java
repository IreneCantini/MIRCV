package it.unipi.dii.aide.mircv.query_processing.document_score;

public class DocumentScore {
    long docid;
    double score;

    public DocumentScore(long docid, double score) {
        this.docid = docid;
        this.score = score;
    }

    public long getDocid() {
        return docid;
    }

    public double getScore() {
        return score;
    }

    public void setDocid(long docid) {
        this.docid = docid;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof DocumentScore){
            DocumentScore c = (DocumentScore) o;
            return docid == c.getDocid();
        }
        return false;
    }
}
