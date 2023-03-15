package it.unipi.dii.aide.mircv.common.data_structures;

public class Posting {
    private long docID;
    private int termFrequency;

    public Posting(long docID, int termFrequency) {
        this.docID = docID;
        this.termFrequency = termFrequency;
    }

    public void setDocID(long docID) {
        this.docID = docID;
    }

    public void setTermFrequency(int termFrequency) {
        this.termFrequency = termFrequency;
    }

    public long getDocID() {
        return docID;
    }

    public int getTermFrequency() {
        return termFrequency;
    }
}
