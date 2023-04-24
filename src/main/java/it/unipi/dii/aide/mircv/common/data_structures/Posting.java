package it.unipi.dii.aide.mircv.common.data_structures;

public class Posting {
    private final long docID;
    private final int termFrequency;

    public Posting(long docID, int termFrequency) {
        this.docID = docID;
        this.termFrequency = termFrequency;
    }

    public long getDocID() {
        return docID;
    }

    public int getTermFrequency() {
        return termFrequency;
    }
}
