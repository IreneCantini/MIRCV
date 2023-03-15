package it.unipi.dii.aide.mircv.common.data_structures;

public class DictionaryElem {
    private String term;
    private int documentFrequency; //in quanti documenti compare il termine
    private long collectionFrequency;   //quante volte compare nella collection
    private long offset_start_doc; //offset di dove inizia la postinglist dei docID
    private int lengthPostingList_doc; //grandezza della postinglist dei docID
    private long offset_start_freq; //offset di dove inizia la postinglist delle frequenze
    private int lengthPostingList_freq; //grandezza della la postinglist delle frequenze

    private long offset_start_skipping; //offset di dove inizia la lista di skipping

    private int lengthSkippingList; //grandezza della skipping list

    public DictionaryElem(){
        this.term = " ";
        this.documentFrequency = 0;
        this.collectionFrequency = 0;
        this.offset_start_doc = 0;
        this.lengthPostingList_doc = 0;
        this.offset_start_freq = 0;
        this.lengthPostingList_freq =0;
        this.offset_start_skipping = 0;
        this.lengthSkippingList = 0;
    }

    public DictionaryElem(String term, int docFreq, long collFreq) {
        int diffLength=20-term.length();
        if(diffLength!=0){
            // andiamo a concatenare " " per arrivare alla dimensione fissa di 20 caratteri
            term = term + " ".repeat(Math.max(0, diffLength));
        }
        this.term=term;
        this.documentFrequency=docFreq;
        this.collectionFrequency=collFreq;
        this.offset_start_doc = 0;
        this.lengthPostingList_doc = 0;
        this.offset_start_freq = 0;
        this.lengthPostingList_freq =0;
    }

    public DictionaryElem(DictionaryElem d){
        this.term = d.getTerm();
        this.documentFrequency = d.getDocumentFrequency();
        this.collectionFrequency = d.getCollectionFrequency();
        this.offset_start_doc = d.getOffset_start_doc();
        this.lengthPostingList_doc = d.getLengthPostingList_doc();
        this.offset_start_freq = d.getOffset_start_freq();
        this.lengthPostingList_freq =d.getLengthPostingList_freq();
    }

    public void setCollectionFrequency(long f) {collectionFrequency = f;}
    public long getCollectionFrequency() {return this.collectionFrequency;}

    public void setDocumentFrequency(int f) {this.documentFrequency = f;}
    public int getDocumentFrequency() {return this.documentFrequency;}

    public void setTerm(String term) {
        int diffLength=20-term.length();
        if(diffLength!=0){
            // andiamo a concatenare " " per arrivare alla dimensione fissa di 20 caratteri
            term = term + " ".repeat(Math.max(0, diffLength));
        }

        this.term = term;
    }
    public String getTerm() {return this.term;}

    public void setOffset_start_freq(long offset_freq) {
        this.offset_start_freq = offset_freq;
    }

    public void setOffset_start_doc(long offset_doc) {
        this.offset_start_doc = offset_doc;
    }

    public void setLengthPostingList_freq(int length) {
        this.lengthPostingList_freq = length;
    }

    public void setLengthPostingList_doc(int length) {
        this.lengthPostingList_doc = length;
    }



    public void incDocumentFrequency(){
        this.documentFrequency++;
    }
    public void incDocumentFrequency(int f){
        this.documentFrequency+=f;
    }

    public void incCollectionFrequency(long f){
        this.collectionFrequency+=f;
    }

    public long getOffset_start_doc() {
        return offset_start_doc;
    }

    public long getOffset_start_freq() {
        return offset_start_freq;
    }

    public int getLengthPostingList_doc() {
        return lengthPostingList_doc;
    }

    public int getLengthPostingList_freq() {
        return lengthPostingList_freq;
    }

    public void incLengthPostingList_doc(int newlength) { this.lengthPostingList_doc+=newlength;}
    public void incLengthPostingList_freq(int newlength) { this.lengthPostingList_freq+=newlength;}

    public long getOffset_start_skipping() {
        return offset_start_skipping;
    }

    public int getLengthSkippingList() {
        return lengthSkippingList;
    }

    public void setOffset_start_skipping(long offset_start_skipping) {
        this.offset_start_skipping = offset_start_skipping;
    }

    public void setLengthSkippingList(int lengthSkippingList) {
        this.lengthSkippingList = lengthSkippingList;
    }


}
