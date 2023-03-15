package it.unipi.dii.aide.mircv.common.data_structures;

public class PostingListElem {
    private long docID;
    private int termFrequency;

    PostingListElem(long docID, int f) {
        this.docID = docID;
        this.termFrequency = f;
    }

    public Integer getTermFrequency() {
        return termFrequency;
    }

    public Long getDocID() {
        return docID;
    }


    /*
    private String term;
    private ArrayList<Long> docs;
    private ArrayList<Integer> termFreq; //memorizza per ogni termine la document frequency
    private int collFreq=0; //memorizza la collection freq

    public Posting(String term,  long docs, Integer termf) {
        this.term = term;
        this.docs=new ArrayList<>();
        this.docs.add(docs);
        this.termFreq=new ArrayList<>();
        this.termFreq.add(termf);
        //this.collFreq+=docf;
    }

    public void addToDocs(long d, Integer f){
        this.docs.add(d);
        this.termFreq.add(f);
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setFreq(int freq) {
        this.collFreq = freq;
    }

    public ArrayList<Long> getDocs() {
        return docs;
    }



    public long getLastElement(){
        return this.docs.get(this.docs.size() - 1);
    }
    */
}
