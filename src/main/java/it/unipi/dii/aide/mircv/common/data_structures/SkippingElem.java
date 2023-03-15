package it.unipi.dii.aide.mircv.common.data_structures;

public class SkippingElem{
    private long docID; // Massimo DocId del blocco preso in considerazione
    private int block_size; // Lunghezza blocco preso in considerazione


    public SkippingElem(){
        this.docID = 0;
        this.block_size = 0;
    }
    public SkippingElem(long docID, int block_size) {
        this.docID = docID;
        this.block_size = block_size;
    }

    public long getDocID() {
        return docID;
    }

    public int getBlock_size() {
        return block_size;
    }

    public void setDocID(long docID) {
        this.docID = docID;
    }

    public void setBlock_size(int block_size) {
        this.block_size = block_size;
    }
}

