package it.unipi.dii.aide.mircv.common.data_structures;

import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

public class DocumentIndexElem {
    private long docId;
    private String docNo;
    private long length;

    public DocumentIndexElem(){
        this.docId=0;
        this.docNo="";
        this.length=0;
    }

    public DocumentIndexElem(long docId, String docNo, long length) {
        int diffLenght=20-docNo.length();
        if(diffLenght!=0){
            // andiamo a concatenare " " per arrivare alla dimensione fissa di 20 caratteri
            docNo = docNo + " ".repeat(Math.max(0, diffLenght));
        }
        this.docId = docId;
        this.docNo = docNo;
        this.length = length;
    }

    public void setDocId(long docId) {
        this.docId = docId;
    }

    public void setDocNo(String docNo) {
        this.docNo = docNo;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getDocId() {
        return docId;
    }

    public String getDocNo() {
        return docNo;
    }

    public long getLength() {
        return length;
    }

    /**
     * Convertitore da bytes del file Dictionary a un oggetto di tipo Dictionary
     * @param array
     * @return
     */
    public void convertToDocumentObject(MappedByteBuffer array) {

        this.docId= array.slice(0, 8).getLong();
        this.docNo=StandardCharsets.UTF_8.decode(array.slice(8,20)).toString();
        this.length=array.slice(28, 8).getLong();
    }
}
