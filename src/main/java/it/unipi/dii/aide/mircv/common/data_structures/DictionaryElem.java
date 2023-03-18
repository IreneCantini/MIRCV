package it.unipi.dii.aide.mircv.common.data_structures;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class DictionaryElem {
    private String term;

    //number of documents in which the term appears at least once
    private int df;

    //number of times the term appears in the collection
    private int cf;

    //offset of the docids posting list
    private long offset_docids;

    //lenght of the docids posting list
    private int docids_len;

    //offset of the term frequencies posting list
    private long offset_tf;

    //length of the term frequencies posting list c
    private int tf_len;

    //maximum term frequency of the term
    private int maxTf;

    //offset of the skipping information
    private long offset_skipInfo;

    //length of the skipping information
    private int skipInfo_len;

    //default constructor (empty element)
    public DictionaryElem(){
        this.term = " ";
        this.df = 0;
        this.cf = 0;
        this.offset_docids = 0;
        this.docids_len = 0;
        this.offset_tf = 0;
        this.tf_len = 0;
        this.maxTf = 0;
        this.offset_skipInfo = 0;
        this.skipInfo_len = 0;
    }

    //constructor for the vocabulary entry for the term passed as parameter
    public DictionaryElem(String term) {
        this.term = term;
        this.df = 0;
        this.cf = 0;
        this.offset_docids = 0;
        this.docids_len = 0;
        this.offset_tf = 0;
        this.tf_len = 0;
        this.maxTf = 0;
        this.offset_skipInfo = 0;
        this.skipInfo_len = 0;
    }

    public DictionaryElem(String term, int df, int cf) {
        this.term = term;
        this.df = df;
        this.cf = cf;
        this.offset_docids = 0;
        this.docids_len = 0;
        this.offset_tf = 0;
        this.tf_len = 0;
        this.maxTf = 0;
        this.offset_skipInfo = 0;
        this.skipInfo_len = 0;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public void setCf(int cf) {
        this.cf = cf;
    }

    public void setOffset_docids(long offset_docids) {
        this.offset_docids = offset_docids;
    }

    public void setDocids_len(int docids_len) {
        this.docids_len = docids_len;
    }

    public void setOffset_tf(long offset_tf) {
        this.offset_tf = offset_tf;
    }

    public void setTf_len(int tf_len) {
        this.tf_len = tf_len;
    }

    public String getTerm() {
        return term;
    }

    public int getDf() {
        return df;
    }

    public int getCf() {
        return cf;
    }

    public long getOffset_docids() {
        return offset_docids;
    }

    public int getDocids_len() {
        return docids_len;
    }

    public long getOffset_tf() {
        return offset_tf;
    }

    public int getTf_len() {
        return tf_len;
    }

    public void incDf(){
        this.df++;
    }

    public void incDf(int n){
        this.df = this.df + n;
    }

    public void incCf(int n){
        this.cf = this.cf + n;
    }

    public void incDocidLen(int n){
        this.docids_len = this.docids_len + n;
    }

    public void incFreqLen(int n){
        this.tf_len = this.tf_len + n;
    }

    public void updateMaxTf(PostingList list) {

        //for each element of the intermediate posting list
        for (Posting posting : list.getPl()) {

            // update the max term frequency
            if (posting.getTermFrequency() > this.maxTf)
                this.maxTf = posting.getTermFrequency();
        }
    }

    public void setMaxTf(int maxTf) { this.maxTf = maxTf; }

    public int getMaxTf() { return this.maxTf; }

    public void setOffset_skipInfo(long offset_skipInfo) {
        this.offset_skipInfo = offset_skipInfo;
    }

    public void setSkipInfo_len(int skipInfo_len) {
        this.skipInfo_len = skipInfo_len;
    }

    public long getOffset_skipInfo() {
        return offset_skipInfo;
    }

    public int getSkipInfo_len() {
        return skipInfo_len;
    }

    public void incSkipInfo_len(){
        this.skipInfo_len += 32;
    }

    public void printVocabularyEntry(){
        System.out.printf("Document Frequency: %d\nCollection Frequency: %d\nMax Term Frequency: %d\nPostingList docid lenght: %d\nPostingList freq: %d\n", this.getDf(), this.getCf(),this.getMaxTf(), this.getDocids_len(), this.getTf_len());
    }

    public void writeDictionaryElemToDisk(FileChannel dictFileChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(56);
        dictFileChannel.position(dictFileChannel.size());

        CharBuffer charBuffer = CharBuffer.allocate(20);
        for(int i = 0; i<this.term.length(); i++)
            charBuffer.put(i, this.term.charAt(i));

        //write the dictionary elem fields into file
        buffer.put(StandardCharsets.UTF_8.encode(charBuffer));
        buffer.putInt(this.df);
        buffer.putInt(this.cf);
        buffer.putLong(this.offset_docids);
        buffer.putInt(this.docids_len);
        buffer.putLong(this.offset_tf);
        buffer.putInt(this.tf_len);
        buffer.putInt(this.getMaxTf());

        buffer = ByteBuffer.wrap(buffer.array());

        while(buffer.hasRemaining()) {
            dictFileChannel.write(buffer);
        }
    }

    public void readDictionaryElemFromDisk(long start_position, FileChannel dictFchannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(20);

        dictFchannel.position(start_position);

        while (buffer.hasRemaining()){
            dictFchannel.read(buffer);
        }

        this.setTerm(new String(buffer.array(), StandardCharsets.UTF_8).trim());

        buffer = ByteBuffer.allocate(36);

        while (buffer.hasRemaining()){
            dictFchannel.read(buffer);
        }

        buffer.rewind();
        this.setDf(buffer.getInt());
        this.setCf(buffer.getInt());
        this.setOffset_docids(buffer.getLong());
        this.setDocids_len(buffer.getInt());
        this.setOffset_tf(buffer.getLong());
        this.setTf_len(buffer.getInt());
        this.setMaxTf(buffer.getInt());
    }
}
