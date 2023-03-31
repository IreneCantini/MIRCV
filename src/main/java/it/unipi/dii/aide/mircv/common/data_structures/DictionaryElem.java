package it.unipi.dii.aide.mircv.common.data_structures;

import it.unipi.dii.aide.mircv.index.SPIMI;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.collection_length;

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

    //inverse document frequencies
    private double idf;

    //Term upper bound for tfidf
    private double maxTFIDF;

    private double maxBM25;

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
        this.idf = 0;
        this.maxTFIDF = 0;
        this.maxBM25 = 0;
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
        this.idf = 0;
        this.maxTFIDF = 0;
        this.maxBM25 = 0;
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
        this.idf = 0;
        this.maxTFIDF = 0;
        this.maxBM25 = 0;
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

    public void setMaxBM25(double maxBM25) {
        this.maxBM25 = maxBM25;
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

    public double getMaxBM25() {
        return maxBM25;
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

    public void setIdf(double idf) {
        this.idf = idf;
    }

    public void setMaxTFIDF(double maxTFIDF) {
        this.maxTFIDF = maxTFIDF;
    }

    public void computeIdf() {
        this.idf = Math.log10(collection_length/(double)this.df);
    }

    public void computeMaxTFIDF() {
        this.maxTFIDF = (1+Math.log10(this.maxTf))*this.idf;
    }

    public void computeMaxBM25(PostingList pl) {
        double current_BM25;
        for (Posting p: pl.getPl())
        {
            current_BM25 = (p.getTermFrequency() / ((1 - 0.75) + 0.75 * (SPIMI.Document_index_map.get(p.getDocID()) / SPIMI.avdl) + p.getTermFrequency()))*this.idf;
            if(current_BM25>this.getMaxBM25()){
                this.setMaxBM25(current_BM25);
            }
        }
    }

    public double getIdf() {
        return idf;
    }

    public double getMaxTFIDF() {
        return maxTFIDF;
    }

    public void printVocabularyEntry(){
        System.out.printf("Document Frequency: %d\nCollection Frequency: %d\nMax Term Frequency: %d\nPostingList docid lenght: %d\n" +
                "PostingList freq: %d\nSkipping length: %d\nIDF: %f\nMax TFIDF: %f\nMax BM25: %f\n",
                this.getDf(), this.getCf(),this.getMaxTf(), this.getDocids_len(), this.getTf_len(),
                this.getSkipInfo_len(), this.getIdf(), this.getMaxTFIDF(), this.getMaxBM25());
    }

    public void writeDictionaryElemToDisk(FileChannel dictFileChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(92);
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
        buffer.putInt(this.maxTf);
        buffer.putLong(this.offset_skipInfo);
        buffer.putInt(this.skipInfo_len);
        buffer.putDouble(this.idf);
        buffer.putDouble(this.maxTFIDF);
        buffer.putDouble(this.maxBM25);

        buffer = ByteBuffer.wrap(buffer.array());

        while(buffer.hasRemaining()) {
            dictFileChannel.write(buffer);
        }
    }

    public void writeDictionaryElemDebugModeToDisk(FileChannel dictFileChannel) throws IOException {
        String dictionary_elem_string = "Term: " + this.term + " df: " + this.df + " cf: " + this.cf + " docids_offset: " + this.offset_docids +
                " docids_len: " + this.docids_len + " freqs_offset: " + this.offset_tf + " freqs_len: " + " maxTf: " + this.maxTf + this.tf_len + "skip_offset: " +
                this.offset_skipInfo + " skip_len: " + this.skipInfo_len + " idf: " + this.idf + " maxTFIDF: " + this.maxTFIDF + " maxBM25: " +
                this.maxBM25;

        BufferedWriter disk_writer = new BufferedWriter(new FileWriter("src/main/resources/Debug/vocabulary_debug.txt", true));
        disk_writer.write(dictionary_elem_string);
        disk_writer.close();

    }


    public void readDictionaryElemFromDisk(long start_position, FileChannel dictFchannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(20);

        dictFchannel.position(start_position);

        while (buffer.hasRemaining()){
            dictFchannel.read(buffer);
        }

        this.setTerm(new String(buffer.array(), StandardCharsets.UTF_8).trim());

        buffer = ByteBuffer.allocate(72);

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
        this.setOffset_skipInfo(buffer.getLong());
        this.setSkipInfo_len(buffer.getInt());
        this.setIdf(buffer.getDouble());
        this.setMaxTFIDF(buffer.getDouble());
        this.setMaxBM25(buffer.getDouble());
    }
}
