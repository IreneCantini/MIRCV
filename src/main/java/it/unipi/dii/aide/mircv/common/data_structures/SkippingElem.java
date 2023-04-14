package it.unipi.dii.aide.mircv.common.data_structures;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class SkippingElem{
    private long docID; // Massimo DocId del blocco preso in considerazione
    private long offset_docId;
    private int block_docId_len; // Lunghezza blocco contenente docId preso in considerazione
    private long offset_freq;
    private int block_freq_len; // Lunghezza blocco contenente frequenze preso in considerazione

    public SkippingElem() {
        docID = 0;
        block_docId_len = 0;
        block_freq_len = 0;
        offset_docId = 0;
        offset_freq = 0;
    }

    public SkippingElem(long docID, long offset_docId, int block_docId_len, long offset_freq, int block_freq_len) {
        this.docID = docID;
        this.offset_docId = offset_docId;
        this.block_docId_len = block_docId_len;
        this.offset_freq = offset_freq;
        this.block_freq_len = block_freq_len;
    }

    public long getDocID() {
        return docID;
    }

    public int getBlock_docId_len() {
        return block_docId_len;
    }

    public int getBlock_freq_len() {
        return block_freq_len;
    }

    public long getOffset_docId() {
        return offset_docId;
    }

    public long getOffset_freq() {
        return offset_freq;
    }

    public void setDocID(long docID) {
        this.docID = docID;
    }

    public void setBlock_docId_len(int block_docId_len) {
        this.block_docId_len = block_docId_len;
    }

    public void setBlock_freq_len(int block_freq_len) {
        this.block_freq_len = block_freq_len;
    }

    public void setOffset_docId(long offset_docId) {
        this.offset_docId = offset_docId;
    }

    public void setOffset_freq(long offset_freq) {
        this.offset_freq = offset_freq;
    }

    public void writeSkippingElemToDisk(FileChannel skipFileChannel) throws IOException {
        ByteBuffer skipInfoBuffer = ByteBuffer.allocate(32);
        skipFileChannel.position(skipFileChannel.size());

        //write the skipping elem fields into file
        skipInfoBuffer.putLong(this.docID);
        skipInfoBuffer.putLong(this.offset_docId);
        skipInfoBuffer.putInt(this.block_docId_len);
        skipInfoBuffer.putLong(this.offset_freq);
        skipInfoBuffer.putInt(this.block_freq_len);

        skipInfoBuffer = ByteBuffer.wrap(skipInfoBuffer.array());

        while(skipInfoBuffer.hasRemaining()) {
            skipFileChannel.write(skipInfoBuffer);
        }
    }

    public void writeSkippingElemDebugModeToDisk() throws IOException {
        String skipping_string = "docID: " + this.docID + ", offset_docId: " + this.offset_docId + ", block_docId_len: " + this.block_docId_len +
                ", offset_freq: " + this.offset_freq + ", block_freq_len: " + this.block_freq_len + "\n";

        BufferedWriter disk_writer = new BufferedWriter(new FileWriter("src/main/resources/Debug/skipping_debug.txt", true));
        disk_writer.write(skipping_string);
        disk_writer.close();
    }

    public void readSkippingElemFromDisk(long start_position, FileChannel skipFileChannel) throws IOException {
        ByteBuffer skipInfoBuffer = ByteBuffer.allocate(32);

        skipFileChannel.position(start_position);

        while (skipInfoBuffer.hasRemaining()){
            skipFileChannel.read(skipInfoBuffer);
        }

        skipInfoBuffer.rewind();
        this.setDocID(skipInfoBuffer.getLong());
        this.setOffset_docId(skipInfoBuffer.getLong());
        this.setBlock_docId_len(skipInfoBuffer.getInt());
        this.setOffset_freq(skipInfoBuffer.getLong());
        this.setBlock_freq_len(skipInfoBuffer.getInt());
    }

    public static void writeArraySkippingElemToDisk(ArrayList<SkippingElem> skip_array, FileChannel skipFileChannel) throws IOException {
        for(SkippingElem skip_elem: skip_array){
            skip_elem.writeSkippingElemToDisk(skipFileChannel);
            if(Flags.isDebug_flag())
                skip_elem.writeSkippingElemDebugModeToDisk();
        }

    }

    public void printSkippingElem() {

        System.out.printf("DocId: '%s'\n", this.getDocID());
        System.out.printf("BlockId Len: '%s'\n", this.getBlock_docId_len());
        System.out.printf("BlockFreqLen: '%s'\n", this.getBlock_freq_len());
    }
}

