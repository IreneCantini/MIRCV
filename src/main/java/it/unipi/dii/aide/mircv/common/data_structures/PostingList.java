package it.unipi.dii.aide.mircv.common.data_structures;

import it.unipi.dii.aide.mircv.common.compression.Unary;
import it.unipi.dii.aide.mircv.common.compression.VariableByte;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class PostingList {
    private String term;
    private ArrayList<Posting> pl;

    public PostingList() {
        this.term = " ";
        this.pl = new ArrayList<>();
    }

    public PostingList(String term) {
        this.term = term;
        this.pl = new ArrayList<>();
    }

    public PostingList(String term, Posting p) {
        this.term = term;
        pl = new ArrayList<>();
        pl.add(p);
    }

    public PostingList(String term, ArrayList<Posting> pl) {
        this.term = term;
        this.pl = pl;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setPl(ArrayList<Posting> pl) {
        this.pl = pl;
    }

    public String getTerm() {
        return term;
    }

    public ArrayList<Posting> getPl() {
        return pl;
    }

    public void addPosting(Posting p) {
        pl.add(p);
    }

    public void printPostingList() {
        System.out.println("Posting List:");
        for (Posting p : this.getPl()) {
            System.out.printf("Docid: %d - Freq: %d\n", p.getDocID(), p.getTermFrequency());
        }
    }

   /* public void writePostingListToDisk(FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        MappedByteBuffer docsByteBuffer;
        MappedByteBuffer freqByteBuffer;

        for(Posting p: this.pl){

            docsByteBuffer = docidsFchannel.map(FileChannel.MapMode.READ_WRITE, docidsFchannel.size(), 8);
            freqByteBuffer = freqsFchannel.map(FileChannel.MapMode.READ_WRITE, freqsFchannel.size(), 4);

            if(docsByteBuffer!=null && freqByteBuffer!=null){
                docsByteBuffer.putLong(p.getDocID());
                freqByteBuffer.putInt(p.getTermFrequency());
            }
        }
    } */

    public void writePostingListToDisk(FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        ByteBuffer docsByteBuffer;
        ByteBuffer freqsByteBuffer;

        docidsFchannel.position(docidsFchannel.size());
        freqsFchannel.position(freqsFchannel.size());

        docsByteBuffer = ByteBuffer.allocate(this.pl.size() * 8);
        freqsByteBuffer = ByteBuffer.allocate(this.pl.size() * 4);

        for (Posting p : this.pl) {
            docsByteBuffer.putLong(p.getDocID());
            freqsByteBuffer.putInt(p.getTermFrequency());
        }

        docsByteBuffer = ByteBuffer.wrap(docsByteBuffer.array());
        freqsByteBuffer = ByteBuffer.wrap(freqsByteBuffer.array());

        while (docsByteBuffer.hasRemaining() && freqsByteBuffer.hasRemaining()) {
            docidsFchannel.write(docsByteBuffer);
            freqsFchannel.write(freqsByteBuffer);
        }
    }

    /*public void writeCompressedPostingListToDisk(DictionaryElem d_elem, FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {

        ArrayList<Long> docids = new ArrayList<>();
        ArrayList<Integer> freqs = new ArrayList<>();

        for(Posting p: this.pl){
            docids.add(p.getDocID());
            freqs.add(p.getTermFrequency());
        }

        byte[] docidsCompressed = VariableByte.fromArrayLongToVariableByte(docids);
        byte[] freqsCompressed = Unary.fromIntToUnary(freqs);

        MappedByteBuffer docsByteBuffer = docidsFchannel.map(FileChannel.MapMode.READ_WRITE, docidsFchannel.size(), docidsCompressed.length);
        MappedByteBuffer freqByteBuffer = freqsFchannel.map(FileChannel.MapMode.READ_WRITE, freqsFchannel.size(), freqsCompressed.length);


        if(docsByteBuffer!=null && freqByteBuffer!=null) {
            docsByteBuffer.put(docidsCompressed);
            freqByteBuffer.put(freqsCompressed);

            //update the posting list lenght in the dictionary elem
            d_elem.setDocids_len(docidsCompressed.length);
            d_elem.setTf_len(freqsCompressed.length);
        }
    }*/

    public void writeCompressedPostingListToDisk(DictionaryElem d_elem, FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {

        ArrayList<Long> docids = new ArrayList<>();
        ArrayList<Integer> freqs = new ArrayList<>();

        for (Posting p : this.pl) {
            docids.add(p.getDocID());
            freqs.add(p.getTermFrequency());
        }

        byte[] docidsCompressed = VariableByte.fromArrayLongToVariableByte(docids);
        byte[] freqsCompressed = Unary.fromIntToUnary(freqs);

        ByteBuffer docsByteBuffer = ByteBuffer.allocate(docidsCompressed.length);
        ByteBuffer freqsByteBuffer = ByteBuffer.allocate(freqsCompressed.length);

        docidsFchannel.position(docidsFchannel.size());
        freqsFchannel.position(freqsFchannel.size());

        docsByteBuffer = ByteBuffer.wrap(docidsCompressed);
        freqsByteBuffer = ByteBuffer.wrap(freqsCompressed);

        while (docsByteBuffer.hasRemaining() && freqsByteBuffer.hasRemaining()) {
            docidsFchannel.write(docsByteBuffer);
            freqsFchannel.write(freqsByteBuffer);
        }

        //update the posting list lenght in the dictionary elem
        d_elem.setDocids_len(docidsCompressed.length);
        d_elem.setTf_len(freqsCompressed.length);
    }

    /*public void readCompressedPostingListFromDisk(DictionaryElem d_elem,FileChannel docidsFchannel, FileChannel freqsFchannel) {
        MappedByteBuffer docsByteBuffer;
        MappedByteBuffer freqByteBuffer;

        try {
            docsByteBuffer = docidsFchannel.map(FileChannel.MapMode.READ_ONLY, d_elem.getOffset_docids(), d_elem.getDocids_len());
            freqByteBuffer = freqsFchannel.map(FileChannel.MapMode.READ_ONLY, d_elem.getOffset_tf(), d_elem.getTf_len());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if(docsByteBuffer!=null && freqByteBuffer!=null){
            byte[] temp_docid = new byte[docsByteBuffer.remaining()];
            byte[] temp_freqs = new byte[freqByteBuffer.remaining()];

            docsByteBuffer.get(temp_docid,0,temp_docid.length);
            freqByteBuffer.get(temp_freqs, 0, temp_freqs.length);

            ArrayList<Long> docids = VariableByte.fromVariableByteToLong(temp_docid);
            ArrayList<Integer> freqs = Unary.fromUnaryToInt(temp_freqs);

            Posting p;

            for(int i=0; i<docids.size(); i++){
                p= new Posting(docids.get(i),freqs.get(i));
                this.pl.add(p);
            }
        }
    }*/

    public void readCompressedPostingListFromDisk(DictionaryElem d_elem,FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        ByteBuffer docsByteBuffer = ByteBuffer.allocate(d_elem.getDocids_len());
        ByteBuffer freqsByteBuffer = ByteBuffer.allocate(d_elem.getTf_len());

        docidsFchannel.position(d_elem.getOffset_docids());
        freqsFchannel.position(d_elem.getOffset_tf());

        while(docsByteBuffer.hasRemaining() && freqsByteBuffer.hasRemaining()) {
            docidsFchannel.read(docsByteBuffer);
            freqsFchannel.read(freqsByteBuffer);
        }

        docsByteBuffer.rewind();
        freqsByteBuffer.rewind();

        ArrayList<Long> docids = VariableByte.fromVariableByteToLong(docsByteBuffer.array());
        ArrayList<Integer> freqs = Unary.fromUnaryToInt(freqsByteBuffer.array());

        Posting p;

        for(int i=0; i<docids.size(); i++){
            p= new Posting(docids.get(i),freqs.get(i));
            this.pl.add(p);
        }
    }

    /*public void readPostingListFromDisk(DictionaryElem d_elem,FileChannel docidsFchannel, FileChannel freqsFchannel) {
        MappedByteBuffer docsByteBuffer;
        MappedByteBuffer freqByteBuffer;

        try {
            docsByteBuffer = docidsFchannel.map(FileChannel.MapMode.READ_ONLY, d_elem.getOffset_docids(), d_elem.getDocids_len());
            freqByteBuffer = freqsFchannel.map(FileChannel.MapMode.READ_ONLY, d_elem.getOffset_tf(), d_elem.getTf_len());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if(docsByteBuffer!=null && freqByteBuffer!=null){
            while(docsByteBuffer.hasRemaining() && freqByteBuffer.hasRemaining()) {
                this.pl.add(new Posting(docsByteBuffer.getLong(), freqByteBuffer.getInt()));
            }
        }
    }*/

    public void readPostingListFromDisk(DictionaryElem d_elem,FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        ByteBuffer docsByteBuffer = ByteBuffer.allocate(d_elem.getDocids_len());
        ByteBuffer freqByteBuffer = ByteBuffer.allocate(d_elem.getTf_len());

        docidsFchannel.position(d_elem.getOffset_docids());
        freqsFchannel.position(d_elem.getOffset_tf());

        while(docsByteBuffer.hasRemaining() && freqByteBuffer.hasRemaining()) {
            docidsFchannel.read(docsByteBuffer);
            freqsFchannel.read(freqByteBuffer);
        }

        docsByteBuffer.rewind();
        freqByteBuffer.rewind();

        for(int i = 0; i< d_elem.getDocids_len()/8; i++){
            this.pl.add(new Posting(docsByteBuffer.getLong(), freqByteBuffer.getInt()));
        }
    }
}
