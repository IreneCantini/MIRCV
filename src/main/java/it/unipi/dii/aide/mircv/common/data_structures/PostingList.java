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

    public void writePostingListToDisk(SkippingElem skip_elem, DictionaryElem d_elem,FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
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

        while (docsByteBuffer.hasRemaining()) {
            docidsFchannel.write(docsByteBuffer);
        }
        while (freqsByteBuffer.hasRemaining()) {
            freqsFchannel.write(freqsByteBuffer);
        }

        //update the posting list lenght in the dictionary elem
        d_elem.incDocidLen(docsByteBuffer.array().length);
        d_elem.incFreqLen(freqsByteBuffer.array().length);

        if(skip_elem != null) {
            //update the posting list lenght in the skipping elem
            skip_elem.setBlock_docId_len(docsByteBuffer.array().length);
            skip_elem.setBlock_freq_len(freqsByteBuffer.array().length);
        }
    }

    public void writeCompressedPostingListToDisk(SkippingElem skip_elem, DictionaryElem d_elem, FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {

        ArrayList<Long> docids = new ArrayList<>();
        ArrayList<Integer> freqs = new ArrayList<>();

        for (Posting p : this.pl) {
            docids.add(p.getDocID());
            freqs.add(p.getTermFrequency());
        }

        byte[] docidsCompressed = VariableByte.fromArrayLongToVariableByte(docids);
        byte[] freqsCompressed = Unary.fromIntToUnary(freqs);

        ByteBuffer docsByteBuffer;
        ByteBuffer freqsByteBuffer;

        docidsFchannel.position(docidsFchannel.size());
        freqsFchannel.position(freqsFchannel.size());

        docsByteBuffer = ByteBuffer.wrap(docidsCompressed);
        freqsByteBuffer = ByteBuffer.wrap(freqsCompressed);

        while (docsByteBuffer.hasRemaining()) {
            docidsFchannel.write(docsByteBuffer);
        }
        while (freqsByteBuffer.hasRemaining()) {
            freqsFchannel.write(freqsByteBuffer);
        }

        //update the posting list lenght in the dictionary elem
        d_elem.incDocidLen(docidsCompressed.length);
        d_elem.incFreqLen(freqsCompressed.length);

        if(skip_elem != null) {
            //update the posting list lenght in the skipping elem
            skip_elem.setBlock_docId_len(docidsCompressed.length);
            skip_elem.setBlock_freq_len(freqsCompressed.length);
        }
    }

    public void readCompressedPostingListFromDisk(DictionaryElem d_elem,FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        ByteBuffer docsByteBuffer = ByteBuffer.allocate(d_elem.getDocids_len());
        ByteBuffer freqsByteBuffer = ByteBuffer.allocate(d_elem.getTf_len());

        docidsFchannel.position(d_elem.getOffset_docids());
        freqsFchannel.position(d_elem.getOffset_tf());

        while(docsByteBuffer.hasRemaining()) {
            docidsFchannel.read(docsByteBuffer);
        }
        while(freqsByteBuffer.hasRemaining()) {
            freqsFchannel.read(freqsByteBuffer);
        }

        docsByteBuffer.rewind();
        freqsByteBuffer.rewind();

        ArrayList<Long> docids = VariableByte.fromVariableByteToLong(docsByteBuffer.array());
        ArrayList<Integer> freqs = Unary.fromUnaryToInt(freqsByteBuffer.array());

        /* test for checking if there are errors of read or decompression. If everything works good the
        following prints will not be executed. */
        if(docids.size()!=freqs.size())
        {
            System.out.println(d_elem.getTerm());
            System.out.println(docids.size() + ", "+ freqs.size());
        }


        Posting p;

        for(int i=0; i<docids.size(); i++){
            p= new Posting(docids.get(i),freqs.get(i));
            this.pl.add(p);
        }
    }

    public void readPostingListFromDisk(DictionaryElem d_elem,FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        ByteBuffer docsByteBuffer = ByteBuffer.allocate(d_elem.getDocids_len());
        ByteBuffer freqByteBuffer = ByteBuffer.allocate(d_elem.getTf_len());

        docidsFchannel.position(d_elem.getOffset_docids());
        freqsFchannel.position(d_elem.getOffset_tf());

        while(docsByteBuffer.hasRemaining()) {
            docidsFchannel.read(docsByteBuffer);
        }
        while(freqByteBuffer.hasRemaining()) {
            freqsFchannel.read(freqByteBuffer);
        }

        docsByteBuffer.rewind();
        freqByteBuffer.rewind();

        for(int i = 0; i< d_elem.getDocids_len()/8; i++){
            this.pl.add(new Posting(docsByteBuffer.getLong(), freqByteBuffer.getInt()));
        }
    }
}
