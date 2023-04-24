package it.unipi.dii.aide.mircv.common.data_structures;

import it.unipi.dii.aide.mircv.cli.utils.UploadDataStructures;
import it.unipi.dii.aide.mircv.common.compression.Unary;
import it.unipi.dii.aide.mircv.common.compression.VariableByte;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.*;

public class PostingList {
    private String term;
    private final ArrayList<Posting> pl;

    /* Term Upper Bound for TF-IDF */
    private double maxTFIDF;

    /* Term Upper Bound for BM25 */
    private double maxBM25;

    /* It is used to go to the next block */
    public Iterator<SkippingElem> skippingElemIterator = null;

    /* It is used to move on to the next posting */
    public Iterator<Posting> postingIterator = null;

    /* Indicates the posting we have advanced to so far using nextPosting() and nextGEQ() */
    private Posting actualPosting;

    /* Indicates the block we have advanced to so far using nextPosting() and nextGEQ() */
    private SkippingElem actualSkippingBlock;

    private RandomAccessFile pl_docId_raf;

    private RandomAccessFile pl_freq_raf;

    public PostingList() {
        this.term = " ";
        this.pl = new ArrayList<>();
        this.actualPosting = null;
    }

    public PostingList(String term) {
        this.term = term;
        this.pl = new ArrayList<>();
    }

    public PostingList(String term, Posting p) {
        this.term = term;
        this.pl = new ArrayList<>();
        this.pl.add(p);
    }

    public void setTerm(String term) {
        this.term = term;
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

    /**
     * Function that write the posting list on the disk without compression
     * @param skip_elem The skipping element that it will write in the appropriate Skipping file
     * @param d_elem The Dictionary element related to the posting list
     * @param docidsFchannel FileChannel related to the docIDs file on disk
     * @param freqsFchannel FileChannel related to the frequencies file on disk
     * @throws IOException if the channel is not found
     */
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

        while (docsByteBuffer.hasRemaining())
            docidsFchannel.write(docsByteBuffer);

        while (freqsByteBuffer.hasRemaining())
            freqsFchannel.write(freqsByteBuffer);

        /* Update the Posting List length in the Dictionary element */
        d_elem.incDocidLen(docsByteBuffer.array().length);
        d_elem.incFreqLen(freqsByteBuffer.array().length);

        if(skip_elem != null) {
            /* Update the Posting List length in the Skipping element */
            skip_elem.setBlock_docId_len(docsByteBuffer.array().length);
            skip_elem.setBlock_freq_len(freqsByteBuffer.array().length);
        }
    }

    /**
     * Function that write the posting list on the disk with compression
     * @param skip_elem The skipping element that it will write in the appropriate Skipping file
     * @param d_elem The Dictionary element related to the posting list
     * @param docidsFchannel FileChannel related to the docIDs file on disk
     * @param freqsFchannel FileChannel related to the frequencies file on disk
     * @throws IOException if the channel is not found
     */
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

        while (docsByteBuffer.hasRemaining())
            docidsFchannel.write(docsByteBuffer);

        while (freqsByteBuffer.hasRemaining())
            freqsFchannel.write(freqsByteBuffer);

        /* Update the Posting List length in the Dictionary element */
        d_elem.incDocidLen(docidsCompressed.length);
        d_elem.incFreqLen(freqsCompressed.length);

        if(skip_elem != null) {
            /* Update the Posting List length in the Skipping element */
            skip_elem.setBlock_docId_len(docidsCompressed.length);
            skip_elem.setBlock_freq_len(freqsCompressed.length);
        }
    }

    /**
     * This function write on the Debug File the entire posting list in DEBUG-MODE
     * @throws IOException if the channel is not found
     */
    public void writePostingListDebugMode() throws IOException {

        boolean first_posting = true;

        StringBuilder pl_string = new StringBuilder(this.getTerm() + " ->");

        for (Posting p : this.pl) {

            if (first_posting) {
                pl_string.append(" ").append(p.getDocID()).append(", ").append(p.getTermFrequency());
                first_posting = false;
            }else {
                pl_string.append(" - ").append(p.getDocID()).append(", ").append(p.getTermFrequency());
            }
        }

        pl_string.append("\n");

        BufferedWriter disk_writer = new BufferedWriter(new FileWriter("src/main/resources/Debug/posting_lists_debug.txt", true));
        disk_writer.write(pl_string.toString());
        disk_writer.close();
    }

    /**
     * Function that read the posting list on the disk with compression
     * @param d_elem The Dictionary element related to the posting list
     * @param docidsFchannel FileChannel related to the docIDs file on disk
     * @param freqsFchannel FileChannel related to the frequencies file on disk
     * @throws IOException if the channel is not found
     */
    public void readCompressedPostingListFromDisk(DictionaryElem d_elem,FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        ByteBuffer docsByteBuffer = ByteBuffer.allocate(d_elem.getDocids_len());
        ByteBuffer freqsByteBuffer = ByteBuffer.allocate(d_elem.getTf_len());
        Posting p;

        docidsFchannel.position(d_elem.getOffset_docids());
        freqsFchannel.position(d_elem.getOffset_tf());

        while (docsByteBuffer.hasRemaining())
            docidsFchannel.read(docsByteBuffer);

        while (freqsByteBuffer.hasRemaining())
            freqsFchannel.read(freqsByteBuffer);

        docsByteBuffer.rewind();
        freqsByteBuffer.rewind();

        ArrayList<Long> docids = VariableByte.fromVariableByteToLong(docsByteBuffer.array());
        ArrayList<Integer> freqs = Unary.fromUnaryToInt(freqsByteBuffer.array());

        if (docids.size() != freqs.size()) {
            System.out.println(d_elem.getTerm());
            System.out.println(docids.size() + ", "+ freqs.size());
        }

        for (int i = 0; i < docids.size(); i++){
            p = new Posting(docids.get(i), freqs.get(i));
            this.pl.add(p);
        }
    }

    /**
     * Function that read the posting list on the disk without compression
     * @param d_elem The Dictionary element related to the posting list
     * @param docidsFchannel FileChannel related to the docIDs file on disk
     * @param freqsFchannel FileChannel related to the frequencies file on disk
     * @throws IOException if the channel is not found
     */
    public void readPostingListFromDisk(DictionaryElem d_elem,FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {

        ByteBuffer docsByteBuffer = ByteBuffer.allocate(d_elem.getDocids_len());
        ByteBuffer freqByteBuffer = ByteBuffer.allocate(d_elem.getTf_len());

        docidsFchannel.position(d_elem.getOffset_docids());
        freqsFchannel.position(d_elem.getOffset_tf());

        while (docsByteBuffer.hasRemaining())
            docidsFchannel.read(docsByteBuffer);

        while (freqByteBuffer.hasRemaining())
            freqsFchannel.read(freqByteBuffer);

        docsByteBuffer.rewind();
        freqByteBuffer.rewind();

        for (int i = 0; i < d_elem.getDocids_len()/8; i++)
            this.pl.add(new Posting(docsByteBuffer.getLong(), freqByteBuffer.getInt()));
    }

    /**
     * Function that read the posting list on the disk with skipping information
     * @param skip skipping element to read
     * @param docidsFchannel FileChannel related to the docIDs file on disk
     * @param freqsFchannel FileChannel related to the frequencies file on disk
     * @throws IOException if the channel is not found
     */
    public void readPostingListFromDiskWithSkipping(SkippingElem skip, FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        ByteBuffer docsByteBuffer = ByteBuffer.allocate(skip.getBlock_docId_len());
        ByteBuffer freqByteBuffer = ByteBuffer.allocate(skip.getBlock_freq_len());

        docidsFchannel.position(skip.getOffset_docId());
        freqsFchannel.position(skip.getOffset_freq());

        while (docsByteBuffer.hasRemaining())
            docidsFchannel.read(docsByteBuffer);

        while (freqByteBuffer.hasRemaining())
            freqsFchannel.read(freqByteBuffer);

        docsByteBuffer.rewind();
        freqByteBuffer.rewind();

        for (int i = 0; i < skip.getBlock_docId_len()/8; i++){
            this.pl.add(new Posting(docsByteBuffer.getLong(), freqByteBuffer.getInt()));
        }
    }

    /**
     * Function that read the posting list on the disk with skipping information
     * @param skip skipping element to read
     * @param docidsFchannel FileChannel related to the docIDs file on disk
     * @param freqsFchannel FileChannel related to the frequencies file on disk
     * @throws IOException if the channel is not found
     */
    public void readCompressedPostingListFromDiskWithSkipping(SkippingElem skip, FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        ByteBuffer docsByteBuffer = ByteBuffer.allocate(skip.getBlock_docId_len());
        ByteBuffer freqsByteBuffer = ByteBuffer.allocate(skip.getBlock_freq_len());

        docidsFchannel.position(skip.getOffset_docId());
        freqsFchannel.position(skip.getOffset_freq());

        while (docsByteBuffer.hasRemaining())
            docidsFchannel.read(docsByteBuffer);

        while (freqsByteBuffer.hasRemaining())
            freqsFchannel.read(freqsByteBuffer);


        docsByteBuffer.rewind();
        freqsByteBuffer.rewind();

        ArrayList<Long> docids = VariableByte.fromVariableByteToLong(docsByteBuffer.array());
        ArrayList<Integer> freqs = Unary.fromUnaryToInt(freqsByteBuffer.array());

        Posting p;

        for (int i = 0; i < docids.size(); i++){
            p = new Posting(docids.get(i), freqs.get(i));
            this.pl.add(p);
        }
    }

    /**
     * Function that retrieves the posting list for the term
     * @param term Term in the query
     * @throws IOException if the channel is not found
     */
    public void obtainPostingList(String term) throws IOException {

        initRandomFileChannels();

        DictionaryElem d = UploadDataStructures.Dictionary.get(term);

        if (d == null) {
            System.out.println("'" + term + "' : not present");
            return;
        }

        /* I read the skipping information and read the first block */
        RandomAccessFile skipping_raf = new RandomAccessFile(PATH_TO_SKIPPING_FILE, "r");

        /* The term contains no blocks as it has the posting list < 1024 */
        /* Number of blocks of the term */
        ArrayList<SkippingElem> blocks;
        if (d.getSkipInfo_len() == 0) {

            blocks = new ArrayList<>();

            /* Still create the information related to the first block that will contain the entire posting list */
            actualSkippingBlock= new SkippingElem(0, d.getOffset_docids(), d.getDocids_len(), d.getOffset_tf(), d.getTf_len());
            blocks.add(actualSkippingBlock);

            skippingElemIterator = blocks.iterator();
            skippingElemIterator.next();

            if (Flags.isCompression_flag())
                readPostingListFromDiskWithSkipping(blocks.get(0), pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            else
                readCompressedPostingListFromDiskWithSkipping(blocks.get(0), pl_docId_raf.getChannel(), pl_freq_raf.getChannel());

            actualSkippingBlock.setDocID(this.getPl().get(this.getPl().size()-1).getDocID());

        }else {

            /* Initial address from where it reads the skipping information of the term in the Dictionary d */
            long startSkipping = d.getOffset_skipInfo();

            /* Size of a skippingElem descriptor */
            long stepSkipping = 32;

            /* Total size of skipping information (basically the number of blocks) */
            long sizeSkipping = d.getSkipInfo_len() / stepSkipping;

            blocks = new ArrayList<>();

            for (long i = 0; i < sizeSkipping; i++) {
                SkippingElem skipElem = new SkippingElem();
                skipElem.readSkippingElemFromDisk(startSkipping + (i * stepSkipping), skipping_raf.getChannel());
                blocks.add(skipElem);
            }

            /* Initialize the current block */
            actualSkippingBlock = blocks.get(0);

            /* Initialize the skippingElem iterator */
            skippingElemIterator = blocks.iterator();
            skippingElemIterator.next();

            /* Reading the first block and initializing the iterator for the posting list */
            if (Flags.isCompression_flag())
                readPostingListFromDiskWithSkipping(blocks.get(0), pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            else
                readCompressedPostingListFromDiskWithSkipping(blocks.get(0), pl_docId_raf.getChannel(), pl_freq_raf.getChannel());

        }

        /* Setting Posting iterator */
        postingIterator = pl.iterator();
        actualPosting = postingIterator.next();

        if (Flags.isScoreMode())
            this.maxBM25 = d.getMaxBM25();
        else
            this.maxTFIDF = d.getMaxTFIDF();
    }

    /**
     * Function that allows to get the next posting in the posting list
     * @throws IOException if the channel is not found
     */
    public void nextPosting() throws IOException {

        /* Got to the bottom of the posting list of the current block */
        if (!postingIterator.hasNext()) {

            /* No blocks */
            if (skippingElemIterator == null) {
                actualPosting = null;
                return;
            }

            /* Finished the blocks, don't have to do anything else */
            if (!skippingElemIterator.hasNext()) {
                actualPosting = null;
                return;
            }

            /* Read a new block and update the posting list iterator with the new one */
            SkippingElem newBlock = skippingElemIterator.next();

            /* Empty the array containing the previous posting list */
            pl.clear();

            if (Flags.isCompression_flag())
                readPostingListFromDiskWithSkipping(newBlock, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            else
                readCompressedPostingListFromDiskWithSkipping(newBlock, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());

            postingIterator = pl.iterator();
        }

        actualPosting = postingIterator.next();
    }

    /**
     * Function that initializes the FileChannel
     * @throws FileNotFoundException if the file is not found
     */
    private void initRandomFileChannels() throws FileNotFoundException {
        pl_docId_raf = new RandomAccessFile(PATH_TO_DOCIDS_POSTINGLIST, "r");
        pl_freq_raf = new RandomAccessFile(PATH_TO_FREQ_POSTINGLIST, "r");
    }

    /**
     * Function that skip directly to the next posting with the docID passed like a parameter
     * @param docID where the function has to skip
     * @throws IOException if the channel is not found
     */
    public void nextGEQ(long docID) throws IOException {

        boolean newBlock = false;

        /* Need to see if the maximum docId of the current block is less than the docId I am trying to jump to
        because if it's less than that I go on to analyze the next one.
        */
        while (actualSkippingBlock.getDocID() < docID) {
            /* Change block */
            if (!skippingElemIterator.hasNext()) {
                /* No blocks */
                actualPosting = null;
                return;
            }

            /* Update the current block with the next block */
            actualSkippingBlock = skippingElemIterator.next();
            newBlock = true;
        }

        /* If there's new block */
        if (newBlock) {
            pl.clear();

            if (Flags.isCompression_flag())
                readPostingListFromDiskWithSkipping(actualSkippingBlock, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            else
                readCompressedPostingListFromDiskWithSkipping(actualSkippingBlock, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());

            postingIterator = pl.iterator();

            actualPosting = postingIterator.next();
        }

        while (postingIterator.hasNext() && actualPosting.getDocID() < docID)
            actualPosting = postingIterator.next();
    }

    public Posting getActualPosting() { return actualPosting; }

    public double getMaxTFIDF() {
        return maxTFIDF;
    }

    public double getMaxBM25() {
        return maxBM25;
    }
}
