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
    private ArrayList<Posting> pl;

    //Term upper bound for tfidf
    private double maxTFIDF;

    private double maxBM25;

    // Serve per passare al blocco successivo: anzichè tenere un indice mano a mano che scorro i blocchi, con l'iterator
    // posso chiamare la next() e mi restituisce il (elemento) blocco successivo
    public Iterator<SkippingElem> skippingElemIterator = null;

    public Iterator<Posting> postingIterator = null;

    private ArrayList<SkippingElem> blocks = null;

    // Indica il posting a cui siamo avanzati finora usando la next e la nextGeq
    private Posting actualPosting;

    // Mi serve per tenere traccia del blocco corrente
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

    public PostingList(String term, ArrayList<Posting> pl) {
        this.term = term;
        this.pl = new ArrayList<>();
        this.pl.addAll(pl);
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

    public void writePostingListDebugMode() throws IOException {
        boolean first_posting = true;

        String pl_string = this.getTerm().toString() + " ->";


        for (Posting p : this.pl) {
            if(first_posting) {
                pl_string = pl_string + " " + p.getDocID() + ", " + p.getTermFrequency();
                first_posting = false;
            }else {
                pl_string = pl_string + " - " + p.getDocID() + ", " + p.getTermFrequency();
            }
        }

        pl_string = pl_string + "\n";

        BufferedWriter disk_writer = new BufferedWriter(new FileWriter("src/main/resources/Debug/posting_lists_debug.txt", true));
        disk_writer.write(pl_string);
        disk_writer.close();
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

    public void readPostingListFromDiskWithSkipping(SkippingElem skip, FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        ByteBuffer docsByteBuffer = ByteBuffer.allocate(skip.getBlock_docId_len());
        ByteBuffer freqByteBuffer = ByteBuffer.allocate(skip.getBlock_freq_len());

        docidsFchannel.position(skip.getOffset_docId());
        freqsFchannel.position(skip.getOffset_freq());

        while(docsByteBuffer.hasRemaining()) {
            docidsFchannel.read(docsByteBuffer);
        }
        while(freqByteBuffer.hasRemaining()) {
            freqsFchannel.read(freqByteBuffer);
        }

        docsByteBuffer.rewind();
        freqByteBuffer.rewind();

        for(int i = 0; i< skip.getBlock_docId_len()/8; i++){
            this.pl.add(new Posting(docsByteBuffer.getLong(), freqByteBuffer.getInt()));
        }
    }

    public void readCompressedPostingListFromDiskWithSkipping(SkippingElem skip, FileChannel docidsFchannel, FileChannel freqsFchannel) throws IOException {
        ByteBuffer docsByteBuffer = ByteBuffer.allocate(skip.getBlock_docId_len());
        ByteBuffer freqsByteBuffer = ByteBuffer.allocate(skip.getBlock_freq_len());

        docidsFchannel.position(skip.getOffset_docId());
        freqsFchannel.position(skip.getOffset_freq());

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

        Posting p;

        for(int i=0; i<docids.size(); i++){
            p = new Posting(docids.get(i),freqs.get(i));
            this.pl.add(p);
        }
    }

    public void obtainPostingListMaxScore(String term) throws IOException {
        //RandomAccessFile to read postinglist
        initRandomFileChannels();
        /*
        RandomAccessFile pl_docId_raf = new RandomAccessFile(PATH_TO_DOCIDS_POSTINGLIST, "r");
        RandomAccessFile pl_freq_raf = new RandomAccessFile(PATH_TO_FREQ_POSTINGLIST, "r");

         */

        // Ricerca nel dizionario delle informazioni relative al termine
        //DictionaryElem d = dictionaryBinarySearch(term);
        DictionaryElem d = UploadDataStructures.Dictionary.get(term);
        if (d == null)
        {
            System.out.println("Termine non presente");
            return;
        }

        // Leggo le informazione dello skipping e leggo il primo blocco
        RandomAccessFile skipping_raf = new RandomAccessFile(PATH_TO_SKIPPING_FILE, "r");

        // Il termine non contiene blocchi in quanto ha la posting list < 1024
        if (d.getSkipInfo_len() == 0) {

            if(Flags.isCompression_flag())
                readPostingListFromDisk(d, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            else
                readCompressedPostingListFromDisk(d, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());

        } else {

            // Indirizzo inziale da dove leggo le informazioni dello skipping del termine presente nel dizionario d
            long startSkipping = d.getOffset_skipInfo();

            // Dimensione di un descrittore dello skippingElem
            long stepSkipping = 32;

            // Dimensione totale delle informazioni di skipping (in pratica il numero di blocchi)
            long sizeSkipping = d.getSkipInfo_len() / stepSkipping;

            blocks = new ArrayList<>();

            for (long i = 0; i < sizeSkipping; i++) {
                SkippingElem skipElem = new SkippingElem();
                skipElem.readSkippingElemFromDisk(startSkipping + (i * stepSkipping), skipping_raf.getChannel());
                blocks.add(skipElem);
            }

            // Inizializzo il blocco corrente
            actualSkippingBlock = blocks.get(0);

            // Inizializzo l'iteratore degli skippingElem
            skippingElemIterator = blocks.iterator();
            skippingElemIterator.next();

            // Lettura del primo blocco e inizializzazione dell'iteratore per la posting list (in modo da usare le funzioni
            // hasNext() e next() fornite dalla classe Iterator
            if (Flags.isCompression_flag()) {
                readPostingListFromDiskWithSkipping(blocks.get(0), pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            }else {
                readCompressedPostingListFromDiskWithSkipping(blocks.get(0), pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            }
        }

        // Setto l'iteratore
        postingIterator = pl.iterator();

        // Inizializzo il Posting corrente
        actualPosting = postingIterator.next();

        if(Flags.isScoreMode())
            this.maxBM25 = d.getMaxBM25();
        else
            this.maxTFIDF = d.getMaxTFIDF();
    }

    public void obtainPostingListDAAT(String term) throws IOException {

        //RandomAccessFile to read postinglist
        RandomAccessFile pl_docId_raf = new RandomAccessFile(PATH_TO_DOCIDS_POSTINGLIST, "r");
        RandomAccessFile pl_freq_raf = new RandomAccessFile(PATH_TO_FREQ_POSTINGLIST, "r");

        // Ricerca nel dizionario delle informazioni relative al termine
        //DictionaryElem d = dictionaryBinarySearch(term);
        DictionaryElem d = UploadDataStructures.Dictionary.get(term);
        if (d == null)
        {
            System.out.println("Termine non presente");
            return;
        }

        // Prelievo della posting list con docID e Freq
        if(Flags.isCompression_flag()){
            readPostingListFromDisk(d, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
        }else {
            readCompressedPostingListFromDisk(d, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
        }

        if(Flags.isScoreMode())
            this.maxBM25 = d.getMaxBM25();
        else
            this.maxTFIDF = d.getMaxTFIDF();

        // Setto l'iteratore
        postingIterator = pl.iterator();

        // Inizializzo il Posting corrente
        actualPosting = postingIterator.next();

    }

    public void nextPosting() throws IOException {

        // Sono arrivato in fondo alla posting list del blocco corrente
        if (!postingIterator.hasNext()) {

            // Se sono nel caso in cui non ho nessun blocco, mi fermo (questo if si può mettere come condizione in OR
            // nell'if sottostante. L'ho messo qui per chiarezza.
            if (skippingElemIterator == null) {
                actualPosting = null;
                return;
            }

            // Se ho finito anche i blocchi non devo fare altro
            if (!skippingElemIterator.hasNext()) {
                actualPosting = null;
                return;
            }

            // Leggo un nuovo blocco e aggiorno l'iteratore della posting list con quella nuova
            SkippingElem newBlock = skippingElemIterator.next();

            // Svuoto l'array contenente la posting list precedente
            pl.clear();

            if (Flags.isCompression_flag()) {
                readPostingListFromDiskWithSkipping(newBlock, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            }else {
                readCompressedPostingListFromDiskWithSkipping(newBlock, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            }

            postingIterator = pl.iterator();
        }

        actualPosting = postingIterator.next();

    }

    private void initRandomFileChannels() throws FileNotFoundException {
        pl_docId_raf = new RandomAccessFile(PATH_TO_DOCIDS_POSTINGLIST, "r");
        pl_freq_raf = new RandomAccessFile(PATH_TO_FREQ_POSTINGLIST, "r");
    }

    public void nextGEQ(long docID) throws IOException {

        boolean newBlock = false;

        // Per prima cosa devo vedere se il massimo docId del blocco corrente è minore del docId a cui sto cercando di
        // saltare perchè se è minore passo ad analizzare quello dopo.
        while (actualSkippingBlock.getDocID() < docID) {
            // Allora devo cambiare blocco
            if (!skippingElemIterator.hasNext()) {
                // Ho finito tutti i blocchi
                actualPosting = null;
                return;
            }

            // Aggiorno il blocco corrente con il prossimo blocco
            actualSkippingBlock = skippingElemIterator.next();
            newBlock = true;
        }

        if (newBlock) {
            // Allora devo aggiornare tutti gli iteratori
            pl.clear();

            /*
            RandomAccessFile pl_docId_raf = new RandomAccessFile(PATH_TO_DOCIDS_POSTINGLIST, "r");
            RandomAccessFile pl_freq_raf = new RandomAccessFile(PATH_TO_FREQ_POSTINGLIST, "r");

             */

            if (Flags.isCompression_flag()) {
                readPostingListFromDiskWithSkipping(actualSkippingBlock, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            }else {
                readCompressedPostingListFromDiskWithSkipping(actualSkippingBlock, pl_docId_raf.getChannel(), pl_freq_raf.getChannel());
            }

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

    public void setMaxTFIDF(double maxTFIDF) {
        this.maxTFIDF = maxTFIDF;
    }

    public double getMaxBM25() {
        return maxBM25;
    }

    public void setMaxBM25(double maxBM25) {
        this.maxBM25 = maxBM25;
    }
}
