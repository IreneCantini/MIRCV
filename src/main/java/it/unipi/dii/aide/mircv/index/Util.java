package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.common.compression.Unary;
import it.unipi.dii.aide.mircv.common.compression.VariableByte;
import it.unipi.dii.aide.mircv.common.data_structures.DictionaryElem;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;

public class Util {
    /**
     * Funzione per sortare l'array di Dictionary
     * @param list
     */
    public static void sort(ArrayList<DictionaryElem> list) { list.sort(Comparator.comparing(DictionaryElem::getTerm));}

    /**
     * Funzione per sortare l'array di Posting List
     * @param list
     */
    public void sortTerm(ArrayList<PostingList> list) { list.sort(Comparator.comparing(PostingList::getTerm)); }

    public static void freeMemory(){
        SPIMI.listTerm.clear();
        SPIMI.listTermDict.clear();
        SPIMI.positionTerm.clear();
    }

    public static void printII(FileChannel dict, FileChannel doc, FileChannel freq, FileChannel skip) throws IOException {

        DictionaryElem d = new DictionaryElem();

        int conta = 0;

        long posLettura = 0;

        while (posLettura < dict.size() && conta<100) {

            conta++;
            MappedByteBuffer mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 20);

            if (mappedByteBuffer != null) {
                d.setTerm(StandardCharsets.UTF_8.decode(mappedByteBuffer).toString());
            }
            System.out.println("Termine letto:" + d.getTerm());
            posLettura+=20; //scorro punto di lettura

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 4);

            if (mappedByteBuffer != null) {
                d.setDocumentFrequency(mappedByteBuffer.getInt());
                System.out.println("Document frequency:" + d.getDocumentFrequency());
            }
            posLettura+=4;

            /*
            if(d.getDocumentFrequency()>10)
            {
                posLettura=posLettura+8+8+4+8+4;
                continue;
            }
             */

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 8);

            if (mappedByteBuffer != null) {
                d.setCollectionFrequency(mappedByteBuffer.getLong());
                System.out.println("Collection frequency:" + d.getCollectionFrequency());
            }
            posLettura+=8;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 8);

            if (mappedByteBuffer != null) {
                d.setOffset_start_doc(mappedByteBuffer.getLong());
            }
            posLettura+=8;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 4);

            if (mappedByteBuffer != null) {
                d.setLengthPostingList_doc(mappedByteBuffer.getInt());
            }
            posLettura+=4;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 8);

            if (mappedByteBuffer != null) {
                d.setOffset_start_freq(mappedByteBuffer.getLong());
            }
            posLettura+=8;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 4);

            if (mappedByteBuffer != null) {
                d.setLengthPostingList_freq(mappedByteBuffer.getInt());
            }
            posLettura+=4;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 8);

            if (mappedByteBuffer != null) {
                d.setOffset_start_skipping(mappedByteBuffer.getLong());
                System.out.println("offset skipping: "+d.getOffset_start_skipping());
            }
            posLettura+=8;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 4);

            if (mappedByteBuffer != null) {
                d.setLengthSkippingList(mappedByteBuffer.getInt());
            }
            posLettura+=4;

            mappedByteBuffer = doc.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_doc(), d.getLengthPostingList_doc());

            if (mappedByteBuffer != null) {
                byte[] arr = new byte[mappedByteBuffer.remaining()];
                mappedByteBuffer.get(arr);
                ArrayList<Long> psd = VariableByte.fromVariableByteToLong(arr);
                System.out.println("docII:" + psd);
            }

            mappedByteBuffer = freq.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_freq(), d.getLengthPostingList_freq());

            if (mappedByteBuffer != null) {
                byte[] arr = new byte[mappedByteBuffer.remaining()];
                mappedByteBuffer.get(arr);
                ArrayList<Integer> psd = Unary.fromUnaryToInt(arr);
                System.out.println("freqII:" + psd);
            }
        /*
            if(d.getTerm().equals("2                   ")){
                System.out.println("trovato 1");
                mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping(), 8);
                if(mappedByteBuffer != null){
                    System.out.println("docMin: "+mappedByteBuffer.getLong());
                }
                System.out.println("lenSkippingList: "+d.getLengthSkippingList());
                long posLettura2=8;
                while (posLettura2<d.getLengthSkippingList()){
                    mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping()+posLettura2, 8);
                    if(mappedByteBuffer != null){
                        System.out.println("docSkip: "+mappedByteBuffer.getLong());
                    }
                    posLettura2+=8;

                    mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping()+ posLettura2, 4);
                    if(mappedByteBuffer != null){
                        System.out.println("LenBlocco: "+mappedByteBuffer.getInt());
                    }

                    posLettura2+=4;
                }
            }

            if(d.getLenghtSkippingList()==8){
                System.out.println("Ho solo il docid minimo");
                mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping(), d.getLenghtSkippingList());
                if(mappedByteBuffer != null){
                    System.out.println("docMin: "+mappedByteBuffer.getLong());
                }
            }*/
        }
        System.out.println("size doc: "+doc.size());
        System.out.println("size freq: "+freq.size());
    }

    /*
    public static void printIINoCompression(FileChannel dict, FileChannel doc, FileChannel freq, FileChannel skip) throws IOException {

        DictionaryElem d = new DictionaryElem();

        int conta = 0;

        long posLettura = 0;

        while (posLettura < dict.size() && conta<100) {

            conta++;
            MappedByteBuffer mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 20);

            if (mappedByteBuffer != null) {
                d.setTerm(StandardCharsets.UTF_8.decode(mappedByteBuffer).toString());
            }
            System.out.println("Termine letto:" + d.getTerm());
            posLettura+=20; //scorro punto di lettura

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 4);

            if (mappedByteBuffer != null) {
                d.setDocumentFrequency(mappedByteBuffer.getInt());
                System.out.println("Document frequency:" + d.getDocumentFrequency());
            }
            posLettura+=4;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 8);

            if (mappedByteBuffer != null) {
                d.setCollectionFrequency(mappedByteBuffer.getLong());
                System.out.println("Collection frequency:" + d.getCollectionFrequency());
            }
            posLettura+=8;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 8);

            if (mappedByteBuffer != null) {
                d.setOffset_start_doc(mappedByteBuffer.getLong());
            }
            posLettura+=8;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 4);

            if (mappedByteBuffer != null) {
                d.setLengthPostingList_doc(mappedByteBuffer.getInt());
            }
            posLettura+=4;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 8);

            if (mappedByteBuffer != null) {
                d.setOffset_start_freq(mappedByteBuffer.getLong());
            }
            posLettura+=8;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 4);

            if (mappedByteBuffer != null) {
                d.setLengthPostingList_freq(mappedByteBuffer.getInt());
            }
            posLettura+=4;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 8);

            if (mappedByteBuffer != null) {
                d.setOffset_start_skipping(mappedByteBuffer.getLong());
                System.out.println("offset skipping: "+d.getOffset_start_skipping());
            }
            posLettura+=8;

            mappedByteBuffer = dict.map(FileChannel.MapMode.READ_ONLY, posLettura, 4);

            if (mappedByteBuffer != null) {
                d.setLengthSkippingList(mappedByteBuffer.getInt());
            }
            posLettura+=4;

            mappedByteBuffer = doc.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_doc(), d.getLengthPostingList_doc());

            if (mappedByteBuffer != null) {

                System.out.print("docII: [");

                // Creo l'array temporaneo di docID
                for (int i = 1; i <= d.getLengthPostingList_doc() / 8; i++)
                    System.out.print(mappedByteBuffer.getLong() +", ");

                System.out.println("]");

                mappedByteBuffer.rewind();
            }

            mappedByteBuffer = freq.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_freq(), d.getLengthPostingList_freq());

            System.out.print("freqII: [");
            // Creo l'array temporaneo di freq
            for (int i = 1; i <= d.getLengthPostingList_freq() / 4; i++)
                System.out.print(mappedByteBuffer.getInt()+ ", ");
            System.out.println("]");

            mappedByteBuffer.rewind();

        /*
            if(d.getTerm().equals("2                   ")){
                System.out.println("trovato 1");
                mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping(), 8);
                if(mappedByteBuffer != null){
                    System.out.println("docMin: "+mappedByteBuffer.getLong());
                }
                System.out.println("lenSkippingList: "+d.getLengthSkippingList());
                long posLettura2=8;
                while (posLettura2<d.getLengthSkippingList()){
                    mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping()+posLettura2, 8);
                    if(mappedByteBuffer != null){
                        System.out.println("docSkip: "+mappedByteBuffer.getLong());
                    }
                    posLettura2+=8;

                    mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping()+ posLettura2, 4);
                    if(mappedByteBuffer != null){
                        System.out.println("LenBlocco: "+mappedByteBuffer.getInt());
                    }

                    posLettura2+=4;
                }
            }

            if(d.getLenghtSkippingList()==8){
                System.out.println("Ho solo il docid minimo");
                mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping(), d.getLenghtSkippingList());
                if(mappedByteBuffer != null){
                    System.out.println("docMin: "+mappedByteBuffer.getLong());
                }
            }*//*
        }
        System.out.println("size doc: "+doc.size());
        System.out.println("size freq: "+freq.size());
    }
    */

    public static void printIINoCompression(FileChannel dict, FileChannel doc, FileChannel freq, FileChannel skip) throws IOException {

        DictionaryElem d = new DictionaryElem();

        int conta = 0;

        long posLettura = 0;

        ByteBuffer mappedByteBuffer;

        while (posLettura < dict.size() && conta<100) {

            conta++;
            mappedByteBuffer= ByteBuffer.allocate(20);
            dict.position(posLettura);
            dict.read(mappedByteBuffer);

            d.setTerm(StandardCharsets.UTF_8.decode(mappedByteBuffer).toString());
            mappedByteBuffer.clear();

            System.out.println("Termine letto:" + d.getTerm());
            posLettura+=20; //scorro punto di lettura

            mappedByteBuffer=ByteBuffer.allocate(4);
            dict.position(posLettura);
            dict.read(mappedByteBuffer);

            d.setDocumentFrequency(mappedByteBuffer.getInt());
            System.out.println("Document frequency:" + d.getDocumentFrequency());
            mappedByteBuffer.clear();

            posLettura+=4;

            mappedByteBuffer=ByteBuffer.allocate(8);
            dict.position(posLettura);
            dict.read(mappedByteBuffer);

            d.setCollectionFrequency(mappedByteBuffer.getLong());
            System.out.println("Collection frequency:" + d.getCollectionFrequency());
            mappedByteBuffer.clear();

            posLettura+=8;

            mappedByteBuffer=ByteBuffer.allocate(8);
            dict.position(posLettura);
            dict.read(mappedByteBuffer);

            d.setOffset_start_doc(mappedByteBuffer.getLong());
            mappedByteBuffer.clear();

            posLettura+=8;

            mappedByteBuffer=ByteBuffer.allocate(4);
            dict.position(posLettura);
            dict.read(mappedByteBuffer);

            d.setLengthPostingList_doc(mappedByteBuffer.getInt());
            mappedByteBuffer.clear();

            posLettura+=4;

            mappedByteBuffer=ByteBuffer.allocate(8);
            dict.position(posLettura);
            dict.read(mappedByteBuffer);

            d.setOffset_start_freq(mappedByteBuffer.getLong());
            mappedByteBuffer.clear();

            posLettura+=8;

            mappedByteBuffer=ByteBuffer.allocate(4);
            dict.position(posLettura);
            dict.read(mappedByteBuffer);

            d.setLengthPostingList_freq(mappedByteBuffer.getInt());
            mappedByteBuffer.clear();

            posLettura+=4;

            mappedByteBuffer=ByteBuffer.allocate(8);
            dict.position(posLettura);
            dict.read(mappedByteBuffer);

            d.setOffset_start_skipping(mappedByteBuffer.getLong());
            System.out.println("offset skipping: "+d.getOffset_start_skipping());
            mappedByteBuffer.clear();

            posLettura+=8;

            mappedByteBuffer=ByteBuffer.allocate(4);
            dict.position(posLettura);
            dict.read(mappedByteBuffer);

            d.setLengthSkippingList(mappedByteBuffer.getInt());

            posLettura+=4;

            mappedByteBuffer=ByteBuffer.allocate(d.getLengthPostingList_doc());
            doc.position(d.getOffset_start_doc());
            doc.read(mappedByteBuffer);


            System.out.print("docII: [");

            // Creo l'array temporaneo di docID
            for (int i = 1; i <= d.getLengthPostingList_doc() / 8; i++)
                System.out.print(mappedByteBuffer.getLong() +", ");

            System.out.println("]");

            mappedByteBuffer.clear();

            mappedByteBuffer=ByteBuffer.allocate(d.getLengthPostingList_freq());
            doc.position(d.getOffset_start_freq());
            doc.read(mappedByteBuffer);

            System.out.print("freqII: [");
            // Creo l'array temporaneo di freq
            for (int i = 1; i <= d.getLengthPostingList_freq() / 4; i++)
                System.out.print(mappedByteBuffer.getInt()+ ", ");
            System.out.println("]");

            mappedByteBuffer.clear();

        /*
            if(d.getTerm().equals("2                   ")){
                System.out.println("trovato 1");
                mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping(), 8);
                if(mappedByteBuffer != null){
                    System.out.println("docMin: "+mappedByteBuffer.getLong());
                }
                System.out.println("lenSkippingList: "+d.getLengthSkippingList());
                long posLettura2=8;
                while (posLettura2<d.getLengthSkippingList()){
                    mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping()+posLettura2, 8);
                    if(mappedByteBuffer != null){
                        System.out.println("docSkip: "+mappedByteBuffer.getLong());
                    }
                    posLettura2+=8;

                    mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping()+ posLettura2, 4);
                    if(mappedByteBuffer != null){
                        System.out.println("LenBlocco: "+mappedByteBuffer.getInt());
                    }

                    posLettura2+=4;
                }
            }

            if(d.getLenghtSkippingList()==8){
                System.out.println("Ho solo il docid minimo");
                mappedByteBuffer = skip.map(FileChannel.MapMode.READ_ONLY, d.getOffset_start_skipping(), d.getLenghtSkippingList());
                if(mappedByteBuffer != null){
                    System.out.println("docMin: "+mappedByteBuffer.getLong());
                }
            }*/
        }
        System.out.println("size doc: "+doc.size());
        System.out.println("size freq: "+freq.size());
    }


    public static void formatTime(long start, long end, String operation) {
        int minutes = (int) ((end - start) / (1000 * 60));
        int seconds = (int) ((end - start) / 1000) % 60;
        if (seconds < 10)
            System.out.println(operation + " done in " + minutes + ":0" + seconds + " minutes");
        else
            System.out.println(operation + " done in " + minutes + ":" + seconds + " minutes");
    }

    public static void deleteFile() throws IOException, InterruptedException {
/*
        for(int i = 0; i < FileManagement.getDicts().size(); i++) {
            FileManagement.getDicts().get(i).close();
            FileManagement.getDocs().get(i).close();
            FileManagement.getFreqs().get(i).close();
            deleteFile_utility("Dictionary_"+i);
            deleteFile_utility("II_Doc_"+i);
            deleteFile_utility("II_Freq_"+i);
        }

 */
    }

    private static void deleteFile_utility(String fileName) throws InterruptedException {
        File file = new File("src/main/resources/"+ fileName + ".dat");

        //creiamo questo while per essere sicuri che il file venga eliminato
        while(!file.delete()) {
            Thread.sleep(100);
        }
    }
}
