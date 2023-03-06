package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.basic.data_structures_management.DictionaryElem;
import it.unipi.dii.aide.mircv.basic.data_structures_management.DocumentIndexElem;
import it.unipi.dii.aide.mircv.basic.data_structures_management.PostingList;
import it.unipi.dii.aide.mircv.basic.data_structures_management.PostingListElem;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;

public class FileManagement {


    /* *** FILE CHANNEL VARIABLES *** */

    //Array containing File Channels of the temporary files
    private static ArrayList<FileChannel> listFileChannelsDict;

    private static ArrayList<FileChannel> listFileChannelsDoc;

    private static ArrayList<FileChannel> listFileChannelsFreq;

    private static FileChannel fileChannelIIDoc;
    private static FileChannel fileChannelIIFreq;
    private static FileChannel fileChannelDict;
    private static FileChannel fileChannelDocIndex;

    public FileManagement(){
        listFileChannelsDict=new ArrayList<>();
        listFileChannelsDoc=new ArrayList<>();
        listFileChannelsFreq=new ArrayList<>();
    }

    public static void createFileDocumentIndex() throws IOException {
        String filePath = "src/main/resources/document_index.dat";
        // Inizializziamo il file
        File file = new File(filePath);
        if(!file.createNewFile()){
            System.out.println("Non è stato possibile creare il file del document index");
        }
        else{
            Path path_docI = Path.of("src/main/resources/document_index.dat");

            fileChannelDocIndex=(FileChannel) Files
                    .newByteChannel(path_docI, EnumSet.of(
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE
                    ));
        }
    }


    /**
     * Inserimento delle strutture in memoria sul disco
     * ogni volta che viene chiamata crea i nuovi file temporanei di Dictionary, IIDoc e IIFreq
     * @throws IOException
     */
    public static void insertOnDisk() throws IOException, InterruptedException {

        String filePath = "src/main/resources/Dictionary_" + listFileChannelsDict.size() + ".dat";

        // Inizializziamo il file
        File file = new File(filePath);
        if(!file.createNewFile()){
            System.out.println("Non è stato possibile creare il file del dictionary");
        }

        Path path = Path.of("src/main/resources/Dictionary_" + listFileChannelsDict.size() + ".dat");

        listFileChannelsDict.add((FileChannel) Files
                .newByteChannel(path, EnumSet.of(
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                )));

        filePath = "src/main/resources/II_Doc_" + listFileChannelsDoc.size() + ".dat";

        // Inizializziamo il file
        file = new File(filePath);
        if(!file.createNewFile()){
            System.out.println("Non è stato possibile creare il file dei docID");
        }

        path = Path.of("src/main/resources/II_Doc_" + listFileChannelsDoc.size() + ".dat");

        listFileChannelsDoc.add((FileChannel) Files
                .newByteChannel(path, EnumSet.of(
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                )));

        filePath = "src/main/resources/II_Freq_" + listFileChannelsFreq.size() + ".dat";

        // Inizializziamo il file
        file = new File(filePath);
        if(!file.createNewFile()){
            System.out.println("Non è stato possibile creare il file delle frequenze");
        }

        path = Path.of("src/main/resources/II_Freq_" + listFileChannelsFreq.size() + ".dat");

        listFileChannelsFreq.add((FileChannel) Files
                .newByteChannel(path, EnumSet.of(
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                )));

        // Inserimento dei valori in memoria sui file
        createFile();

        // Pulizia della memoria
        Util.freeMemory();
    }

    /**
     * Creazione del file contenente le liste di docID e frequenza
     * @throws IOException
     */
    public static void createFile() throws IOException {

        int pos;

        for (PostingList term : SPIMI.listTerm) {
            pos=SPIMI.positionTerm.get(term.getTerm());

            SPIMI.listTermDict.get(pos).setOffset_start_doc(listFileChannelsDoc.get(listFileChannelsDoc.size()-1).size());
            SPIMI.listTermDict.get(pos).setOffset_start_freq(listFileChannelsFreq.get(listFileChannelsFreq.size()-1).size());

            for (PostingListElem p: term.getPl()) {
                writeByteToFile(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(p.getDocID()).array(), "docII", 8);
                writeByteToFile(ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(p.getTermFrequency()).array(), "freqII", 4);
            }
        }
        // Ora che sono sicura di aver aggiornato tutte le variabili necessarie posso scrivere il termine nel dizionario
        writeDictionaryToFile();
    }


    /**
     * Creazione dei file finali contenenti tutto il dizionario e i due II derivanti dalla collection
     * @throws IOException
     */

    public static void createNewFiles() throws IOException {
        String filePath = "src/main/resources/dictionary.dat";

        File file = new File(filePath);
        file.createNewFile();

        Path pathII = Path.of("src/main/resources/dictionary.dat");
       fileChannelDict=(FileChannel) Files
                .newByteChannel(pathII, EnumSet.of(
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING
                ));

        filePath = "src/main/resources/inverted_index_doc.dat";
        // Inizializziamo il file
        file = new File(filePath);
        if(!file.createNewFile())
            System.out.println("errore creazione file");

        pathII = Path.of("src/main/resources/inverted_index_doc.dat");
        fileChannelIIDoc=(FileChannel) Files
                .newByteChannel(pathII, EnumSet.of(
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING
                ));

        filePath = "src/main/resources/inverted_index_freq.dat";
        // Inizializziamo il file
        file = new File(filePath);
        file.createNewFile();

        pathII = Path.of("src/main/resources/inverted_index_freq.dat");
        fileChannelIIFreq=(FileChannel) Files
                .newByteChannel(pathII, EnumSet.of(
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING
                ));

        filePath = "src/main/resources/skipping_file.dat";
        // Inizializziamo il file
        file = new File(filePath);
        file.createNewFile();
        /*
        pathII = Path.of("src/main/resources/skipping_file.dat");
        this.fileChannel_skipping=(FileChannel) Files
                .newByteChannel(pathII, EnumSet.of(
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING
                ));

         */
    }


    /**
     * Funzione per svolgere le scritture sui file puntati dai vari filechannel
     * @param b
     * @param where
     * @param endByte
     * @throws IOException
     */
    public static void writeByteToFile(byte[] b, String where, long endByte) throws IOException {

        MappedByteBuffer mappedByteBuffer = switch (where) {
            case "dict" -> listFileChannelsDict.get(listFileChannelsDict.size()-1)
                    .map(FileChannel.MapMode.READ_WRITE, listFileChannelsDict.get(listFileChannelsDict.size()-1).size(), endByte);
            case "docII" -> listFileChannelsDoc.get(listFileChannelsDoc.size()-1)
                    .map(FileChannel.MapMode.READ_WRITE, listFileChannelsDoc.get(listFileChannelsDoc.size()-1).size(), endByte);
            case "freqII" -> listFileChannelsFreq.get(listFileChannelsFreq.size()-1)
                    .map(FileChannel.MapMode.READ_WRITE, listFileChannelsFreq.get(listFileChannelsFreq.size()-1).size(), endByte);
            case "docIndex" -> fileChannelDocIndex
                    .map(FileChannel.MapMode.READ_WRITE, fileChannelDocIndex.size(), endByte);

            case "new_dict" -> fileChannelDict
                    .map(FileChannel.MapMode.READ_WRITE, fileChannelDict.size(), endByte);
            case "docII_new" -> fileChannelIIDoc
                    .map(FileChannel.MapMode.READ_WRITE, fileChannelIIDoc.size(), endByte);
            case "freqII_new" -> fileChannelIIFreq
                    .map(FileChannel.MapMode.READ_WRITE, fileChannelIIFreq.size(), endByte);
            /*
            case "skipping" -> fileChannel_skipping
                    .map(FileChannel.MapMode.READ_WRITE, fileChannel_skipping.size(), endByte);

             */

            default -> null;
        };

        if (mappedByteBuffer != null) {
            mappedByteBuffer.put(b);
        }
    }

    /**
     * Creazione del file contenente il Dictionary
     * @throws IOException
     */
    public static void writeDictionaryToFile() throws IOException {

        // ordino l'array contenente il dizionario
        Util.sort(SPIMI.listTermDict);
        //sortTerm(listTerm);

        // scrivo il dizionario su file
        for(DictionaryElem d: SPIMI.listTermDict)
            writeOneDict(d, "dict");
    }

    /**
     * Scrive un termine del dizionario su disco
     * @param d
     * @param where
     * @throws IOException
     */
    public static void writeOneDict(DictionaryElem d, String where) throws IOException {
        byte[] bytes = d.getTerm().getBytes();
        writeByteToFile(bytes, where, 20);

        bytes = ByteBuffer.allocate(4).putInt(d.getDocumentFrequency()).array();
        writeByteToFile(bytes, where, 4);

        bytes = ByteBuffer.allocate(8).putLong(d.getCollectionFrequency()).array();
        writeByteToFile(bytes,where, 8);

        bytes = ByteBuffer.allocate(8).putLong(d.getOffset_start_doc()).array();
        writeByteToFile(bytes, where, 8);

        bytes = ByteBuffer.allocate(4).putInt(d.getLengthPostingList_doc()).array();
        writeByteToFile(bytes, where, 4);

        bytes = ByteBuffer.allocate(8).putLong(d.getOffset_start_freq()).array();
        writeByteToFile(bytes, where, 8);

        bytes = ByteBuffer.allocate(4).putInt(d.getLengthPostingList_freq()).array();
        writeByteToFile(bytes, where, 4);

        bytes = ByteBuffer.allocate(8).putLong(d.getOffset_start_skipping()).array();
        writeByteToFile(bytes, where, 8);

        bytes = ByteBuffer.allocate(4).putInt(d.getLengthSkippingList()).array();
        writeByteToFile(bytes, where, 4);

    }

    /**
     * Scrive le informazioni di un documento sul document index
     * @param doc
     * @throws IOException
     */
    public static void writeOneDoc(DocumentIndexElem doc) throws IOException {
        // Scrivo docID
        byte[] bytes = ByteBuffer.allocate(8).putLong(doc.getDocId()).array();
        writeByteToFile(bytes, "docIndex", 8);

        // Scrivo docNo
        bytes = doc.getDocNo().getBytes();
        writeByteToFile(bytes, "docIndex", 20);

        // Scrivo doclength
        bytes = ByteBuffer.allocate(8).putLong(doc.getLength()).array();
        writeByteToFile(bytes, "docIndex", 8);
    }


    public static int getSizeFileTempDocs() { return listFileChannelsDoc.size(); }

    public static int getSizeFileTempDicts() {
        System.out.println("Size dicts: " + listFileChannelsDict.size());
        return listFileChannelsDict.size(); }

    public static ArrayList<FileChannel> getDicts() { return listFileChannelsDict; }

    public static ArrayList<FileChannel> getDocs() { return listFileChannelsDoc; }

    public static ArrayList<FileChannel> getFreqs() { return listFileChannelsFreq; }

    public static FileChannel getIIDoc() { return fileChannelIIDoc; }

    public static FileChannel getIIFreq() { return fileChannelIIFreq; }

    public static FileChannel getDict() { return fileChannelDict; }

    public static DictionaryElem convertToDictionaryObject(MappedByteBuffer array) {
        DictionaryElem d = new DictionaryElem();

        d.setTerm(StandardCharsets.UTF_8.decode(array.slice(0,20)).toString());
        d.setDocumentFrequency(array.slice(20, 4).getInt());
        d.setCollectionFrequency(array.slice(24, 8).getLong());
        d.setOffset_start_doc(array.slice(32, 8).getLong());
        d.setLengthPostingList_doc(array.slice(40, 4).getInt());
        d.setOffset_start_freq(array.slice(44, 8).getLong());
        d.setLengthPostingList_freq(array.slice(52, 4).getInt());
        d.setOffset_start_skipping(array.slice(56, 8).getLong());
        d.setLengthSkippingList(array.slice(64, 4).getInt());

        return d;
    }
}

