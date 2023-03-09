package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.basic.compression.Unary;
import it.unipi.dii.aide.mircv.basic.compression.VariableByte;
import it.unipi.dii.aide.mircv.basic.data_structures_management.DictionaryElem;
import it.unipi.dii.aide.mircv.basic.data_structures_management.min_heap.Comparator;
import it.unipi.dii.aide.mircv.basic.data_structures_management.min_heap.OrderedList;
import static it.unipi.dii.aide.mircv.index.FileManagement.*;


import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.EnumSet;
import java.util.PriorityQueue;

public class Merger {

    // array contenente, per ogni file temporaneo di dictionary, l'offset al quale siamo arrivati
    private static long[] offsetDict;

    private static int numTempFiles;

    // array dei fileChannel relativi ai files temporanei
    private static ArrayList<FileChannel> arrayTempDict;

    private static ArrayList<FileChannel> arrayTempDocs;

    private static ArrayList<FileChannel> arrayTempFreqs;

    // variabili contenenti i fileChannel dei nuovi files
    private static FileChannel finalIIDocs;

    private static FileChannel finalIIFreqs;

    private static FileChannel finalDict;


    public Merger() {

        numTempFiles = FileManagement.getSizeFileTempDicts();

        offsetDict = new long[numTempFiles];

        arrayTempDict = FileManagement.getDicts();

        arrayTempDocs = FileManagement.getDocs();

        arrayTempFreqs = FileManagement.getFreqs();

    }

    private static void setFinalFileChannels() {

        finalIIDocs = FileManagement.getIIDoc();

        finalIIFreqs = FileManagement.getIIFreq();

        finalDict = FileManagement.getDict();
    }

    public static void mergeFiles() throws IOException {

        boolean firstIteration = true;

        // Dichiarazione variabili per la gestione del merger
        DictionaryElem currentD = new DictionaryElem();
        DictionaryElem prevD = new DictionaryElem();

        ArrayList<Integer> currentFreqList= new ArrayList<>();
        ArrayList<Integer> prevFreqList= new ArrayList<>();

        ArrayList<Long> currentDocList= new ArrayList<>();
        ArrayList<Long> prevDocList= new ArrayList<>();

        MappedByteBuffer mappedByteBuffer;

        OrderedList ol;

        // priority queue per mantenere in ordine alfabetico i termini prelevati dai vari blocchi
        PriorityQueue<OrderedList> pQueue
                = new PriorityQueue<>(numTempFiles, new Comparator());


        // Creazione dei file finali
        FileManagement.createNewFiles();
        // Assegnazione FileChannel
        setFinalFileChannels();

        // Inserisco nella priority queue tutti i primi termini dei file dictionary temporanei
        for (int i = 0; i < numTempFiles; i++) {

            mappedByteBuffer = arrayTempDict.get(i).map(FileChannel.MapMode.READ_ONLY, offsetDict[i], 20);

            if (mappedByteBuffer != null)
                currentD.setTerm(StandardCharsets.UTF_8.decode(mappedByteBuffer).toString());

            // creo nuovo elemento della priority queue
            ol = new OrderedList(currentD.getTerm(), i);
            pQueue.add(ol);
        }

        byte[] temp;
        // ciclo fino a quando la priority queue non è vuota
        // (cioè fino a che non ho visitato tutti i termini dei vari file temporanei)
        while (!pQueue.isEmpty()) {

            // prendo l'elemento minore
            ol = pQueue.poll();

            // leggo 68 byte perchè sono i byte occupati da un elemento dictionary (68 con skipping)
            mappedByteBuffer = arrayTempDict.get(ol.getIndex()).map(FileChannel.MapMode.READ_ONLY, offsetDict[ol.getIndex()], 56);

            currentD = FileManagement.convertToDictionaryObject(mappedByteBuffer);

            // con skipping += 68
            offsetDict[ol.getIndex()] += 56;

            //vado a leggere nel file corretto contenente i documenti alla posizione offset
            mappedByteBuffer = arrayTempDocs.get(ol.getIndex()).map(FileChannel.MapMode.READ_ONLY, currentD.getOffset_start_doc(), currentD.getLengthPostingList_doc());

            if (mappedByteBuffer != null) {

                // setto il nuovo offset di partenza
                currentD.setOffset_start_doc(finalIIDocs.size());

                // Creo l'array temporaneo di docID
                for (int i = 1; i <= currentD.getLengthPostingList_doc() / 8; i++)
                    currentDocList.add(mappedByteBuffer.getLong());

                mappedByteBuffer.rewind();
            }

            //vado a leggere nel file corretto contenente le frequenze alla posizione offset
            mappedByteBuffer = arrayTempFreqs.get(ol.getIndex()).map(FileChannel.MapMode.READ_ONLY, currentD.getOffset_start_freq(), currentD.getLengthPostingList_freq());

            if (mappedByteBuffer != null) {

                // setto il nuovo offset di partenza
                currentD.setOffset_start_freq(finalIIFreqs.size());

                // Creo l'array temporaneo di freq
                for (int i = 1; i <= currentD.getLengthPostingList_freq() / 4; i++)
                    currentFreqList.add(mappedByteBuffer.getInt());

                mappedByteBuffer.rewind();
            }

            if (currentD.getTerm().equals(prevD.getTerm()) || firstIteration) {

                //modifico currentD perchè all'interno di prevD ho lo stesso termine
                // oppere perchè se sono nella prima iterazione prevD è vuoto

                currentD.incDocumentFrequency(prevD.getDocumentFrequency());
                currentD.incCollectionFrequency(prevD.getCollectionFrequency());

                currentD.incLengthPostingList_doc(prevD.getLengthPostingList_doc());
                currentD.setOffset_start_doc(prevD.getOffset_start_doc());

                currentD.incLengthPostingList_freq(prevD.getLengthPostingList_freq());
                currentD.setOffset_start_freq(prevD.getOffset_start_freq());

                prevFreqList.addAll(currentFreqList);
                prevDocList.addAll(currentDocList);

                firstIteration=false;

            } else {
                // Il termine che ho dentro prevD non è presente all'interno di altri file temporanei
                // Quindi posso andarlo a scrivere all'interno dei dizionario finale
                // prima comprimo tutto
                // TODO: verificare se serve la compressione (flag)

                // compressione array di frequenze
                temp = Unary.fromIntToUnary(prevFreqList);

                prevD.setOffset_start_freq(finalIIFreqs.size());
                prevFreqList.clear();
                prevFreqList = new ArrayList<>(currentFreqList);

                FileManagement.writeByteToFile(temp, "freqII_new", temp.length);

                // aggiorno la lunghezza della posting list compressa
                prevD.setLengthPostingList_freq(temp.length);

                // TODO: skipping
                //doSkipping(old_d, old_list_doc);

                //compressione array di docID
                temp = VariableByte.fromArrayLongToVariableByte(prevDocList);
                prevD.setOffset_start_freq(finalIIDocs.size());
                prevDocList.clear();
                prevDocList = new ArrayList<>(currentDocList);

                FileManagement.writeByteToFile(temp, "docII_new", temp.length);

                // aggiorno la lunghezza della posting list compressa
                prevD.setLengthPostingList_doc(temp.length);

                // posso andare a scrivere old_d in dictionary
                writeOneDict(prevD, "new_dict");
            }

            // Modifico le variabili temporanee per la prossima iterazione
            prevD= new DictionaryElem(currentD);
            currentFreqList.clear();
            currentDocList.clear();

            /* Inserisco all'interno della priority queue il prossimo termine presente nel file dictionary temporaneo
              della parola appena estratta come minore in ordine alfabetico*/
            if(arrayTempDict.get(ol.getIndex()).size() > offsetDict[ol.getIndex()]) {

                mappedByteBuffer = arrayTempDict.get(ol.getIndex()).map(FileChannel.MapMode.READ_ONLY, offsetDict[ol.getIndex()], 20);

                if (mappedByteBuffer != null)
                    currentD.setTerm(StandardCharsets.UTF_8.decode(mappedByteBuffer).toString());

                ol = new OrderedList(currentD.getTerm(), ol.getIndex());
                pQueue.add(ol);
            }
        }
    }


}
