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

    private static long[] offsetDict;

    private static int numTempFiles;

    private static ArrayList<FileChannel> arrayTempDict;

    private static ArrayList<FileChannel> arrayTempDocs;

    private static ArrayList<FileChannel> arrayTempFreqs;

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

        /**
         * 1. Creare un array di dimensione uguale al numero di file temporanei (es. per i file temporanei Dict)
         * 2. Creare un array di offset relativi a ogni file (es. per ogni file Dictionary teniamo un campo con l'offset
         *    a cui siamo arrivati)
         * 3. Array di fileChannels con i relativi
         * 4. Fase di merge:
         *      4.1
         */

        boolean firstIteration = true;

        DictionaryElem nextD = new DictionaryElem();
        DictionaryElem prevD = new DictionaryElem();

        ArrayList<Integer> nextFreq= new ArrayList<>();
        ArrayList<Integer> prevFreq= new ArrayList<>();

        ArrayList<Integer> nextDoc= new ArrayList<>();
        ArrayList<Integer> prevDoc= new ArrayList<>();

        MappedByteBuffer mappedByteBuffer;

        OrderedList ol;

        PriorityQueue<OrderedList> pQueue
                = new PriorityQueue<>(numTempFiles, new Comparator());


        createNewFiles();
        setFinalFileChannels();

        for (int i=0; i < numTempFiles; i++) {

            mappedByteBuffer = arrayTempDict.get(i).map(FileChannel.MapMode.READ_ONLY, offsetDict[i], 20);

            if (mappedByteBuffer != null)
                nextD.setTerm(StandardCharsets.UTF_8.decode(mappedByteBuffer).toString());

            // creo nuovo elemento della priority queue
            ol = new OrderedList(nextD.getTerm(), i);
            pQueue.add(ol);
        }

        while (!pQueue.isEmpty()) {

            byte[] temp;

            ArrayList<Long> postingList = new ArrayList<>();
            ArrayList<Integer> freqsList = new ArrayList<>();

            // prendo l'elemento minore
            ol = pQueue.poll();

            mappedByteBuffer = arrayTempDict.get(ol.getIndex()).map(FileChannel.MapMode.READ_ONLY, offsetDict[ol.getIndex()], 68);

            nextD = FileManagement.convertToDictionaryObject(mappedByteBuffer);

            offsetDict[ol.getIndex()] += 68;

            // scrivo in II_doc
            //vado a leggere nel file corretto contenente i documenti alla posizione offset

            mappedByteBuffer = arrayTempDocs.get(ol.getIndex()).map(FileChannel.MapMode.READ_ONLY, nextD.getOffset_start_doc(), nextD.getLengthPostingList_doc());

            if (mappedByteBuffer != null) {

                // setto il nuovo offset di partenza
                nextD.setOffset_start_doc(finalIIDocs.size());

                temp = new byte[mappedByteBuffer.remaining()];
                mappedByteBuffer.get(temp);

                writeByteToFile(temp, "docII_new", nextD.getLengthPostingList_doc());

                // TODO: Controllare se bisogna fare la compressione o meno
                postingList.addAll(VariableByte.fromVariableByteToLong(temp));
            }

            mappedByteBuffer = arrayTempFreqs.get(ol.getIndex()).map(FileChannel.MapMode.READ_ONLY, nextD.getOffset_start_freq(), nextD.getLengthPostingList_freq());

            if (mappedByteBuffer != null) {

                temp = new byte[mappedByteBuffer.remaining()];
                mappedByteBuffer.get(temp);

                // inserisco le frequenze dentro un array temporaneo per poi decomprimerlo tutto insieme
                // caso in cui ho la stessa parola in più file temporanei
                // TODO: Controllare se bisogna fare la compressione o meno
                freqsList.addAll(Unary.fromUnaryToInt(temp));
            }

            if (nextD.getTerm().equals(prevD.getTerm()) || firstIteration) {

                //modifico new_d perchè all'interno di old_d ho lo stesso termine
                // oppere perchè se sono nella prima iterazione old_d è vuoto

                nextD.incDocumentFrequency(prevD.getDocumentFrequency());
                nextD.incCollectionFrequency(prevD.getCollectionFrequency());
                nextD.incLengthPostingList_doc(prevD.getLengthPostingList_doc());
                nextD.setOffset_start_doc(prevD.getOffset_start_doc());
                prevFreq.addAll(nextFreq);
                prevDoc.addAll(nextDoc);
                firstIteration=false;

            } else {
                // Il termine che ho dentro old_d non è presente all'interno di altri file temporanei
                // Quindi posso andarlo a scrivere all'interno dei dizionario finale
                // prima comprimo tutto l'array delle frequenze e lo scrivo
                // TODO: verificare se serve la compressione (flag)
                temp = Unary.fromIntToUnary(prevFreq);

                prevD.setOffset_start_freq(finalIIFreqs.size());
                prevFreq.clear();
                prevFreq = new ArrayList<>(nextFreq);

                writeByteToFile(temp, "freqII_new", temp.length);

                prevD.setLengthPostingList_freq(temp.length);

                //doSkipping(old_d, old_list_doc);


                prevDoc.clear();
                prevDoc = new ArrayList<>(nextDoc);

                // posso andare a scrivere old_d in dictionary
                writeOneDict(prevD, "new_dict");
            }

            // Modifico le variabili temporanee per la prossima iterazione
            prevD= new DictionaryElem(nextD);
            nextFreq.clear();
            nextDoc.clear();

            /* Inserisco all'interno della priority queue il prossimo termine presente nel file dictionary temporaneo
              della parola appena estratta come minore in ordine alfabetico*/
            if(arrayTempDict.get(ol.getIndex()).size() > offsetDict[ol.getIndex()]) {

                mappedByteBuffer = arrayTempDict.get(ol.getIndex()).map(FileChannel.MapMode.READ_ONLY, offsetDict[ol.getIndex()], 20);

                if (mappedByteBuffer != null)
                    nextD.setTerm(StandardCharsets.UTF_8.decode(mappedByteBuffer).toString());

                ol = new OrderedList(nextD.getTerm(), ol.getIndex());
                pQueue.add(ol);
            }
        }
    }


}
