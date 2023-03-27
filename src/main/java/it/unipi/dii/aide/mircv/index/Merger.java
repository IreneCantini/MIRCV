package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.common.data_structures.*;
import it.unipi.dii.aide.mircv.common.data_structures.min_heap.ComparatorTerm;
import it.unipi.dii.aide.mircv.common.data_structures.min_heap.OrderedList;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.common.data_structures.SkippingElem.writeArraySkippingElemToDisk;
import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.*;
import static java.lang.Math.min;

public class Merger {
    private static final long[] currentOffsetDictionary = new long[SPIMI.block_number+1];

    public static void executeMerge() throws IOException, InterruptedException {
        DictionaryElem current_dict_elem = new DictionaryElem();

        DictionaryElem previous_dict_elem = new DictionaryElem();

        PostingList current_pl = new PostingList();
        PostingList previous_pl = new PostingList();

        MappedByteBuffer mappedByteBuffer;

        OrderedList ol;

        // variable to implement skipping
        int howManyDoc;
        List<Posting> subPostingList;
        PostingList temp;
        ArrayList<SkippingElem> arrSkipInfo = new ArrayList<>();

        boolean firstIteration = true;

        PriorityQueue<OrderedList> pQueue = new PriorityQueue<>(SPIMI.block_number == 0 ? 1 : SPIMI.block_number, new ComparatorTerm());

        //create the final files and associate the file Channels to them
        createFinalFile();

        //for-loop for inserting in the priority queue all the first term which are in temporary dictionary files
        for(int i = 0; i <= SPIMI.block_number; i++){
            //read from the temporary dictionary file ith the first term and insert it in the priority queue
            try {
                mappedByteBuffer = RandomAccessFile_map.get(i).get(0).getChannel().map(FileChannel.MapMode.READ_ONLY, 0, 20);

                if (mappedByteBuffer != null) {
                    String[] term = StandardCharsets.UTF_8.decode(mappedByteBuffer).toString().split("\0");
                    ol = new OrderedList(term[0], i);
                    pQueue.add(ol);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        //until the priority queue becomes empty pick the lowest term that hasn't been processed yet
        while (!pQueue.isEmpty()){
            ol = pQueue.poll();

            //insert in the priority queue the next term from the same temporary dictionary file of the term just picked
            if(RandomAccessFile_map.get(ol.getIndex()).get(0).getChannel().size() > currentOffsetDictionary[ol.getIndex()] + 92) {
                mappedByteBuffer = RandomAccessFile_map.get(ol.getIndex()).get(0).getChannel().map(FileChannel.MapMode.READ_ONLY,currentOffsetDictionary[ol.getIndex()] + 92, 20);

                if(mappedByteBuffer != null){
                    String[] term = StandardCharsets.UTF_8.decode(mappedByteBuffer).toString().split("\0");
                    ol = new OrderedList(term[0], ol.getIndex());
                    pQueue.add(ol);
                }
            }

            //read from the disk the Dictionary elem corresponding to the term picked from the dictionary file corresponding to the picked index
            current_dict_elem.readDictionaryElemFromDisk(currentOffsetDictionary[ol.getIndex()], RandomAccessFile_map.get(ol.getIndex()).get(0).getChannel());
            currentOffsetDictionary[ol.getIndex()] += 92;

            //read from disk the Posting list corresponding to the current Dictionary
            current_pl.readPostingListFromDisk(current_dict_elem, RandomAccessFile_map.get(ol.getIndex()).get(1).getChannel(), RandomAccessFile_map.get(ol.getIndex()).get(2).getChannel());

            if(firstIteration) {
                //initialize previous dictionary elem
                previous_dict_elem = current_dict_elem;
                previous_pl.getPl().addAll(current_pl.getPl());
                firstIteration = false;
            }else if(current_dict_elem.getTerm().equals(previous_dict_elem.getTerm())) {
                //update previous dictionary elem
                previous_dict_elem.incDf(current_dict_elem.getDf());
                previous_dict_elem.incCf(current_dict_elem.getCf());

                previous_dict_elem.incDocidLen(current_dict_elem.getDocids_len());
                previous_dict_elem.incFreqLen(current_dict_elem.getTf_len());

                if(current_dict_elem.getMaxTf() > previous_dict_elem.getMaxTf())
                    previous_dict_elem.setMaxTf(current_dict_elem.getMaxTf());

                previous_pl.getPl().addAll(current_pl.getPl());
            }else{
                //the term picked is different from the previous one, so it is possible to write the dictionary and the posting list to disk
                //firstly we are going to check if it is necessary to do skipping after that we are going to check the compression flag.
                //if the flag compression is true it is necessary to do the compression before writing it on the disk

                //set the len of the posting list to 0 for setting it with the final lenght of the merged posting list using the inc method
                previous_dict_elem.setDocids_len(0);
                previous_dict_elem.setTf_len(0);
                previous_dict_elem.computeIdf();
                previous_dict_elem.computeMaxTFIDF();
                previous_dict_elem.computeMaxBM25(previous_pl);

                if(previous_pl.getPl().size()>= 1024) {
                    howManyDoc = (int) Math.ceil(Math.sqrt(previous_pl.getPl().size()));
                    for (int i = 0; i < previous_pl.getPl().size(); i += howManyDoc) {

                        subPostingList = previous_pl.getPl().subList(i, min(i + howManyDoc, previous_pl.getPl().size()));
                        arrSkipInfo.add(new SkippingElem(subPostingList.get(subPostingList.size()-1).getDocID(), RandomAccessFile_map.get(SPIMI.block_number+1).get(1).getChannel().size(), 0, RandomAccessFile_map.get(SPIMI.block_number+1).get(2).getChannel().size(), 0));

                        temp = new PostingList();
                        temp.getPl().addAll(subPostingList);
                        //update the len of the skip field of the dictionary elem
                        previous_dict_elem.incSkipInfo_len();

                        if (!Flags.isCompression_flag()) {
                            //write posting list to final files
                            temp.writePostingListToDisk(arrSkipInfo.get(arrSkipInfo.size()-1), previous_dict_elem, RandomAccessFile_map.get(SPIMI.block_number + 1).get(1).getChannel(), RandomAccessFile_map.get(SPIMI.block_number + 1).get(2).getChannel());
                        } else {
                            //write the compressed posting list
                            temp.writeCompressedPostingListToDisk(arrSkipInfo.get(arrSkipInfo.size()-1), previous_dict_elem, RandomAccessFile_map.get(SPIMI.block_number + 1).get(1).getChannel(), RandomAccessFile_map.get(SPIMI.block_number + 1).get(2).getChannel());
                        }
                    }

                    //update the offset of the skip field of the dictionary elem and then write the skipping elem into the file
                    previous_dict_elem.setOffset_skipInfo(skippingBlock_raf.getChannel().size());
                    writeArraySkippingElemToDisk(arrSkipInfo, skippingBlock_raf.getChannel());
                    arrSkipInfo.clear();
                }
                else{
                    if (!Flags.isCompression_flag()) {
                        //write posting list to final files
                        previous_pl.writePostingListToDisk(null, previous_dict_elem, RandomAccessFile_map.get(SPIMI.block_number + 1).get(1).getChannel(), RandomAccessFile_map.get(SPIMI.block_number + 1).get(2).getChannel());
                    } else {
                        //write the compressed posting list
                        previous_pl.writeCompressedPostingListToDisk(null, previous_dict_elem, RandomAccessFile_map.get(SPIMI.block_number + 1).get(1).getChannel(), RandomAccessFile_map.get(SPIMI.block_number + 1).get(2).getChannel());
                    }
                }

                //write dictionary to final file
                previous_dict_elem.writeDictionaryElemToDisk(RandomAccessFile_map.get(SPIMI.block_number+1).get(0).getChannel());

                //update the offset of the final posting list files
                current_dict_elem.setOffset_docids(RandomAccessFile_map.get(SPIMI.block_number+1).get(1).getChannel().size());
                current_dict_elem.setOffset_tf(RandomAccessFile_map.get(SPIMI.block_number +1).get(2).getChannel().size());

                previous_dict_elem = current_dict_elem;
                previous_pl.getPl().clear();
                previous_pl.getPl().addAll(current_pl.getPl());

            }

            //clean the variable for the next iteration
            current_dict_elem = new DictionaryElem();
            current_pl.getPl().clear();
        }

        //clean Memory
        RandomAccessFile_map.clear();

        //delete temporary files
        deleteTemporaryFile();
    }
}
