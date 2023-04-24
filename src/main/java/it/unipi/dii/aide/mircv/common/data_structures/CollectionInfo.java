package it.unipi.dii.aide.mircv.common.data_structures;

import java.io.*;
import java.nio.ByteBuffer;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.CollectionInfo_raf;

public class CollectionInfo {

    /* Total number of docIDs present in the collection */
    private static long docid_counter = 0;

    /* Sum of the total length of the documents present in the collection */
    private static long total_doc_len = 0;

    /**
     * Function that write out the information about the collection
     * @throws IOException if the channel is not found
     */
    public static void writeCollectionInfoToDisk() throws IOException {

        ByteBuffer docIndexBuffer = ByteBuffer.allocate(16);
        CollectionInfo_raf.getChannel().position(CollectionInfo_raf.getChannel().size());

        /* Write the Collection Info into file */
        docIndexBuffer.putLong(docid_counter);
        docIndexBuffer.putLong(total_doc_len);

        docIndexBuffer = ByteBuffer.wrap(docIndexBuffer.array());

        while (docIndexBuffer.hasRemaining())
            CollectionInfo_raf.getChannel().write(docIndexBuffer);
    }

    /**
     * Function that write out the information about the collection in DEBUG-MODE
     * @throws IOException if the channel is not found
     */
    public static void writeCollectionInfoDebugModeToDisk() throws IOException {

        String collection_info_string = "Total docIDs: " + docid_counter + ", Collection length: " + total_doc_len;

        BufferedWriter disk_writer = new BufferedWriter(
                new FileWriter("src/main/resources/Debug/collection_info_debug.txt", true));
        disk_writer.write(collection_info_string);
        disk_writer.close();
    }

    /* SETTER AND GETTER SECTION */

    public static void setDocid_counter(long docid_counter) {
        CollectionInfo.docid_counter = docid_counter;
    }

    public static void setTotal_doc_len(long total_doc_len) {
        CollectionInfo.total_doc_len = total_doc_len;
    }

    public static long getDocid_counter() {
        return docid_counter;
    }

    public static long getTotal_doc_len() {
        return total_doc_len;
    }
}
