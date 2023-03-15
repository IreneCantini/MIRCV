package it.unipi.dii.aide.mircv.common.data_structures;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.doc_raf;

public class DocumentIndexElem {
    private String docNo;
    private long docId;
    private long length;

    public DocumentIndexElem(){
        this.docId=0;
        this.docNo="";
        this.length=0;
    }

    public DocumentIndexElem(long docId, String docNo, long length) {
        int diffLenght=20-docNo.length();
        if(diffLenght!=0){
            // andiamo a concatenare " " per arrivare alla dimensione fissa di 20 caratteri
            docNo = docNo + " ".repeat(Math.max(0, diffLenght));
        }
        this.docId = docId;
        this.docNo = docNo;
        this.length = length;
    }

    public void setDocId(long docId) {
        this.docId = docId;
    }

    public void setDocNo(String docNo) {
        this.docNo = docNo;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getDocId() {
        return docId;
    }

    public String getDocNo() {
        return docNo;
    }

    public long getLength() {
        return length;
    }

    /*public void writeDocIndexElemToDisk() throws IOException {

        MappedByteBuffer docIndexBuffer;

        docIndexBuffer = doc_raf.getChannel().map(FileChannel.MapMode.READ_WRITE, doc_raf.getChannel().size(), 36);


        if(docIndexBuffer != null){
            //write the document index elem fields into the corresponding file
            CharBuffer charBuffer = CharBuffer.allocate(20);
            for(int i = 0; i<this.docNo.length(); i++)
                charBuffer.put(i, this.docNo.charAt(i));

            docIndexBuffer.put(StandardCharsets.UTF_8.encode(charBuffer));
            docIndexBuffer.putLong(this.docId);
            docIndexBuffer.putLong(this.length);
        }
    }*/

    public void writeDocIndexElemToDisk(FileChannel docIndexFileChannel) throws IOException {
        ByteBuffer docIndexBuffer = ByteBuffer.allocate(36);
        docIndexFileChannel.position(docIndexFileChannel.size());

        CharBuffer charBuffer = CharBuffer.allocate(20);
        for(int i = 0; i<this.docNo.length(); i++)
            charBuffer.put(i, this.docNo.charAt(i));

        //write the dictionary elem fields into file
        docIndexBuffer.put(StandardCharsets.UTF_8.encode(charBuffer));
        docIndexBuffer.putLong(this.docId);
        docIndexBuffer.putLong(this.length);

        docIndexBuffer = ByteBuffer.wrap(docIndexBuffer.array());

        while(docIndexBuffer.hasRemaining()) {
            docIndexFileChannel.write(docIndexBuffer);
        }
    }

    /*public void readDocumentIndexElemFromDisk(int position) throws IOException {
        MappedByteBuffer buffer;

        buffer = doc_raf.getChannel().map(FileChannel.MapMode.READ_ONLY,position, 20);

        if(buffer!=null){
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
            String[] term = charBuffer.toString().split("\0");
            this.setDocNo(term[0]);
        }

        buffer = doc_raf.getChannel().map(FileChannel.MapMode.READ_ONLY, position + 20, 16);

        this.setDocId(buffer.getLong());
        this.setLength(buffer.getLong());
    }*/

    public void readDocumentIndexElemFromDisk(int start_position, FileChannel docIndexFileChannel) throws IOException {
        ByteBuffer docIndexBuffer = ByteBuffer.allocate(20);

        docIndexFileChannel.position(start_position);

        while (docIndexBuffer.hasRemaining()){
            docIndexFileChannel.read(docIndexBuffer);
        }

        this.setDocNo(new String(docIndexBuffer.array(), StandardCharsets.UTF_8).trim());

        docIndexBuffer = ByteBuffer.allocate(16);

        while (docIndexBuffer.hasRemaining()){
            docIndexFileChannel.read(docIndexBuffer);
        }

        docIndexBuffer.rewind();
        this.setDocId(docIndexBuffer.getLong());
        this.setLength(docIndexBuffer.getLong());
    }
}
