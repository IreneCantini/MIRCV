# Multimedia Information Retrieval and Computer Vision
## _Search Engine_

![IntelliJ IDEA](https://img.shields.io/badge/IntelliJIDEA-000000.svg?style=for-the-badge&logo=intellij-idea&logoColor=white) ![Java](https://img.shields.io/badge/java-%23ED8B00.svg?style=for-the-badge&logo=java&logoColor=white)

## Features

* The program should: 
    * use the document collection available at [MSMARCO], which is a collection of 8.8 million documents about 2.2GB in size;
    * read the data using UNICODE, removing punctuation signs and dealing with any errors in the data. 
    * create an inverted index, lexicon, and document table structures and store them on disk. 
    * execute ranked queries using an interface similar to openList(), closeList(), next(), getDocid(), and getFreq() on inverted lists, returning the top       10 or 20 results according to the TFIDF scoring function. 
    * be able to perform conjunctive and disjunctive queries and should have a demo interface that reads user queries via a command line prompt and returns       the <pid> of each result.
     
### Files
   
   Name  | Description | Size | Extension
------------- | ------------- | ------------- | -------------
dictionary  | Contains statistics about the term | 69.3 MB | .dat
document_index  | Contains information about the document | 318.3 MB | .dat
inverted_index_doc  | Contains all the posting lists for each document | 749.3 MB | .dat
inverted_index_freq  | Contains all the frequencies for a term for each document | 59.1 MB | .dat
skipping_file  | Contains the skipping list structure | 56 byte | .dat
stopwords  | Contains all the stopwords | 4 KB | .txt


[MSMARCO]: <https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020>
