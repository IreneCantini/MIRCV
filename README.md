# Information Retrieval System
## _Search Engine_

![IntelliJ IDEA](https://img.shields.io/badge/IntelliJIDEA-000000.svg?style=for-the-badge&logo=intellij-idea&logoColor=white) ![Java](https://img.shields.io/badge/java-%23ED8B00.svg?style=for-the-badge&logo=java&logoColor=white)

## Introduction to the project
This project deals with the topic of information retrieval. It constructs a search engine in which user translates her/his information needs into queries and obtains a list of documents that the system considers most relevant based on scores computed with different metrics. 
It is composed by three principal parts:
1. The Index construction
2. The queries processing
3. The performance evaluation

## Execute Index construction
To build the index it is important adding two files in src\main\resources:
- a file named "collection.tsv" containing the document collection available at https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020
- a file named "stopword.txt" containing the english stopword (it could be downloaded online)

The executable file is inside the path: src\main\java\it\unipi\dii\aide\mircv\index\Indexer.java
After the program has been launched you have to answer to three questions, displayed by the command line interface, in order to set some preferences about compression, filtering and debug mode. 
The costruction takes, in average, 30 minutes to execute and after that you will have 7 new files saved in src\main\resources. They will be:
- collection_info
- docid_posting_lists
- document_index 
- flags
- freq_posting_lists
- skipping
- vocabulary 

These files will be used in the next phases.

## Execute Queries processing
The executable file is inside the path: src\main\java\it\unipi\dii\aide\mircv\cli\Main.java
After the program has been launched you have to insert the desidered query and to answer to three questions, displayed by the command line interface, in order to select one of two possible options:
- Disjunctive or Conjunctive queries
- DAAT or MaxScore
- TFIDF or BM25
Then you have to insert also the number (k) of documents you want to obtain for the inserted query.
After that the program shows you the list of the top k ranked documents with the corresponding scores.

## Execute performance evaluation
To test the system performancce you need to add the test file, named "test.tsv" in src\main\resources that you can download from https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020.

The executable file is inside the path: src\main\java\it\unipi\dii\aide\mircv\query_processing\test_performance\TestPerformanceMain.java
After the program has been launched you have to answer to three questions, displayed by the command line interface, in order to select one of two possible options:
- Disjunctive or Conjunctive queries
- DAAT or MaxScore
- TFIDF or BM25
At the end of the execution the program shows the list of documents retrieved for each query with the corresponding response time (considering 5 documents per query) and average response time.
