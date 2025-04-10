# NLP ETL Pipeline Process Diagram

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'primaryColor': '#ffd8d8', 'edgeLabelBackground':'#fff'}}}%%

flowchart TD
    classDef module fill:#f0f0f0,stroke:#333,stroke-width:2px
    classDef storage fill:#e0f7ff,stroke:#039be5,stroke-dasharray: 5 5

    subgraph data_sources[Data Sources Module]
        direction TB
        DS1[(Hive Tables)]
        DS2[(HDFS/Local Files)]
        DS3[(RDBMS)]
        DS4[(NoSQL)]
        DS5[(Vector DBs)]
    end

    subgraph preprocessing[Preprocessing Module]
        direction TB
        P1[HTML Parsing]
        P2[Text Cleaning]
        P3[Data Normalization]
    end

    subgraph chunking[Chunking Module]
        direction TB
        C1[Fixed Size]
        C2[Semantic]
        C3[Recursive]
        C4[Chunk Smoothing]
    end

    subgraph embedding[Embedding Module]
        direction TB
        E1[TF-IDF]
        E2[Sentence-BERT]
        E3[OpenAI]
        E4[Custom Models]
    end

    subgraph data_targets[Data Targets Module]
        direction TB
        DT1[(Hive Tables)]
        DT2[(Cloud Storage)]
        DT3[(RDBMS)]
        DT4[(Vector DBs)]
        DT5[(NoSQL)]
    end

    data_sources --> preprocessing
    preprocessing --> chunking
    chunking --> embedding
    embedding --> data_targets

    %% Intermediate Storage
    storage1[(Raw Storage)]:::storage
    storage2[(Processed Storage)]:::storage
    storage3[(Chunk Storage)]:::storage
    storage4[(Embedding Storage)]:::storage

    data_sources --> storage1
    preprocessing --> storage2
    chunking --> storage3
    embedding --> storage4
    storage4 --> data_targets

    %% Configuration & Patterns
    note1["
    Configuration Manager:
    - Source/Target Configs
    - Processing Params
    Design Patterns:
    - Factory: Source/Target Creation
    - Strategy: Chunking/Embedding
    - Observer: Monitoring
    "]:::module

    class data_sources,preprocessing,chunking,embedding,data_targets module