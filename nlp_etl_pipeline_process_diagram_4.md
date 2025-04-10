```mermaid

graph LR
    %% Define the main modules/steps
    DS[Data Sources] --> PRE[Preprocessing]
    PRE --> CHK[Chunking]
    CHK --> EMB[Embedding]
    EMB --> DT[Data Targets]
    
    %% Data Sources section
    subgraph data_sources
        DS1[Text Files]
        DS2[Web Crawling]
        DS3[APIs]
        DS4[Databases]
        
        DS1 & DS2 & DS3 & DS4 --> DS
    end
    
    %% Preprocessing section
    subgraph preprocessing
        PRE1[Cleaning]
        PRE2[Tokenization]
        PRE3[Filtering]
        PRE4[Normalization]
        
        DS --> PRE1 --> PRE2 --> PRE3 --> PRE4 --> PRE
    end
    
    %% Chunking section
    subgraph chunking
        CHK1[Sentence Splitting]
        CHK2[Fixed Size Chunks]
        CHK3[Semantic Chunking]
        CHK4[Overlap Configuration]
        
        PRE --> CHK1 & CHK2 & CHK3 --> CHK4 --> CHK
    end
    
    %% Embedding section
    subgraph embedding
        EMB1[Model Selection]
        EMB2[Vector Generation]
        EMB3[Dimensionality Reduction]
        EMB4[Quality Check]
        
        CHK --> EMB1 --> EMB2 --> EMB3 --> EMB4 --> EMB
    end
    
    %% Data Targets section
    subgraph data_targets
        DT1[Vector Database]
        DT2[Search Index]
        DT3[Data Warehouse]
        DT4[Analytics Platform]
        
        EMB --> DT1 & DT2 & DT3 & DT4
    end
    
    %% Styling
    classDef primary fill:#4287f5,stroke:#0051cb,color:white,stroke-width:2px;
    classDef module fill:#c9e1ff,stroke:#4287f5,stroke-width:1px;
    
    class DS,PRE,CHK,EMB,DT primary;
    class DS1,DS2,DS3,DS4,PRE1,PRE2,PRE3,PRE4,CHK1,CHK2,CHK3,CHK4,EMB1,EMB2,EMB3,EMB4,DT1,DT2,DT3,DT4 module;