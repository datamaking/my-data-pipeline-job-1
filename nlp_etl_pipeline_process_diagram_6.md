```mermaid
graph TD
    subgraph "Data Sources"
        A1[File Systems] 
        A2[Relational Databases]
        A3[Hive Data Warehouse]
        A4[NoSQL Databases]
        A5[Vector Databases]
    end

    subgraph "Source Layer"
        B[source_factory.py]
    end

    subgraph "Preprocessing Layer"
        C1[preprocessing_factory.py]
        C2[html_parser.py]
        C3[data_cleaner.py]
    end

    subgraph "Chunking Layer"
        D1[chunking_factory.py]
        D2[fixed_size_chunker.py]
        D3[semantic_chunker.py] 
        D4[chunk_smoother.py]
    end

    subgraph "Embedding Layer"
        E1[embedding_factory.py]
        E2[tfidf_embedder.py]
        E3[sentence_embedder.py]
    end

    subgraph "Target Layer"
        F[target_factory.py]
    end

    subgraph "Target Systems"
        G1[File Systems]
        G2[Relational Databases]
        G3[Hive Data Warehouse]
        G4[NoSQL Databases]
        G5[Vector Databases]
    end

    subgraph "Pipeline Orchestration"
        H1[pipeline_builder.py]
        H2[Domain-Specific Pipelines]
    end

    subgraph "Configuration Management"
        I1[config_manager.py]
        I2[Source Configs]
        I3[Preprocessing Config]
        I4[Chunking Config]
        I5[Embedding Config]
        I6[Target Configs]
    end

    %% Connect data sources to source layer
    A1 --> B
    A2 --> B
    A3 --> B
    A4 --> B
    A5 --> B

    %% Main ETL Flow
    B --> C1
    C1 --> C2
    C2 --> C3
    C3 --> D1
    D1 --> D2
    D2 --> D3
    D3 --> D4
    D4 --> E1
    E1 --> E2
    E2 --> E3
    E3 --> F

    %% Connect target layer to target systems
    F --> G1
    F --> G2
    F --> G3
    F --> G4
    F --> G5

    %% Configuration connections
    I1 --> I2
    I1 --> I3
    I1 --> I4
    I1 --> I5
    I1 --> I6
    
    I2 -.-> B
    I3 -.-> C1
    I4 -.-> D1
    I5 -.-> E1
    I6 -.-> F

    %% Pipeline orchestration
    H1 --> H2
    H2 -.-> B
    H2 -.-> C1
    H2 -.-> D1
    H2 -.-> E1
    H2 -.-> F
    
    classDef sourceClass fill:#e1f5fe,stroke:#01579b
    classDef preprocessClass fill:#e8f5e9,stroke:#1b5e20
    classDef chunkClass fill:#fff3e0,stroke:#e65100
    classDef embedClass fill:#f3e5f5,stroke:#4a148c
    classDef targetClass fill:#fce4ec,stroke:#880e4f
    classDef configClass fill:#fffde7,stroke:#f57f17
    classDef pipelineClass fill:#e0f2f1,stroke:#004d40
    
    class A1,A2,A3,A4,A5 sourceClass
    class B,B1,B2,B3,B4,B5 sourceClass
    class C1,C2,C3 preprocessClass
    class D1,D2,D3,D4 chunkClass
    class E1,E2,E3 embedClass
    class F,F1,F2,F3,F4,F5 targetClass
    class G1,G2,G3,G4,G5 targetClass
    class I1,I2,I3,I4,I5,I6 configClass
    class H1,H2 pipelineClass