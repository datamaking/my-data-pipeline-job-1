# NLP ETL Pipeline Project Structure and Flow

```mermaid
graph TD
    subgraph Project Structure
        A[main.py] --> B[pipeline/pipeline_builder.py]
        B --> C[config/config_manager.py]
        C --> D[source/source_factory.py]
        C --> E[preprocessing/preprocessing_factory.py]
        C --> F[chunking/chunking_factory.py]
        C --> G[embedding/embedding_factory.py]
        C --> H[target/target_factory.py]
        
        D --> I[source/base_source.py]
        D --> J[(source/*_source.py)]
        E --> K[preprocessing/base_preprocessor.py]
        E --> L[/preprocessing/*_processor.py/]
        F --> M[chunking/base_chunker.py]
        F --> N[/chunking/*_chunker.py/]
        G --> O[embedding/base_embedder.py]
        G --> P[/embedding/*_embedder.py/]
        H --> Q[target/base_target.py]
        H --> R[(target/*_target.py)]
        
        C --> S[config/source_configs/]
        C --> T[config/target_configs/]
        S --> U[file_config.py, rdbms_config.py...]
        T --> V[file_config.py, vector_db_config.py...]
        
        A --> W[utils/logger.py]
        A --> X[utils/exception_handler.py]
        
        B --> Y[pipeline/domain_pipelines/*_pipeline.py]
    end

    subgraph Data Flow
        Z[Data Sources] --> SOURCE[Source Factory]
        SOURCE --> PRE[Preprocessing]
        PRE --> CHUNK[Chunking]
        CHUNK --> EMBED[Embedding]
        EMBED --> TARGET[Target Factory]
        TARGET --> ZZ[Data Targets]
    end

    style A fill:#4CAF50,stroke:#333
    style Z fill:#7CB9E8,stroke:#333
    style ZZ fill:#7CB9E8,stroke:#333
    style B,C,D,E,F,G,H fill:#FFC107,stroke:#333
    style I,K,M,O,Q fill:#E91E63,stroke:#333
    style J,L,N,P,R,U,V,Y fill:#9C27B0,stroke:#333
    style W,X fill:#009688,stroke:#333

    classDef entrypoint fill:#4CAF50,stroke:#333
    classDef config fill:#FFC107,stroke:#333
    classDef abstract fill:#E91E63,stroke:#333
    classDef concrete fill:#9C27B0,stroke:#333
    classDef utils fill:#009688,stroke:#333
    classDef storage fill:#7CB9E8,stroke:#333
    
    class A entrypoint
    class Z,ZZ storage
    class B,C,D,E,F,G,H config
    class I,K,M,O,Q abstract
    class J,L,N,P,R,U,V,Y concrete
    class W,X utils