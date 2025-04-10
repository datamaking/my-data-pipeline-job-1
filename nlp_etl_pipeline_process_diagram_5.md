# NLP ETL Pipeline (nlp_etl_pipeline)

```mermaid
graph TD
    A[(data_sources)] --> B[preprocessing]
    B --> C[chunking]
    C --> D[embedding]
    D --> E[(data_targets)]

    style A fill:#7CB9E8,stroke:#333
    style E fill:#7CB9E8,stroke:#333
    style B fill:#F4A460,stroke:#333
    style C fill:#F4A460,stroke:#333
    style D fill:#F4A460,stroke:#333

    classDef storage fill:#7CB9E8,stroke:#333
    classDef process fill:#F4A460,stroke:#333
    
    class A,E storage
    class B,C,D process