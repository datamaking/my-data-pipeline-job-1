# NLP ETL Pipeline Diagram

This diagram represents the NLP ETL pipeline with its main modules:

1. **data_sources**: Where the raw data is collected.
2. **preprocessing**: Where data cleaning and preparation occurs.
3. **chunking**: Where the data is segmented into manageable pieces.
4. **embedding**: Where the segmented data is converted into embeddings.
5. **data_targets**: Where the processed data is stored or forwarded.

```mermaid
flowchart TD
    A[data_sources] --> B[preprocessing]
    B --> C[chunking]
    C --> D[embedding]
    D --> E[data_targets]
