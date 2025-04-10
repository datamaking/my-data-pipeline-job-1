# ChatGPT

111


Below is an example of a complete PySpark NLP ETL pipeline project that meets your requirements. In this example, the code is organized into a clear project structure with each module implemented as a separate Python package. The modules use only the following design patterns:

- **Singleton Pattern** (used for the configuration manager)  
- **Factory Pattern** (used in data source, embedding, and target module selection)  
- **Strategy Pattern** (used for the chunking strategies)  
- **Template Method Pattern** (used for the preprocessing module)  
- **Builder Pattern** (used for the pipeline builder that assembles the whole ETL process)

This example uses placeholder implementations (for example, dummy Spark jobs and simplified code) to illustrate the overall design. In a real-life project you would add the actual Spark logic, business rules, and more advanced error handling.

Below is the complete code including the project directory tree, code files, test cases (using pytest), a requirements file, and a README with instructions.

---

## Project Structure

```
nlp_etl_pipeline/
├── config
│   ├── base_config.py
│   ├── file_config.py
│   ├── hive_config.py
│   ├── rdbms_config.py
│   ├── nosql_config.py
│   ├── vectordb_config.py
│   ├── preprocess_config.py
│   ├── chunking_config.py
│   ├── embedding_config.py
│   ├── target_config.py
│   └── config_manager.py
├── data_source
│   ├── base_datasource.py
│   ├── hive_source.py
│   ├── file_source.py
│   ├── rdbms_source.py
│   ├── nosql_source.py
│   ├── vectordb_source.py
│   └── datasource_factory.py
├── preprocessing
│   ├── preprocessing_template.py
│   ├── html_parser.py
│   └── data_cleaner.py
├── chunking
│   ├── base_chunker.py
│   ├── chunk_strategy_simple.py
│   ├── chunk_strategy_overlap.py
│   └── chunking_manager.py
├── embedding
│   ├── base_embedder.py
│   ├── tfidf_embedder.py
│   ├── sentence_embedder.py
│   └── embedder_factory.py
├── target
│   ├── base_target.py
│   ├── hive_target.py
│   ├── file_target.py
│   ├── rdbms_target.py
│   ├── nosql_target.py
│   ├── vectordb_target.py
│   └── target_factory.py
├── logging_module
│   └── logger.py
├── exception_module
│   └── custom_exceptions.py
├── pipeline
│   ├── pipeline_builder.py
│   └── pipeline_executor.py
├── tests
│   ├── test_config.py
│   ├── test_datasource.py
│   ├── test_preprocessing.py
│   ├── test_chunking.py
│   ├── test_embedding.py
│   └── test_target.py
├── requirements.txt
├── README.md
└── main.py
```

---

## Code Files

Below are the code files with comments explaining the design patterns used.

### 1. `config/base_config.py`

```python
# nlp_etl_pipeline/config/base_config.py

class BaseConfig:
    def __init__(self):
        self.params = {}

    def get_param(self, key):
        return self.params.get(key)

    def set_param(self, key, value):
        self.params[key] = value
```

### 2. `config/file_config.py`

```python
# nlp_etl_pipeline/config/file_config.py

from .base_config import BaseConfig

class FileConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "file_path": "/path/to/input/file.txt",
            "file_format": "txt"
        }
```

### 3. `config/hive_config.py`

```python
# nlp_etl_pipeline/config/hive_config.py

from .base_config import BaseConfig

class HiveConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "hive_table": "default.table_name",
            "hive_database": "default"
        }
```

### 4. `config/rdbms_config.py`

```python
# nlp_etl_pipeline/config/rdbms_config.py

from .base_config import BaseConfig

class RDBMSConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "jdbc_url": "jdbc:mysql://localhost:3306/db",
            "db_table": "source_table",
            "user": "username",
            "password": "password"
        }
```

### 5. `config/nosql_config.py`

```python
# nlp_etl_pipeline/config/nosql_config.py

from .base_config import BaseConfig

class NoSQLConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "mongo_uri": "mongodb://localhost:27017",
            "collection": "source_collection"
        }
```

### 6. `config/vectordb_config.py`

```python
# nlp_etl_pipeline/config/vectordb_config.py

from .base_config import BaseConfig

class VectorDBConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "vector_db_type": "ChromaDB",  # could be 'PostgreSQL' or 'Neo4j'
            "host": "localhost",
            "port": 1234
        }
```

### 7. `config/preprocess_config.py`

```python
# nlp_etl_pipeline/config/preprocess_config.py

from .base_config import BaseConfig

class PreprocessConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "remove_html_tags": True,
            "lowercase": True,
            "strip_whitespace": True
        }
```

### 8. `config/chunking_config.py`

```python
# nlp_etl_pipeline/config/chunking_config.py

from .base_config import BaseConfig

class ChunkingConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "strategy": "simple",  # could be "simple" or "overlap"
            "chunk_size": 100,
            "overlap": 10
        }
```

### 9. `config/embedding_config.py`

```python
# nlp_etl_pipeline/config/embedding_config.py

from .base_config import BaseConfig

class EmbeddingConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "method": "tfidf",  # could be "tfidf" or "sentence"
            "max_features": 5000
        }
```

### 10. `config/target_config.py`

```python
# nlp_etl_pipeline/config/target_config.py

from .base_config import BaseConfig

class TargetConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "target_system": "hive",  # options: hive, file, rdbms, nosql, vectordb
            "load_type": "full",      # options: full, scd2, cdc
        }
```

### 11. `config/config_manager.py`  
*(Singleton Pattern)*

```python
# nlp_etl_pipeline/config/config_manager.py

from threading import Lock

class ConfigManager:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if not cls._instance:
                cls._instance = super(ConfigManager, cls).__new__(cls)
                cls._instance._configs = {}
            return cls._instance

    def register_config(self, name, config):
        self._configs[name] = config

    def get_config(self, name):
        return self._configs.get(name)

# Example usage in code:
# from config.config_manager import ConfigManager
# from config.file_config import FileConfig
# manager = ConfigManager()
# manager.register_config("file", FileConfig())
```

---

### 12. `data_source/base_datasource.py`

```python
# nlp_etl_pipeline/data_source/base_datasource.py

from pyspark.sql import SparkSession

class BaseDataSource:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder.appName("NLP_ETL").getOrCreate()

    def read(self):
        raise NotImplementedError("Subclasses must implement the read method.")
```

### 13. `data_source/hive_source.py`

```python
# nlp_etl_pipeline/data_source/hive_source.py

from .base_datasource import BaseDataSource

class HiveSource(BaseDataSource):
    def read(self):
        table = self.config.get_param("hive_table")
        # Read from Hive table
        df = self.spark.sql(f"SELECT * FROM {table}")
        return df
```

### 14. `data_source/file_source.py`

```python
# nlp_etl_pipeline/data_source/file_source.py

from .base_datasource import BaseDataSource

class FileSource(BaseDataSource):
    def read(self):
        path = self.config.get_param("file_path")
        fmt = self.config.get_param("file_format")
        # Depending on format, use different reading logic.
        if fmt == "json":
            df = self.spark.read.json(path)
        elif fmt == "html":
            # For HTML, one might use a custom parser here.
            df = self.spark.read.text(path)
        else:
            df = self.spark.read.text(path)
        return df
```

### 15. `data_source/rdbms_source.py`

```python
# nlp_etl_pipeline/data_source/rdbms_source.py

from .base_datasource import BaseDataSource

class RDBMSSource(BaseDataSource):
    def read(self):
        params = self.config.params
        df = self.spark.read.format("jdbc") \
                .option("url", params["jdbc_url"]) \
                .option("dbtable", params["db_table"]) \
                .option("user", params["user"]) \
                .option("password", params["password"]) \
                .load()
        return df
```

### 16. `data_source/nosql_source.py`

```python
# nlp_etl_pipeline/data_source/nosql_source.py

from .base_datasource import BaseDataSource

class NoSQLSource(BaseDataSource):
    def read(self):
        # This is a placeholder. In practice, you would use a connector
        # (e.g. pymongo) to get data then create a Spark DataFrame.
        print("Reading data from NoSQL source")
        return self.spark.createDataFrame([], schema=[])
```

### 17. `data_source/vectordb_source.py`

```python
# nlp_etl_pipeline/data_source/vectordb_source.py

from .base_datasource import BaseDataSource

class VectorDBSource(BaseDataSource):
    def read(self):
        # This is a placeholder for reading from a vector database.
        print("Reading data from Vector DB source")
        return self.spark.createDataFrame([], schema=[])
```

### 18. `data_source/datasource_factory.py`  
*(Factory Pattern)*

```python
# nlp_etl_pipeline/data_source/datasource_factory.py

from .hive_source import HiveSource
from .file_source import FileSource
from .rdbms_source import RDBMSSource
from .nosql_source import NoSQLSource
from .vectordb_source import VectorDBSource

class DataSourceFactory:
    @staticmethod
    def get_data_source(source_type, config):
        if source_type == "hive":
            return HiveSource(config)
        elif source_type == "file":
            return FileSource(config)
        elif source_type == "rdbms":
            return RDBMSSource(config)
        elif source_type == "nosql":
            return NoSQLSource(config)
        elif source_type == "vectordb":
            return VectorDBSource(config)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
```

---

### 19. `preprocessing/preprocessing_template.py`  
*(Template Method Pattern)*

```python
# nlp_etl_pipeline/preprocessing/preprocessing_template.py

class PreprocessingTemplate:
    def process(self, data):
        # Template method defining the skeleton of preprocessing.
        data = self.parse(data)
        data = self.clean(data)
        return data

    def parse(self, data):
        raise NotImplementedError("Subclasses must implement the parse method.")

    def clean(self, data):
        raise NotImplementedError("Subclasses must implement the clean method.")
```

### 20. `preprocessing/html_parser.py`

```python
# nlp_etl_pipeline/preprocessing/html_parser.py

from bs4 import BeautifulSoup
from .preprocessing_template import PreprocessingTemplate

class HTMLParser(PreprocessingTemplate):
    def parse(self, data):
        # Assume data is a DataFrame with a column 'html'
        def parse_html(content):
            soup = BeautifulSoup(content, 'html.parser')
            return soup.get_text()
        return data.withColumn("parsed_text", 
                               data.rdd.map(lambda row: parse_html(row.html)))
    
    def clean(self, data):
        # This parser does not clean beyond HTML parsing.
        return data
```

### 21. `preprocessing/data_cleaner.py`

```python
# nlp_etl_pipeline/preprocessing/data_cleaner.py

from .preprocessing_template import PreprocessingTemplate

class DataCleaner(PreprocessingTemplate):
    def parse(self, data):
        # No parsing; pass-through
        return data

    def clean(self, data):
        # Dummy cleaning logic: lowercasing, trimming whitespace.
        from pyspark.sql.functions import lower, trim
        return data.select(*(lower(trim(c)).alias(c) for c in data.columns))
```

---

### 22. `chunking/base_chunker.py`

```python
# nlp_etl_pipeline/chunking/base_chunker.py

class BaseChunker:
    def __init__(self, config):
        self.config = config

    def chunk(self, text):
        raise NotImplementedError("Subclasses must implement chunk()")
```

### 23. `chunking/chunk_strategy_simple.py`  
*(Strategy Pattern Implementation)*

```python
# nlp_etl_pipeline/chunking/chunk_strategy_simple.py

from .base_chunker import BaseChunker

class SimpleChunker(BaseChunker):
    def chunk(self, text):
        chunk_size = self.config.get_param("chunk_size")
        return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
```

### 24. `chunking/chunk_strategy_overlap.py`

```python
# nlp_etl_pipeline/chunking/chunk_strategy_overlap.py

from .base_chunker import BaseChunker

class OverlapChunker(BaseChunker):
    def chunk(self, text):
        chunk_size = self.config.get_param("chunk_size")
        overlap = self.config.get_param("overlap")
        chunks = []
        i = 0
        while i < len(text):
            chunk = text[i:i+chunk_size]
            chunks.append(chunk)
            i += chunk_size - overlap
        return chunks
```

### 25. `chunking/chunking_manager.py`  
*(Using Strategy Pattern to choose strategy)*

```python
# nlp_etl_pipeline/chunking/chunking_manager.py

from .chunk_strategy_simple import SimpleChunker
from .chunk_strategy_overlap import OverlapChunker

class ChunkingManager:
    def __init__(self, config):
        self.config = config
        self.strategy = self._get_strategy(config.get_param("strategy"))

    def _get_strategy(self, strategy_name):
        if strategy_name == "simple":
            return SimpleChunker(self.config)
        elif strategy_name == "overlap":
            return OverlapChunker(self.config)
        else:
            raise ValueError("Unknown chunking strategy")

    def chunk_text(self, text):
        # You can add chunk smoothing process here as needed.
        return self.strategy.chunk(text)
```

---

### 26. `embedding/base_embedder.py`

```python
# nlp_etl_pipeline/embedding/base_embedder.py

class BaseEmbedder:
    def __init__(self, config):
        self.config = config

    def embed(self, text):
        raise NotImplementedError("Subclasses must implement embed()")
```

### 27. `embedding/tfidf_embedder.py`

```python
# nlp_etl_pipeline/embedding/tfidf_embedder.py

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from .base_embedder import BaseEmbedder

class TfIdfEmbedder(BaseEmbedder):
    def embed(self, text_df):
        # text_df is assumed to be a Spark DataFrame with a 'text' column.
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        words_data = tokenizer.transform(text_df)
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=self.config.get_param("max_features"))
        featurizedData = hashingTF.transform(words_data)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        model = idf.fit(featurizedData)
        rescaledData = model.transform(featurizedData)
        return rescaledData
```

### 28. `embedding/sentence_embedder.py`

```python
# nlp_etl_pipeline/embedding/sentence_embedder.py

from .base_embedder import BaseEmbedder
# In practice, you might use a pre-trained model from Spark NLP or other libraries.
class SentenceEmbedder(BaseEmbedder):
    def embed(self, text_df):
        # Placeholder for sentence embedding logic.
        # For demonstration, we simply add a dummy embedding column.
        from pyspark.sql.functions import lit
        return text_df.withColumn("embedding", lit("dummy_embedding"))
```

### 29. `embedding/embedder_factory.py`  
*(Factory Pattern)*

```python
# nlp_etl_pipeline/embedding/embedder_factory.py

from .tfidf_embedder import TfIdfEmbedder
from .sentence_embedder import SentenceEmbedder

class EmbedderFactory:
    @staticmethod
    def get_embedder(config):
        method = config.get_param("method")
        if method == "tfidf":
            return TfIdfEmbedder(config)
        elif method == "sentence":
            return SentenceEmbedder(config)
        else:
            raise ValueError("Unknown embedding method")
```

---

### 30. `target/base_target.py`

```python
# nlp_etl_pipeline/target/base_target.py

class BaseTarget:
    def __init__(self, config):
        self.config = config

    def write(self, df):
        raise NotImplementedError("Subclasses must implement write()")
```

### 31. `target/hive_target.py`

```python
# nlp_etl_pipeline/target/hive_target.py

from .base_target import BaseTarget

class HiveTarget(BaseTarget):
    def write(self, df):
        table = self.config.get_param("target_table") or "default_output_table"
        df.write.mode("overwrite").saveAsTable(table)
```

### 32. `target/file_target.py`

```python
# nlp_etl_pipeline/target/file_target.py

from .base_target import BaseTarget

class FileTarget(BaseTarget):
    def write(self, df):
        path = self.config.get_param("file_path") or "/path/to/output.txt"
        fmt = self.config.get_param("file_format") or "csv"
        df.write.mode("overwrite").format(fmt).save(path)
```

### 33. `target/rdbms_target.py`

```python
# nlp_etl_pipeline/target/rdbms_target.py

from .base_target import BaseTarget

class RDBMSTarget(BaseTarget):
    def write(self, df):
        params = self.config.params
        df.write.format("jdbc") \
          .option("url", params.get("jdbc_url")) \
          .option("dbtable", params.get("db_table")) \
          .option("user", params.get("user")) \
          .option("password", params.get("password")) \
          .mode("append") \
          .save()
```

### 34. `target/nosql_target.py`

```python
# nlp_etl_pipeline/target/nosql_target.py

from .base_target import BaseTarget

class NoSQLTarget(BaseTarget):
    def write(self, df):
        # Placeholder: In a real implementation, write to a NoSQL database.
        print("Writing data to NoSQL target")
```

### 35. `target/vectordb_target.py`

```python
# nlp_etl_pipeline/target/vectordb_target.py

from .base_target import BaseTarget

class VectorDBTarget(BaseTarget):
    def write(self, df):
        # Placeholder: In a real implementation, write to a vector DB.
        print("Writing data to VectorDB target")
```

### 36. `target/target_factory.py`  
*(Factory Pattern)*

```python
# nlp_etl_pipeline/target/target_factory.py

from .hive_target import HiveTarget
from .file_target import FileTarget
from .rdbms_target import RDBMSTarget
from .nosql_target import NoSQLTarget
from .vectordb_target import VectorDBTarget

class TargetFactory:
    @staticmethod
    def get_target(config):
        target_system = config.get_param("target_system")
        if target_system == "hive":
            return HiveTarget(config)
        elif target_system == "file":
            return FileTarget(config)
        elif target_system == "rdbms":
            return RDBMSTarget(config)
        elif target_system == "nosql":
            return NoSQLTarget(config)
        elif target_system == "vectordb":
            return VectorDBTarget(config)
        else:
            raise ValueError("Unknown target system")
```

---

### 37. `logging_module/logger.py`

```python
# nlp_etl_pipeline/logging_module/logger.py

import logging

def get_logger(name=__name__):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
```

---

### 38. `exception_module/custom_exceptions.py`

```python
# nlp_etl_pipeline/exception_module/custom_exceptions.py

class ETLException(Exception):
    pass

class ConfigException(ETLException):
    pass

class DataSourceException(ETLException):
    pass

class TargetException(ETLException):
    pass
```

---

### 39. `pipeline/pipeline_builder.py`  
*(Builder Pattern)*

```python
# nlp_etl_pipeline/pipeline/pipeline_builder.py

from config.config_manager import ConfigManager
from data_source.datasource_factory import DataSourceFactory
from preprocessing.data_cleaner import DataCleaner
from chunking.chunking_manager import ChunkingManager
from embedding.embedder_factory import EmbedderFactory
from target.target_factory import TargetFactory
from logging_module.logger import get_logger

class PipelineBuilder:
    def __init__(self):
        self.logger = get_logger("PipelineBuilder")
        self.config_manager = ConfigManager()
        self.source = None
        self.preprocessor = None
        self.chunker = None
        self.embedder = None
        self.target = None

    def build_source(self, source_type, config_name):
        config = self.config_manager.get_config(config_name)
        self.source = DataSourceFactory.get_data_source(source_type, config)
        self.logger.info(f"Source {source_type} built with config {config_name}")
        return self

    def build_preprocessor(self, config_name, preprocessor_type="cleaner"):
        config = self.config_manager.get_config(config_name)
        # For demonstration, we use DataCleaner; you could choose different preprocessors.
        self.preprocessor = DataCleaner()
        self.logger.info("Preprocessor built")
        return self

    def build_chunker(self, config_name):
        config = self.config_manager.get_config(config_name)
        self.chunker = ChunkingManager(config)
        self.logger.info("Chunking module built")
        return self

    def build_embedder(self, config_name):
        config = self.config_manager.get_config(config_name)
        self.embedder = EmbedderFactory.get_embedder(config)
        self.logger.info("Embedding module built")
        return self

    def build_target(self, config_name):
        config = self.config_manager.get_config(config_name)
        self.target = TargetFactory.get_target(config)
        self.logger.info("Target module built")
        return self

    def get_pipeline(self):
        if not all([self.source, self.preprocessor, self.chunker, self.embedder, self.target]):
            raise Exception("Pipeline not completely built.")
        return {
            "source": self.source,
            "preprocessor": self.preprocessor,
            "chunker": self.chunker,
            "embedder": self.embedder,
            "target": self.target
        }
```

### 40. `pipeline/pipeline_executor.py`

```python
# nlp_etl_pipeline/pipeline/pipeline_executor.py

from logging_module.logger import get_logger

class PipelineExecutor:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = get_logger("PipelineExecutor")

    def execute(self):
        # Read from source
        self.logger.info("Starting pipeline execution")
        df = self.pipeline["source"].read()

        # Preprocess data
        df = self.pipeline["preprocessor"].process(df)

        # For illustration, assume there is a column "text" to chunk and embed.
        # In reality, you would adjust this to your schema.
        if "text" in df.columns:
            rdd = df.rdd.map(lambda row: row.text)
            # Chunk each text value (this is an example using Python list processing)
            chunked = rdd.map(lambda text: self.pipeline["chunker"].chunk_text(text)).collect()
            self.logger.info(f"Chunked data: {chunked}")

        # Embed the text (assume DataFrame is appropriate for the embedder)
        df = self.pipeline["embedder"].embed(df)

        # Write to target
        self.pipeline["target"].write(df)
        self.logger.info("Pipeline execution finished")
```

---

### 41. `main.py`

```python
# nlp_etl_pipeline/main.py

from config.config_manager import ConfigManager
from config.file_config import FileConfig
from config.hive_config import HiveConfig
from config.rdbms_config import RDBMSConfig
from config.nosql_config import NoSQLConfig
from config.vectordb_config import VectorDBConfig
from config.preprocess_config import PreprocessConfig
from config.chunking_config import ChunkingConfig
from config.embedding_config import EmbeddingConfig
from config.target_config import TargetConfig
from pipeline.pipeline_builder import PipelineBuilder
from pipeline.pipeline_executor import PipelineExecutor

def main():
    # Register all configurations in the Singleton ConfigManager.
    cm = ConfigManager()
    cm.register_config("file_source", FileConfig())
    cm.register_config("hive_source", HiveConfig())
    cm.register_config("rdbms_source", RDBMSConfig())
    cm.register_config("nosql_source", NoSQLConfig())
    cm.register_config("vectordb_source", VectorDBConfig())
    cm.register_config("preprocess", PreprocessConfig())
    cm.register_config("chunking", ChunkingConfig())
    cm.register_config("embedding", EmbeddingConfig())
    cm.register_config("target", TargetConfig())

    # Build a pipeline. Domain-type pipelines (ADMIN, HR, FINANCE, IT HELPDESK)
    # can be configured by using different configuration registrations.
    builder = PipelineBuilder()
    pipeline = (builder
                .build_source("file", "file_source")
                .build_preprocessor("preprocess")
                .build_chunker("chunking")
                .build_embedder("embedding")
                .build_target("target")
                .get_pipeline())

    executor = PipelineExecutor(pipeline)
    executor.execute()

if __name__ == "__main__":
    main()
```

---

## Test Cases (using pytest)

Below are example tests for each module. These tests are intentionally minimal; you would normally add more robust tests.

### 42. `tests/test_config.py`

```python
# nlp_etl_pipeline/tests/test_config.py

from config.config_manager import ConfigManager
from config.file_config import FileConfig

def test_config_manager_singleton():
    cm1 = ConfigManager()
    cm2 = ConfigManager()
    assert cm1 is cm2

def test_file_config():
    fc = FileConfig()
    assert fc.get_param("file_path") is not None
```

### 43. `tests/test_datasource.py`

```python
# nlp_etl_pipeline/tests/test_datasource.py

from config.file_config import FileConfig
from data_source.datasource_factory import DataSourceFactory

def test_file_datasource():
    config = FileConfig()
    ds = DataSourceFactory.get_data_source("file", config)
    # Here you might mock Spark session; for now we simply verify instance type.
    assert ds.__class__.__name__ == "FileSource"
```

### 44. `tests/test_preprocessing.py`

```python
# nlp_etl_pipeline/tests/test_preprocessing.py

from pyspark.sql import SparkSession
import tempfile
from preprocessing.data_cleaner import DataCleaner
from pyspark.sql.functions import lit

def test_data_cleaner():
    spark = SparkSession.builder.appName("TestPreprocessing").getOrCreate()
    data = [("  Hello World  ",)]
    df = spark.createDataFrame(data, ["text"])
    cleaner = DataCleaner()
    cleaned_df = cleaner.clean(df)
    result = cleaned_df.collect()[0][0]
    assert result == "hello world"
```

### 45. `tests/test_chunking.py`

```python
# nlp_etl_pipeline/tests/test_chunking.py

from config.chunking_config import ChunkingConfig
from chunking.chunking_manager import ChunkingManager

def test_simple_chunker():
    config = ChunkingConfig()
    # Set simple strategy and small chunk size for testing.
    config.set_param("strategy", "simple")
    config.set_param("chunk_size", 5)
    manager = ChunkingManager(config)
    chunks = manager.chunk_text("abcdefghij")
    assert chunks == ['abcde', 'fghij']
```

### 46. `tests/test_embedding.py`

```python
# nlp_etl_pipeline/tests/test_embedding.py

from pyspark.sql import SparkSession
from config.embedding_config import EmbeddingConfig
from embedding.embedder_factory import EmbedderFactory

def test_tfidf_embedder():
    spark = SparkSession.builder.appName("TestEmbedder").getOrCreate()
    data = [("This is a test",)]
    df = spark.createDataFrame(data, ["text"])
    config = EmbeddingConfig()
    config.set_param("method", "tfidf")
    embedder = EmbedderFactory.get_embedder(config)
    result_df = embedder.embed(df)
    assert "features" in result_df.columns
```

### 47. `tests/test_target.py`

```python
# nlp_etl_pipeline/tests/test_target.py

from config.target_config import TargetConfig
from target.target_factory import TargetFactory
from pyspark.sql import SparkSession

def test_file_target():
    spark = SparkSession.builder.appName("TestTarget").getOrCreate()
    data = [("test",)]
    df = spark.createDataFrame(data, ["col"])
    config = TargetConfig()
    config.set_param("target_system", "file")
    config.set_param("file_path", "/tmp/test_output")
    config.set_param("file_format", "csv")
    target = TargetFactory.get_target(config)
    # This test will try to write to file; in real unit tests, this would be mocked.
    target.write(df)
    # If no exception is raised, we consider it successful.
    assert True
```

---

## `requirements.txt`

```plaintext
pyspark
pytest
beautifulsoup4
```

You may also need connectors or client libraries for MongoDB, PostgreSQL, and Neo4j depending on your target systems.

---

## `README.md`

```markdown
# NLP ETL Pipeline with PySpark

## Project Overview

This project implements an NLP ETL pipeline in PySpark. The pipeline is modular and supports reading from various data sources (Hive, file, RDBMS, NoSQL, VectorDB), preprocessing (HTML parsing, data cleaning), chunking text data, creating vector embeddings (TF-IDF and sentence embeddings), and writing to different targets. It is designed using five design patterns:
- **Singleton** (for configuration management)
- **Factory** (for data source, embedding, and target selection)
- **Strategy** (for chunking methods)
- **Template Method** (for preprocessing)
- **Builder** (for assembling the pipeline)

The pipeline is also extensible so that you can create different domain-specific pipelines (ADMIN, HR, FINANCE, IT HELPDESK) by varying the configurations.

## Project Structure

```
nlp_etl_pipeline/
├── config/                # Configuration classes (Python class based)
├── data_source/           # Data source modules and factory for source selection
├── preprocessing/         # Preprocessing module using Template Method pattern
├── chunking/              # Chunking strategies using Strategy pattern
├── embedding/             # Embedding modules and factory for embedding methods
├── target/                # Target modules and factory for target systems
├── logging_module/        # Custom logging module
├── exception_module/      # Custom exception classes for ETL errors
├── pipeline/              # Pipeline builder (Builder pattern) & executor
├── tests/                 # Pytest test cases for each module
├── requirements.txt       # Required Python packages
├── README.md              # This file
└── main.py                # Entry point to execute the ETL pipeline
```

## Setup Instructions

1. **Clone the repository**  
   ```bash
   git clone <repository_url>
   cd nlp_etl_pipeline
   ```

2. **Create a virtual environment** (optional but recommended)  
   ```bash
   python -m venv venv
   source venv/bin/activate   # Linux/Mac
   venv\Scripts\activate      # Windows
   ```

3. **Install the dependencies**  
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the Tests**  
   ```bash
   pytest tests/
   ```

5. **Execute the Pipeline**  
   Customize the configuration files under the `config/` folder as per your environment. Then run:
   ```bash
   python main.py
   ```

## Extending the Pipeline

- **Creating Domain-specific Pipelines:**  
  Register different configuration instances in `main.py` (or a domain-specific launcher) and build your pipeline accordingly.

- **Adding New Data Sources or Targets:**  
  Implement a new source/target module that inherits from `BaseDataSource`/`BaseTarget` and add it to the respective factory.

- **Implementing New Chunking or Embedding Strategies:**  
  Create a new Strategy implementation (for chunking) or a new embedder class (for embeddings) and update the corresponding factory to support it.

## Notes

- This project uses only the Singleton, Factory, Strategy, Template Method, and Builder design patterns.
- Replace placeholder logic with actual Spark operations and connectors as needed.
```

---

This complete project provides a highly configurable and extensible NLP ETL pipeline using PySpark. You can adjust and extend each module based on your data systems and specific use cases.


222

Below is a complete implementation of the NLP ETL pipeline project. This implementation uses PySpark and is organized into multiple modules. It follows the five design patterns you requested:  
- **Singleton Pattern** (for configuration management)  
- **Factory Pattern** (for instantiating data sources, embedders, and targets)  
- **Strategy Pattern** (for the chunking approaches)  
- **Template Method Pattern** (for preprocessing modules)  
- **Builder Pattern** (for assembling the complete ETL pipeline)  

In the code below, each file is presented with its relative path and full functionality. (Keep in mind that “full functionality” here means that the code is complete and runnable but many sections use simplified/dummy implementations which you would replace with production logic in a real environment.)  

---

### Project Directory Structure

```
nlp_etl_pipeline/
├── config
│   ├── base_config.py
│   ├── file_config.py
│   ├── hive_config.py
│   ├── rdbms_config.py
│   ├── nosql_config.py
│   ├── vectordb_config.py
│   ├── preprocess_config.py
│   ├── chunking_config.py
│   ├── embedding_config.py
│   ├── target_config.py
│   └── config_manager.py
├── data_source
│   ├── base_datasource.py
│   ├── hive_source.py
│   ├── file_source.py
│   ├── rdbms_source.py
│   ├── nosql_source.py
│   ├── vectordb_source.py
│   └── datasource_factory.py
├── preprocessing
│   ├── preprocessing_template.py
│   ├── html_parser.py
│   └── data_cleaner.py
├── chunking
│   ├── base_chunker.py
│   ├── chunk_strategy_simple.py
│   ├── chunk_strategy_overlap.py
│   └── chunking_manager.py
├── embedding
│   ├── base_embedder.py
│   ├── tfidf_embedder.py
│   ├── sentence_embedder.py
│   └── embedder_factory.py
├── target
│   ├── base_target.py
│   ├── hive_target.py
│   ├── file_target.py
│   ├── rdbms_target.py
│   ├── nosql_target.py
│   ├── vectordb_target.py
│   └── target_factory.py
├── logging_module
│   └── logger.py
├── exception_module
│   └── custom_exceptions.py
├── pipeline
│   ├── pipeline_builder.py
│   └── pipeline_executor.py
├── tests
│   ├── test_config.py
│   ├── test_datasource.py
│   ├── test_preprocessing.py
│   ├── test_chunking.py
│   ├── test_embedding.py
│   └── test_target.py
├── requirements.txt
├── README.md
└── main.py
```

---

Below is the complete code for each file:

---

#### **File: nlp_etl_pipeline/config/base_config.py**

```python
# nlp_etl_pipeline/config/base_config.py

class BaseConfig:
    def __init__(self):
        self.params = {}

    def get_param(self, key):
        return self.params.get(key)

    def set_param(self, key, value):
        self.params[key] = value
```

---

#### **File: nlp_etl_pipeline/config/file_config.py**

```python
# nlp_etl_pipeline/config/file_config.py

from .base_config import BaseConfig

class FileConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "file_path": "data/input/sample.txt",
            "file_format": "txt"  # could be txt, json, html, etc.
        }
```

---

#### **File: nlp_etl_pipeline/config/hive_config.py**

```python
# nlp_etl_pipeline/config/hive_config.py

from .base_config import BaseConfig

class HiveConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "hive_table": "default.input_table",
            "hive_database": "default"
        }
```

---

#### **File: nlp_etl_pipeline/config/rdbms_config.py**

```python
# nlp_etl_pipeline/config/rdbms_config.py

from .base_config import BaseConfig

class RDBMSConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "jdbc_url": "jdbc:mysql://localhost:3306/db",
            "db_table": "source_table",
            "user": "username",
            "password": "password"
        }
```

---

#### **File: nlp_etl_pipeline/config/nosql_config.py**

```python
# nlp_etl_pipeline/config/nosql_config.py

from .base_config import BaseConfig

class NoSQLConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "mongo_uri": "mongodb://localhost:27017",
            "collection": "source_collection"
        }
```

---

#### **File: nlp_etl_pipeline/config/vectordb_config.py**

```python
# nlp_etl_pipeline/config/vectordb_config.py

from .base_config import BaseConfig

class VectorDBConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "vector_db_type": "ChromaDB",  # options: "ChromaDB", "PostgreSQL", "Neo4j"
            "host": "localhost",
            "port": 1234
        }
```

---

#### **File: nlp_etl_pipeline/config/preprocess_config.py**

```python
# nlp_etl_pipeline/config/preprocess_config.py

from .base_config import BaseConfig

class PreprocessConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "remove_html_tags": True,
            "lowercase": True,
            "strip_whitespace": True
        }
```

---

#### **File: nlp_etl_pipeline/config/chunking_config.py**

```python
# nlp_etl_pipeline/config/chunking_config.py

from .base_config import BaseConfig

class ChunkingConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "strategy": "simple",  # options: "simple", "overlap"
            "chunk_size": 100,
            "overlap": 10
        }
```

---

#### **File: nlp_etl_pipeline/config/embedding_config.py**

```python
# nlp_etl_pipeline/config/embedding_config.py

from .base_config import BaseConfig

class EmbeddingConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "method": "tfidf",  # options: "tfidf", "sentence"
            "max_features": 5000
        }
```

---

#### **File: nlp_etl_pipeline/config/target_config.py**

```python
# nlp_etl_pipeline/config/target_config.py

from .base_config import BaseConfig

class TargetConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.params = {
            "target_system": "hive",  # options: hive, file, rdbms, nosql, vectordb
            "load_type": "full",      # options: full, scd2, cdc
            # For file target
            "file_path": "data/output/result",
            "file_format": "csv",
            # For RDBMS target
            "jdbc_url": "jdbc:mysql://localhost:3306/db",
            "db_table": "target_table",
            "user": "username",
            "password": "password",
            # For hive target
            "target_table": "default.output_table"
        }
```

---

#### **File: nlp_etl_pipeline/config/config_manager.py**  
*(Singleton Pattern)*

```python
# nlp_etl_pipeline/config/config_manager.py

from threading import Lock

class ConfigManager:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if not cls._instance:
                cls._instance = super(ConfigManager, cls).__new__(cls)
                cls._instance._configs = {}
            return cls._instance

    def register_config(self, name, config):
        self._configs[name] = config

    def get_config(self, name):
        return self._configs.get(name)
```

---

#### **File: nlp_etl_pipeline/data_source/base_datasource.py**

```python
# nlp_etl_pipeline/data_source/base_datasource.py

from pyspark.sql import SparkSession

class BaseDataSource:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder.appName("NLP_ETL").getOrCreate()

    def read(self):
        raise NotImplementedError("Subclasses must implement the read method.")
```

---

#### **File: nlp_etl_pipeline/data_source/hive_source.py**

```python
# nlp_etl_pipeline/data_source/hive_source.py

from .base_datasource import BaseDataSource

class HiveSource(BaseDataSource):
    def read(self):
        table = self.config.get_param("hive_table")
        # Read from Hive table using Spark SQL.
        df = self.spark.sql(f"SELECT * FROM {table}")
        return df
```

---

#### **File: nlp_etl_pipeline/data_source/file_source.py**

```python
# nlp_etl_pipeline/data_source/file_source.py

from .base_datasource import BaseDataSource

class FileSource(BaseDataSource):
    def read(self):
        path = self.config.get_param("file_path")
        fmt = self.config.get_param("file_format")
        if fmt == "json":
            df = self.spark.read.json(path)
        elif fmt == "html":
            # Read as plain text and assume HTML parsing later.
            df = self.spark.read.text(path).withColumnRenamed("value", "html")
        else:
            df = self.spark.read.text(path).withColumnRenamed("value", "text")
        return df
```

---

#### **File: nlp_etl_pipeline/data_source/rdbms_source.py**

```python
# nlp_etl_pipeline/data_source/rdbms_source.py

from .base_datasource import BaseDataSource

class RDBMSSource(BaseDataSource):
    def read(self):
        params = self.config.params
        df = self.spark.read.format("jdbc") \
                .option("url", params["jdbc_url"]) \
                .option("dbtable", params["db_table"]) \
                .option("user", params["user"]) \
                .option("password", params["password"]) \
                .load()
        return df
```

---

#### **File: nlp_etl_pipeline/data_source/nosql_source.py**

```python
# nlp_etl_pipeline/data_source/nosql_source.py

from .base_datasource import BaseDataSource
from pyspark.sql.types import StructType

class NoSQLSource(BaseDataSource):
    def read(self):
        # In a full implementation, use a connector (e.g., pymongo) to fetch data.
        # Here we simulate with an empty DataFrame.
        print("Reading data from NoSQL source")
        schema = StructType([])
        return self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)
```

---

#### **File: nlp_etl_pipeline/data_source/vectordb_source.py**

```python
# nlp_etl_pipeline/data_source/vectordb_source.py

from .base_datasource import BaseDataSource
from pyspark.sql.types import StructType

class VectorDBSource(BaseDataSource):
    def read(self):
        # In production, connect to the vector DB and load data.
        print("Reading data from Vector DB source")
        schema = StructType([])
        return self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)
```

---

#### **File: nlp_etl_pipeline/data_source/datasource_factory.py**  
*(Factory Pattern)*

```python
# nlp_etl_pipeline/data_source/datasource_factory.py

from .hive_source import HiveSource
from .file_source import FileSource
from .rdbms_source import RDBMSSource
from .nosql_source import NoSQLSource
from .vectordb_source import VectorDBSource

class DataSourceFactory:
    @staticmethod
    def get_data_source(source_type, config):
        if source_type == "hive":
            return HiveSource(config)
        elif source_type == "file":
            return FileSource(config)
        elif source_type == "rdbms":
            return RDBMSSource(config)
        elif source_type == "nosql":
            return NoSQLSource(config)
        elif source_type == "vectordb":
            return VectorDBSource(config)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
```

---

#### **File: nlp_etl_pipeline/preprocessing/preprocessing_template.py**  
*(Template Method Pattern)*

```python
# nlp_etl_pipeline/preprocessing/preprocessing_template.py

class PreprocessingTemplate:
    def process(self, data):
        data = self.parse(data)
        data = self.clean(data)
        return data

    def parse(self, data):
        raise NotImplementedError("Subclasses must implement the parse method.")

    def clean(self, data):
        raise NotImplementedError("Subclasses must implement the clean method.")
```

---

#### **File: nlp_etl_pipeline/preprocessing/html_parser.py**

```python
# nlp_etl_pipeline/preprocessing/html_parser.py

from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from .preprocessing_template import PreprocessingTemplate

class HTMLParser(PreprocessingTemplate):
    def parse(self, data):
        # UDF to extract text from HTML.
        def extract_text(html):
            return BeautifulSoup(html, 'html.parser').get_text() if html else ""
        extract_udf = udf(extract_text, StringType())
        if "html" in data.columns:
            data = data.withColumn("parsed_text", extract_udf(data["html"]))
        return data
    
    def clean(self, data):
        # No additional cleaning for HTML; you might add further logic.
        return data
```

---

#### **File: nlp_etl_pipeline/preprocessing/data_cleaner.py**

```python
# nlp_etl_pipeline/preprocessing/data_cleaner.py

from pyspark.sql.functions import lower, trim
from .preprocessing_template import PreprocessingTemplate

class DataCleaner(PreprocessingTemplate):
    def parse(self, data):
        # No parsing step for cleaning, pass data through.
        return data

    def clean(self, data):
        # Lowercase and trim all string columns.
        for col in data.columns:
            data = data.withColumn(col, lower(trim(data[col])))
        return data
```

---

#### **File: nlp_etl_pipeline/chunking/base_chunker.py**

```python
# nlp_etl_pipeline/chunking/base_chunker.py

class BaseChunker:
    def __init__(self, config):
        self.config = config

    def chunk(self, text):
        raise NotImplementedError("Subclasses must implement chunk()")
```

---

#### **File: nlp_etl_pipeline/chunking/chunk_strategy_simple.py**  
*(Strategy Pattern Implementation)*

```python
# nlp_etl_pipeline/chunking/chunk_strategy_simple.py

from .base_chunker import BaseChunker

class SimpleChunker(BaseChunker):
    def chunk(self, text):
        chunk_size = self.config.get_param("chunk_size")
        return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
```

---

#### **File: nlp_etl_pipeline/chunking/chunk_strategy_overlap.py**

```python
# nlp_etl_pipeline/chunking/chunk_strategy_overlap.py

from .base_chunker import BaseChunker

class OverlapChunker(BaseChunker):
    def chunk(self, text):
        chunk_size = self.config.get_param("chunk_size")
        overlap = self.config.get_param("overlap")
        chunks = []
        i = 0
        while i < len(text):
            chunk = text[i:i+chunk_size]
            chunks.append(chunk)
            i += chunk_size - overlap
        return chunks
```

---

#### **File: nlp_etl_pipeline/chunking/chunking_manager.py**  
*(Selects the appropriate Strategy)*

```python
# nlp_etl_pipeline/chunking/chunking_manager.py

from .chunk_strategy_simple import SimpleChunker
from .chunk_strategy_overlap import OverlapChunker

class ChunkingManager:
    def __init__(self, config):
        self.config = config
        self.strategy = self._get_strategy(config.get_param("strategy"))

    def _get_strategy(self, strategy_name):
        if strategy_name == "simple":
            return SimpleChunker(self.config)
        elif strategy_name == "overlap":
            return OverlapChunker(self.config)
        else:
            raise ValueError("Unknown chunking strategy")

    def chunk_text(self, text):
        # You may add a smoothing process here if needed.
        return self.strategy.chunk(text)
```

---

#### **File: nlp_etl_pipeline/embedding/base_embedder.py**

```python
# nlp_etl_pipeline/embedding/base_embedder.py

class BaseEmbedder:
    def __init__(self, config):
        self.config = config

    def embed(self, text_df):
        raise NotImplementedError("Subclasses must implement embed()")
```

---

#### **File: nlp_etl_pipeline/embedding/tfidf_embedder.py**

```python
# nlp_etl_pipeline/embedding/tfidf_embedder.py

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from .base_embedder import BaseEmbedder

class TfIdfEmbedder(BaseEmbedder):
    def embed(self, text_df):
        # Assume a column named "text" exists in text_df.
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        words_data = tokenizer.transform(text_df)
        num_features = self.config.get_param("max_features")
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=num_features)
        featurizedData = hashingTF.transform(words_data)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)
        return rescaledData
```

---

#### **File: nlp_etl_pipeline/embedding/sentence_embedder.py**

```python
# nlp_etl_pipeline/embedding/sentence_embedder.py

from pyspark.sql.functions import lit
from .base_embedder import BaseEmbedder

class SentenceEmbedder(BaseEmbedder):
    def embed(self, text_df):
        # In a production system, apply a trained sentence embedding model.
        # Here we add a dummy embedding column.
        return text_df.withColumn("embedding", lit("dummy_sentence_embedding"))
```

---

#### **File: nlp_etl_pipeline/embedding/embedder_factory.py**  
*(Factory Pattern)*

```python
# nlp_etl_pipeline/embedding/embedder_factory.py

from .tfidf_embedder import TfIdfEmbedder
from .sentence_embedder import SentenceEmbedder

class EmbedderFactory:
    @staticmethod
    def get_embedder(config):
        method = config.get_param("method")
        if method == "tfidf":
            return TfIdfEmbedder(config)
        elif method == "sentence":
            return SentenceEmbedder(config)
        else:
            raise ValueError("Unknown embedding method")
```

---

#### **File: nlp_etl_pipeline/target/base_target.py**

```python
# nlp_etl_pipeline/target/base_target.py

class BaseTarget:
    def __init__(self, config):
        self.config = config

    def write(self, df):
        raise NotImplementedError("Subclasses must implement write()")
```

---

#### **File: nlp_etl_pipeline/target/hive_target.py**

```python
# nlp_etl_pipeline/target/hive_target.py

from .base_target import BaseTarget

class HiveTarget(BaseTarget):
    def write(self, df):
        table = self.config.get_param("target_table") or "default_output_table"
        # Write DataFrame as Hive table.
        df.write.mode("overwrite").saveAsTable(table)
```

---

#### **File: nlp_etl_pipeline/target/file_target.py**

```python
# nlp_etl_pipeline/target/file_target.py

from .base_target import BaseTarget

class FileTarget(BaseTarget):
    def write(self, df):
        path = self.config.get_param("file_path") or "data/output/result"
        fmt = self.config.get_param("file_format") or "csv"
        df.write.mode("overwrite").format(fmt).save(path)
```

---

#### **File: nlp_etl_pipeline/target/rdbms_target.py**

```python
# nlp_etl_pipeline/target/rdbms_target.py

from .base_target import BaseTarget

class RDBMSTarget(BaseTarget):
    def write(self, df):
        params = self.config.params
        df.write.format("jdbc") \
          .option("url", params.get("jdbc_url")) \
          .option("dbtable", params.get("db_table")) \
          .option("user", params.get("user")) \
          .option("password", params.get("password")) \
          .mode("append") \
          .save()
```

---

#### **File: nlp_etl_pipeline/target/nosql_target.py**

```python
# nlp_etl_pipeline/target/nosql_target.py

from .base_target import BaseTarget

class NoSQLTarget(BaseTarget):
    def write(self, df):
        # In a full solution, write to a NoSQL database.
        print("Writing data to NoSQL target")
```

---

#### **File: nlp_etl_pipeline/target/vectordb_target.py**

```python
# nlp_etl_pipeline/target/vectordb_target.py

from .base_target import BaseTarget

class VectorDBTarget(BaseTarget):
    def write(self, df):
        # In production, write to the vector database.
        print("Writing data to VectorDB target")
```

---

#### **File: nlp_etl_pipeline/target/target_factory.py**  
*(Factory Pattern)*

```python
# nlp_etl_pipeline/target/target_factory.py

from .hive_target import HiveTarget
from .file_target import FileTarget
from .rdbms_target import RDBMSTarget
from .nosql_target import NoSQLTarget
from .vectordb_target import VectorDBTarget

class TargetFactory:
    @staticmethod
    def get_target(config):
        target_system = config.get_param("target_system")
        if target_system == "hive":
            return HiveTarget(config)
        elif target_system == "file":
            return FileTarget(config)
        elif target_system == "rdbms":
            return RDBMSTarget(config)
        elif target_system == "nosql":
            return NoSQLTarget(config)
        elif target_system == "vectordb":
            return VectorDBTarget(config)
        else:
            raise ValueError("Unknown target system")
```

---

#### **File: nlp_etl_pipeline/logging_module/logger.py**

```python
# nlp_etl_pipeline/logging_module/logger.py

import logging

def get_logger(name=__name__):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
```

---

#### **File: nlp_etl_pipeline/exception_module/custom_exceptions.py**

```python
# nlp_etl_pipeline/exception_module/custom_exceptions.py

class ETLException(Exception):
    pass

class ConfigException(ETLException):
    pass

class DataSourceException(ETLException):
    pass

class TargetException(ETLException):
    pass
```

---

#### **File: nlp_etl_pipeline/pipeline/pipeline_builder.py**  
*(Builder Pattern)*

```python
# nlp_etl_pipeline/pipeline/pipeline_builder.py

from config.config_manager import ConfigManager
from data_source.datasource_factory import DataSourceFactory
from preprocessing.data_cleaner import DataCleaner
from chunking.chunking_manager import ChunkingManager
from embedding.embedder_factory import EmbedderFactory
from target.target_factory import TargetFactory
from logging_module.logger import get_logger

class PipelineBuilder:
    def __init__(self):
        self.logger = get_logger("PipelineBuilder")
        self.config_manager = ConfigManager()
        self.source = None
        self.preprocessor = None
        self.chunker = None
        self.embedder = None
        self.target = None

    def build_source(self, source_type, config_name):
        config = self.config_manager.get_config(config_name)
        self.source = DataSourceFactory.get_data_source(source_type, config)
        self.logger.info(f"Source '{source_type}' built with config '{config_name}'")
        return self

    def build_preprocessor(self, config_name, preprocessor_type="cleaner"):
        # Here you could expand to choose different preprocessors.
        self.preprocessor = DataCleaner()
        self.logger.info("Preprocessor (DataCleaner) built")
        return self

    def build_chunker(self, config_name):
        config = self.config_manager.get_config(config_name)
        self.chunker = ChunkingManager(config)
        self.logger.info("Chunking module built")
        return self

    def build_embedder(self, config_name):
        config = self.config_manager.get_config(config_name)
        self.embedder = EmbedderFactory.get_embedder(config)
        self.logger.info("Embedding module built")
        return self

    def build_target(self, config_name):
        config = self.config_manager.get_config(config_name)
        self.target = TargetFactory.get_target(config)
        self.logger.info("Target module built")
        return self

    def get_pipeline(self):
        if not all([self.source, self.preprocessor, self.chunker, self.embedder, self.target]):
            raise Exception("Pipeline is not completely built.")
        return {
            "source": self.source,
            "preprocessor": self.preprocessor,
            "chunker": self.chunker,
            "embedder": self.embedder,
            "target": self.target
        }
```

---

#### **File: nlp_etl_pipeline/pipeline/pipeline_executor.py**

```python
# nlp_etl_pipeline/pipeline/pipeline_executor.py

from logging_module.logger import get_logger
from pyspark.sql.functions import col

class PipelineExecutor:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = get_logger("PipelineExecutor")

    def execute(self):
        self.logger.info("Pipeline execution started.")
        
        # 1. Read data from the source
        df = self.pipeline["source"].read()
        self.logger.info("Data read from source.")
        
        # 2. Preprocess the data (parse and clean)
        df = self.pipeline["preprocessor"].process(df)
        self.logger.info("Data preprocessing completed.")
        
        # 3. If a column 'text' exists, perform chunking
        if "text" in df.columns:
            texts = df.select("text").rdd.flatMap(lambda row: row)
            chunked_texts = texts.map(lambda txt: self.pipeline["chunker"].chunk_text(txt)).collect()
            self.logger.info(f"Chunked data sample: {chunked_texts[:2]}")
        
        # 4. Create embeddings
        df = self.pipeline["embedder"].embed(df)
        self.logger.info("Embedding creation completed.")
        
        # 5. Write to target
        self.pipeline["target"].write(df)
        self.logger.info("Data written to target.")
        self.logger.info("Pipeline execution finished.")
```

---

#### **File: nlp_etl_pipeline/main.py**

```python
# nlp_etl_pipeline/main.py

from config.config_manager import ConfigManager
from config.file_config import FileConfig
from config.hive_config import HiveConfig
from config.rdbms_config import RDBMSConfig
from config.nosql_config import NoSQLConfig
from config.vectordb_config import VectorDBConfig
from config.preprocess_config import PreprocessConfig
from config.chunking_config import ChunkingConfig
from config.embedding_config import EmbeddingConfig
from config.target_config import TargetConfig
from pipeline.pipeline_builder import PipelineBuilder
from pipeline.pipeline_executor import PipelineExecutor
from logging_module.logger import get_logger

def main():
    logger = get_logger("Main")
    cm = ConfigManager()
    
    # Register configurations
    cm.register_config("file_source", FileConfig())
    cm.register_config("hive_source", HiveConfig())
    cm.register_config("rdbms_source", RDBMSConfig())
    cm.register_config("nosql_source", NoSQLConfig())
    cm.register_config("vectordb_source", VectorDBConfig())
    cm.register_config("preprocess", PreprocessConfig())
    cm.register_config("chunking", ChunkingConfig())
    cm.register_config("embedding", EmbeddingConfig())
    cm.register_config("target", TargetConfig())
    
    # Build and execute pipeline.
    builder = PipelineBuilder()
    pipeline = (builder
                .build_source("file", "file_source")
                .build_preprocessor("preprocess")
                .build_chunker("chunking")
                .build_embedder("embedding")
                .build_target("target")
                .get_pipeline())
    
    executor = PipelineExecutor(pipeline)
    executor.execute()

if __name__ == "__main__":
    main()
```

---

#### **File: nlp_etl_pipeline/tests/test_config.py**

```python
# nlp_etl_pipeline/tests/test_config.py

from config.config_manager import ConfigManager
from config.file_config import FileConfig

def test_config_manager_singleton():
    cm1 = ConfigManager()
    cm2 = ConfigManager()
    assert cm1 is cm2

def test_file_config():
    fc = FileConfig()
    assert fc.get_param("file_path") is not None
```

---

#### **File: nlp_etl_pipeline/tests/test_datasource.py**

```python
# nlp_etl_pipeline/tests/test_datasource.py

from config.file_config import FileConfig
from data_source.datasource_factory import DataSourceFactory

def test_file_datasource():
    config = FileConfig()
    ds = DataSourceFactory.get_data_source("file", config)
    from data_source.file_source import FileSource
    assert isinstance(ds, FileSource)
```

---

#### **File: nlp_etl_pipeline/tests/test_preprocessing.py**

```python
# nlp_etl_pipeline/tests/test_preprocessing.py

import tempfile
from pyspark.sql import SparkSession
from preprocessing.data_cleaner import DataCleaner

def test_data_cleaner():
    spark = SparkSession.builder.appName("TestPreprocessing").getOrCreate()
    data = [("  Hello World  ",)]
    df = spark.createDataFrame(data, ["text"])
    cleaner = DataCleaner()
    cleaned_df = cleaner.clean(df)
    result = cleaned_df.collect()[0]["text"]
    assert result == "hello world"
    spark.stop()
```

---

#### **File: nlp_etl_pipeline/tests/test_chunking.py**

```python
# nlp_etl_pipeline/tests/test_chunking.py

from config.chunking_config import ChunkingConfig
from chunking.chunking_manager import ChunkingManager

def test_simple_chunker():
    config = ChunkingConfig()
    config.set_param("strategy", "simple")
    config.set_param("chunk_size", 5)
    manager = ChunkingManager(config)
    chunks = manager.chunk_text("abcdefghij")
    assert chunks == ['abcde', 'fghij']
```

---

#### **File: nlp_etl_pipeline/tests/test_embedding.py**

```python
# nlp_etl_pipeline/tests/test_embedding.py

from pyspark.sql import SparkSession
from config.embedding_config import EmbeddingConfig
from embedding.embedder_factory import EmbedderFactory

def test_tfidf_embedder():
    spark = SparkSession.builder.appName("TestEmbedder").getOrCreate()
    data = [("This is a test",)]
    df = spark.createDataFrame(data, ["text"])
    config = EmbeddingConfig()
    config.set_param("method", "tfidf")
    embedder = EmbedderFactory.get_embedder(config)
    result_df = embedder.embed(df)
    # Check that the resulting DataFrame contains the "features" column.
    assert "features" in result_df.columns
    spark.stop()
```

---

#### **File: nlp_etl_pipeline/tests/test_target.py**

```python
# nlp_etl_pipeline/tests/test_target.py

from config.target_config import TargetConfig
from target.target_factory import TargetFactory
from pyspark.sql import SparkSession

def test_file_target():
    spark = SparkSession.builder.appName("TestTarget").getOrCreate()
    data = [("test",)]
    df = spark.createDataFrame(data, ["col"])
    config = TargetConfig()
    config.set_param("target_system", "file")
    config.set_param("file_path", "data/output/test_result")
    config.set_param("file_format", "csv")
    target = TargetFactory.get_target(config)
    # Attempt to write to file (in production, use a temporary directory or mocks)
    target.write(df)
    spark.stop()
```

---

#### **File: nlp_etl_pipeline/requirements.txt**

```plaintext
pyspark
pytest
beautifulsoup4
```

---

#### **File: nlp_etl_pipeline/README.md**

```markdown
# NLP ETL Pipeline with PySpark

## Overview

This project implements a modular NLP ETL pipeline using PySpark. It supports:
- Reading from multiple data sources (Hive, file, RDBMS, NoSQL, VectorDB)
- Preprocessing text data (HTML parsing, data cleaning)
- Chunking text using different strategies (simple, overlap)
- Creating embeddings (TF-IDF, sentence embeddings)
- Writing the output to various target systems (Hive, file, RDBMS, NoSQL, VectorDB)

The project is designed using the following design patterns:
- **Singleton Pattern** (for configuration management)
- **Factory Pattern** (for data source, embedding, and target selection)
- **Strategy Pattern** (for chunking strategies)
- **Template Method Pattern** (for the preprocessing module)
- **Builder Pattern** (for pipeline construction)

## Directory Structure

```
nlp_etl_pipeline/
├── config/                # Python class-based configuration files
├── data_source/           # Data source modules and factory for source selection
├── preprocessing/         # Preprocessing (HTML parsing, cleaning)
├── chunking/              # Chunking strategies (simple and overlapping)
├── embedding/             # Embedding modules (TF-IDF and sentence embeddings)
├── target/                # Target modules and factory for target systems
├── logging_module/        # Custom logging setup
├── exception_module/      # Custom exception classes for the pipeline
├── pipeline/              # Pipeline builder and executor using Builder pattern
├── tests/                 # Test cases using pytest
├── requirements.txt       # Python dependencies
├── README.md              # Project instructions
└── main.py                # Main entry point to run the pipeline
```

## Setup Instructions

1. **Clone the repository**

   ```bash
   git clone <repository_url>
   cd nlp_etl_pipeline
   ```

2. **Create and activate a virtual environment** (optional but recommended)

   ```bash
   python -m venv venv
   source venv/bin/activate   # Linux/Mac
   venv\Scripts\activate      # Windows
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Run the Tests**

   ```bash
   pytest tests/
   ```

5. **Execute the Pipeline**

   Customize configurations under the `config/` directory if needed, then run:

   ```bash
   python main.py
   ```

## Extending the Pipeline

- **Domain-Specific Pipelines:**  
  Create or register different configuration instances for domains like ADMIN, HR, FINANCE, or IT HELPDESK in `main.py` or a separate launcher script.

- **Adding Data Sources/Targets:**  
  Create new modules inheriting from `BaseDataSource` or `BaseTarget` and update the corresponding factory.

- **New Chunking/Embedding Strategies:**  
  Implement new strategies or embedders and update the Strategy or Factory logic accordingly.

## Notes

- The current code includes dummy implementations for connectors (e.g., NoSQL and VectorDB). Replace these with actual implementations as needed.
- Logging is configured to use a simple console output.
- The ETL pipeline can be executed as a single module or in parts without modifying existing modules.
```

---

### Final Notes

This complete project codebase is fully self-contained. To run it successfully, ensure that you have a working Spark installation and that the directories (such as `data/input` and `data/output`) exist or are adjusted according to your environment. Replace any dummy implementations (such as connectors for NoSQL or vector databases) with your production logic as needed.

Feel free to extend, test, and modify the functionality based on your requirements. Enjoy building your NLP ETL pipeline!


333

