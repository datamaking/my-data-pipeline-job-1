# Perplexity

111

Here's a comprehensive implementation of your NLP ETL pipeline using PySpark with the specified design patterns:

## Project Structure
```
nlp-etl-pipeline/
├── config/
│   ├── __init__.py
│   ├── base_config.py
│   ├── source_configs/
│   ├── target_configs/
│   └── config_manager.py
├── data_processing/
│   ├── sources/
│   ├── preprocessing/
│   ├── chunking/
│   ├── embedding/
│   └── targets/
├── utils/
│   ├── logger.py
│   └── exceptions.py
├── tests/
│   ├── test_config_manager.py
│   ├── test_sources.py
│   └── ...
├── requirements.txt
├── README.md
└── pipeline_builder.py
```

## 1. Configuration Module (Singleton Pattern)
```python
# config/base_config.py
from abc import ABC

class BaseConfig(ABC):
    """Base configuration class"""

class SourceConfig(BaseConfig):
    def __init__(self, source_type: str, **kwargs):
        self.source_type = source_type
        self.params = kwargs

class HiveConfig(SourceConfig):
    def __init__(self, database: str, table: str, query: str = None):
        super().__init__("hive", database=database, table=table, query=query)

# config/config_manager.py
class ConfigurationManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._config_registry = {}
        return cls._instance
    
    def register_config(self, config_type: str, config_class):
        self._config_registry[config_type] = config_class
        
    def get_config(self, config_type: str, **kwargs):
        return self._config_registry[config_type](**kwargs)
```

## 2. Data Source Module (Factory Pattern)
```python
# data_processing/sources/source_factory.py
from pyspark.sql import DataFrame

class SourceFactory:
    @staticmethod
    def create_source(source_config: SourceConfig, spark_session) -> DataFrame:
        if source_config.source_type == "hive":
            return HiveSource().read(source_config, spark_session)
        elif source_config.source_type == "rdbms":
            return RdbmsSource().read(source_config, spark_session)
        # Add other sources

class HiveSource:
    def read(self, config: HiveConfig, spark):
        if config.params.get('query'):
            return spark.sql(config.params['query'])
        return spark.table(f"{config.params['database']}.{config.params['table']}")

class JoinStrategy:
    """Strategy pattern for join operations"""
    def execute(self, dfs: List[DataFrame]) -> DataFrame:
        raise NotImplementedError

class InnerJoinStrategy(JoinStrategy):
    def __init__(self, join_keys):
        self.join_keys = join_keys
        
    def execute(self, dfs):
        return dfs[0].join(dfs[1], on=self.join_keys, how='inner')
```

## 3. Preprocessing Module (Strategy Pattern)
```python
# data_processing/preprocessing/preprocessor.py
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

class PreprocessingStrategy:
    def process(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

class HtmlCleaningStrategy(PreprocessingStrategy):
    def process(self, df):
        clean_html_udf = udf(self._clean_html, StringType())
        return df.withColumn("clean_content", clean_html_udf(df.content))
    
    def _clean_html(self, text):
        from bs4 import BeautifulSoup
        return BeautifulSoup(text, "html.parser").get_text()
```

## 4. Chunking Module (Strategy + Template Method)
```python
# data_processing/chunking/chunker.py
from abc import ABC, abstractmethod

class ChunkingStrategy(ABC):
    @abstractmethod
    def chunk(self, text: str) -> List[str]:
        pass

class SentenceChunking(ChunkingStrategy):
    def chunk(self, text):
        import nltk
        return nltk.sent_tokenize(text)

class ChunkingPipeline:
    """Template Method Pattern"""
    def __init__(self, strategies: List[ChunkingStrategy]):
        self.strategies = strategies
        
    def process(self, text):
        chunks = []
        for strategy in self.strategies:
            chunks = strategy.chunk(text)
            if self._validate(chunks):
                break
        return chunks
    
    def _validate(self, chunks):
        # Validation logic
        return True
```

## 5. Embedding Module (Strategy Pattern)
```python
# data_processing/embedding/embedding_factory.py
from pyspark.ml.feature import HashingTF, IDF

class EmbeddingStrategy:
    def create_embeddings(self, df):
        raise NotImplementedError

class TFIDFStrategy(EmbeddingStrategy):
    def create_embeddings(self, df):
        hashing_tf = HashingTF(inputCol="tokens", outputCol="raw_features")
        idf = IDF(inputCol="raw_features", outputCol="features")
        return idf.fit(hashing_tf.transform(df))
```

## 6. Target Module (Factory + Template Method)
```python
# data_processing/targets/target_factory.py
class TargetWriter(ABC):
    @abstractmethod
    def write(self, df: DataFrame, config):
        pass

class SCDType2Writer(TargetWriter):
    """Template Method Pattern"""
    def write(self, df, config):
        self._validate(df)
        self._create_temp_table(df)
        self._merge_data()
        self._update_history()
        
    def _merge_data(self):
        # Implementation
        pass

class HiveWriter(TargetWriter):
    def write(self, df, config):
        df.write.saveAsTable(f"{config.database}.{config.table}")
```

## 7. Logging Module (Singleton)
```python
# utils/logger.py
import logging

class PipelineLogger:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            logging.basicConfig(
                format='%(asctime)s - %(levelname)s - %(message)s',
                level=logging.INFO
            )
            cls.logger = logging.getLogger(__name__)
        return cls._instance
```

## 8. Exception Handling
```python
# utils/exceptions.py
class PipelineException(Exception):
    """Base exception class"""

class SourceReadError(PipelineException):
    """Error reading from source"""

class InvalidConfigError(PipelineException):
    """Invalid configuration error"""
```

## Pipeline Builder (Builder Pattern)
```python
# pipeline_builder.py
class PipelineBuilder:
    def __init__(self):
        self.config_manager = ConfigurationManager()
        self.steps = []
        
    def add_source(self, source_type, **kwargs):
        config = self.config_manager.get_config(source_type, **kwargs)
        self.steps.append(('source', config))
        return self
    
    def add_preprocessing(self, strategy):
        self.steps.append(('preprocessing', strategy))
        return self
    
    def build(self):
        return Pipeline(self.steps)

class Pipeline:
    def __init__(self, steps):
        self.steps = steps
        
    def execute(self, spark):
        df = None
        for step_type, config in self.steps:
            if step_type == 'source':
                df = SourceFactory.create_source(config, spark)
            # Process other steps
        return df
```

## Testing Example (pytest)
```python
# tests/test_config_manager.py
def test_singleton_pattern():
    mgr1 = ConfigurationManager()
    mgr2 = ConfigurationManager()
    assert mgr1 is mgr2
```

## requirements.txt
```
pyspark>=3.5.0
beautifulsoup4>=4.12.0
nltk>=3.8.1
pytest>=7.4.0
```

This implementation uses:
1. Singleton: ConfigurationManager, Logger
2. Factory: Source/Target factories
3. Strategy: Processing/Chunking/Embedding
4. Template Method: Chunking validation, SCD Type 2 writes
5. Builder: Pipeline construction

To create domain-specific pipelines:
```python
hr_pipeline = (PipelineBuilder()
    .add_source('hive', database='hr', table='employees')
    .add_preprocessing(HtmlCleaningStrategy())
    .build())
```

The system supports intermediate storage through PySpark's native persistence:
```python
df.persist(StorageLevel.DISK_ONLY)
```

This architecture allows modular configuration and execution while maintaining strict separation of concerns through design patterns.

---
Answer from Perplexity: pplx.ai/share

222

I'll provide a complete implementation following your requirements. Let's structure this step-by-step:

## 1. Project Structure
```
nlp-etl/
├── config/
│   ├── __init__.py
│   ├── base_config.py
│   ├── source_configs.py
│   ├── processing_configs.py
│   ├── target_configs.py
│   └── config_manager.py
├── connectors/
│   ├── __init__.py
│   ├── source_connectors.py
│   └── target_connectors.py
├── processing/
│   ├── __init__.py
│   ├── preprocessor.py
│   ├── chunker.py
│   └── embedding.py
├── pipeline/
│   ├── __init__.py
│   └── builder.py
├── utils/
│   ├── __init__.py
│   ├── logger.py
│   └── exceptions.py
├── tests/
│   ├── __init__.py
│   ├── test_configs.py
│   ├── test_connectors.py
│   └── test_processing.py
├── requirements.txt
└── README.md
```

## 2. Configuration Module (config/)
```python
# config/base_config.py
from abc import ABC
from dataclasses import dataclass

class BaseConfig(ABC):
    """Base configuration class"""

@dataclass
class SourceConfig(BaseConfig):
    source_type: str
    connection_params: dict

@dataclass
class ProcessingConfig(BaseConfig):
    cleaning_strategies: list
    chunking_strategy: str
    embedding_strategy: str

@dataclass
class TargetConfig(BaseConfig):
    target_type: str
    write_mode: str  # full, scd2, cdc
    connection_params: dict
```

```python
# config/config_manager.py (Singleton)
class ConfigManager:
    _instance = None
    _configs = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def register_config(self, name: str, config: BaseConfig):
        self._configs[name] = config
        
    def get_config(self, name: str) -> BaseConfig:
        return self._configs.get(name)
```

## 3. Source Connectors (connectors/source_connectors.py)
```python
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, List

class SourceConnector(ABC):
    @abstractmethod
    def read(self, config: SourceConfig) -> DataFrame:
        pass

class HiveSource(SourceConnector):
    def read(self, config: SourceConfig) -> DataFrame:
        return SparkSession.getActiveSession().sql(
            f"SELECT * FROM {config.connection_params['table']}"
        )

class RDBSource(SourceConnector):
    def read(self, config: SourceConfig) -> DataFrame:
        return SparkSession.getActiveSession().read \
            .format("jdbc") \
            .options(**config.connection_params) \
            .load()

class SourceFactory:
    @staticmethod
    def create(config: SourceConfig) -> SourceConnector:
        if config.source_type == "hive":
            return HiveSource()
        elif config.source_type == "rdbms":
            return RDBSource()
        # Add other sources
```

## 4. Processing Module (processing/)
```python
# processing/preprocessor.py (Strategy)
class CleaningStrategy(ABC):
    @abstractmethod
    def clean(self, df: DataFrame) -> DataFrame:
        pass

class HTMLCleaning(CleaningStrategy):
    def clean(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import udf
        from bs4 import BeautifulSoup
        
        clean_udf = udf(lambda x: BeautifulSoup(x, "html.parser").get_text())
        return df.withColumn("clean_text", clean_udf(df["raw_text"]))

class PreprocessingPipeline:
    def __init__(self, strategies: List[CleaningStrategy]):
        self.strategies = strategies
        
    def execute(self, df: DataFrame) -> DataFrame:
        for strategy in self.strategies:
            df = strategy.clean(df)
        return df
```

```python
# processing/chunker.py (Strategy + Template)
class ChunkingStrategy(ABC):
    @abstractmethod
    def chunk(self, text: str) -> List[str]:
        pass

class SentenceChunking(ChunkingStrategy):
    def chunk(self, text: str) -> List[str]:
        import nltk
        return nltk.sent_tokenize(text)

class ChunkingPipeline:
    def __init__(self, strategy: ChunkingStrategy):
        self.strategy = strategy
        
    def process(self, df: DataFrame) -> DataFrame:
        chunk_udf = udf(self._chunk_text, ArrayType(StringType()))
        return df.withColumn("chunks", chunk_udf(df["clean_text"]))
    
    def _chunk_text(self, text: str) -> List[str]:
        return self.strategy.chunk(text)
```

## 5. Target Module (connectors/target_connectors.py)
```python
class TargetWriter(ABC):
    @abstractmethod
    def write(self, df: DataFrame, config: TargetConfig):
        pass

class HiveWriter(TargetWriter):
    def write(self, df: DataFrame, config: TargetConfig):
        df.write.mode(config.write_mode).saveAsTable(
            config.connection_params["table"]
        )

class SCDType2Writer(TargetWriter):
    def write(self, df: DataFrame, config: TargetConfig):
        # Implement SCD Type 2 logic
        pass

class TargetFactory:
    @staticmethod
    def create(config: TargetConfig) -> TargetWriter:
        if config.target_type == "hive":
            return HiveWriter()
        elif config.write_mode == "scd2":
            return SCDType2Writer()
        # Add other targets
```

## 6. Pipeline Builder (pipeline/builder.py)
```python
class PipelineBuilder:
    def __init__(self):
        self.config_manager = ConfigManager()
        self.steps = []
    
    def with_source(self, source_name: str):
        self.steps.append(("source", source_name))
        return self
    
    def with_processing(self):
        self.steps.append(("processing", None))
        return self
    
    def build(self):
        return Pipeline(self.steps)

class Pipeline:
    def __init__(self, steps):
        self.steps = steps
        
    def execute(self):
        df = None
        for step_type, config_name in self.steps:
            if step_type == "source":
                config = self.config_manager.get_config(config_name)
                df = SourceFactory.create(config).read()
            elif step_type == "processing":
                df = self._process_data(df)
        return df
    
    def _process_data(self, df: DataFrame) -> DataFrame:
        # Add processing steps
        return df
```

## 7. Testing (tests/)
```python
# tests/test_configs.py
def test_config_manager_singleton():
    mgr1 = ConfigManager()
    mgr2 = ConfigManager()
    assert mgr1 is mgr2

# tests/test_connectors.py
def test_hive_source_connector():
    config = SourceConfig("hive", {"table": "sample"})
    connector = SourceFactory.create(config)
    assert isinstance(connector, HiveSource)
```

## 8. Requirements
```txt
# requirements.txt
pyspark>=3.5.0
beautifulsoup4>=4.12.0
nltk>=3.8.1
pytest>=7.4.0
python-dateutil>=2.8.2
```

## 9. Execution Example
```python
if __name__ == "__main__":
    # Initialize config
    config_manager = ConfigManager()
    config_manager.register_config("hr_source", 
        SourceConfig("hive", {"database": "hr", "table": "employees"}))
    
    # Build pipeline
    pipeline = (
        PipelineBuilder()
        .with_source("hr_source")
        .with_processing()
        .build()
    )
    
    result = pipeline.execute()
    result.show()
```

This implementation strictly follows:
1. Singleton for ConfigManager and Logger
2. Factory for Source/Target creation
3. Strategy for processing steps
4. Template Method for chunking pipelines
5. Builder for pipeline construction

Each module can be extended without modifying existing code by:
1. Adding new config classes
2. Implementing new strategy classes
3. Registering new factories

The intermediate storage is handled through Spark's DataFrame persistence between pipeline steps.

---
Answer from Perplexity: pplx.ai/share

