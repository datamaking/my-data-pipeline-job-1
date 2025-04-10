#!/bin/bash

# Set base project directory name
BASE_DIR="nlp-etl-pipeline"

# Create base directory
mkdir -p "${BASE_DIR}"

# Create config directories and files
mkdir -p "${BASE_DIR}/config/source_configs"
mkdir -p "${BASE_DIR}/config/target_configs"

touch "${BASE_DIR}/config/__init__.py"
touch "${BASE_DIR}/config/base_config.py"
touch "${BASE_DIR}/config/config_manager.py"
touch "${BASE_DIR}/config/preprocessing_config.py"
touch "${BASE_DIR}/config/chunking_config.py"
touch "${BASE_DIR}/config/embedding_config.py"

touch "${BASE_DIR}/config/source_configs/__init__.py"
touch "${BASE_DIR}/config/source_configs/file_config.py"
touch "${BASE_DIR}/config/source_configs/rdbms_config.py"
touch "${BASE_DIR}/config/source_configs/hive_config.py"
touch "${BASE_DIR}/config/source_configs/nosql_config.py"
touch "${BASE_DIR}/config/source_configs/vector_db_config.py"

touch "${BASE_DIR}/config/target_configs/__init__.py"
touch "${BASE_DIR}/config/target_configs/file_config.py"
touch "${BASE_DIR}/config/target_configs/rdbms_config.py"
touch "${BASE_DIR}/config/target_configs/hive_config.py"
touch "${BASE_DIR}/config/target_configs/nosql_config.py"
touch "${BASE_DIR}/config/target_configs/vector_db_config.py"

# Create source directories and files
mkdir -p "${BASE_DIR}/source"
touch "${BASE_DIR}/source/__init__.py"
touch "${BASE_DIR}/source/source_factory.py"
touch "${BASE_DIR}/source/base_source.py"
touch "${BASE_DIR}/source/file_source.py"
touch "${BASE_DIR}/source/rdbms_source.py"
touch "${BASE_DIR}/source/hive_source.py"
touch "${BASE_DIR}/source/nosql_source.py"
touch "${BASE_DIR}/source/vector_db_source.py"

# Create preprocessing directories and files
mkdir -p "${BASE_DIR}/preprocessing"
touch "${BASE_DIR}/preprocessing/__init__.py"
touch "${BASE_DIR}/preprocessing/preprocessing_factory.py"
touch "${BASE_DIR}/preprocessing/base_preprocessor.py"
touch "${BASE_DIR}/preprocessing/html_parser.py"
touch "${BASE_DIR}/preprocessing/data_cleaner.py"

# Create chunking directories and files
mkdir -p "${BASE_DIR}/chunking"
touch "${BASE_DIR}/chunking/__init__.py"
touch "${BASE_DIR}/chunking/chunking_factory.py"
touch "${BASE_DIR}/chunking/base_chunker.py"
touch "${BASE_DIR}/chunking/fixed_size_chunker.py"
touch "${BASE_DIR}/chunking/semantic_chunker.py"
touch "${BASE_DIR}/chunking/chunk_smoother.py"

# Create embedding directories and files
mkdir -p "${BASE_DIR}/embedding"
touch "${BASE_DIR}/embedding/__init__.py"
touch "${BASE_DIR}/embedding/embedding_factory.py"
touch "${BASE_DIR}/embedding/base_embedder.py"
touch "${BASE_DIR}/embedding/tfidf_embedder.py"
touch "${BASE_DIR}/embedding/sentence_embedder.py"

# Create target directories and files
mkdir -p "${BASE_DIR}/target"
touch "${BASE_DIR}/target/__init__.py"
touch "${BASE_DIR}/target/target_factory.py"
touch "${BASE_DIR}/target/base_target.py"
touch "${BASE_DIR}/target/file_target.py"
touch "${BASE_DIR}/target/rdbms_target.py"
touch "${BASE_DIR}/target/hive_target.py"
touch "${BASE_DIR}/target/nosql_target.py"
touch "${BASE_DIR}/target/vector_db_target.py"

# Create utils directories and files
mkdir -p "${BASE_DIR}/utils"
touch "${BASE_DIR}/utils/__init__.py"
touch "${BASE_DIR}/utils/logger.py"
touch "${BASE_DIR}/utils/exception_handler.py"

# Create pipeline directories and files
mkdir -p "${BASE_DIR}/pipeline/domain_pipelines"
touch "${BASE_DIR}/pipeline/__init__.py"
touch "${BASE_DIR}/pipeline/pipeline_builder.py"
touch "${BASE_DIR}/pipeline/base_pipeline.py"
touch "${BASE_DIR}/pipeline/domain_pipelines/__init__.py"
touch "${BASE_DIR}/pipeline/domain_pipelines/admin_pipeline.py"
touch "${BASE_DIR}/pipeline/domain_pipelines/hr_pipeline.py"
touch "${BASE_DIR}/pipeline/domain_pipelines/finance_pipeline.py"
touch "${BASE_DIR}/pipeline/domain_pipelines/it_helpdesk_pipeline.py"

# Create tests directories and files
mkdir -p "${BASE_DIR}/tests"
touch "${BASE_DIR}/tests/__init__.py"
touch "${BASE_DIR}/tests/test_config.py"
touch "${BASE_DIR}/tests/test_source.py"
touch "${BASE_DIR}/tests/test_preprocessing.py"
touch "${BASE_DIR}/tests/test_chunking.py"
touch "${BASE_DIR}/tests/test_embedding.py"
touch "${BASE_DIR}/tests/test_target.py"
touch "${BASE_DIR}/tests/test_pipeline.py"
touch "${BASE_DIR}/tests/test_utils.py"

# Create root files
touch "${BASE_DIR}/main.py"
touch "${BASE_DIR}/requirements.txt"
touch "${BASE_DIR}/README.md"

echo "Project structure created under '${BASE_DIR}'."
