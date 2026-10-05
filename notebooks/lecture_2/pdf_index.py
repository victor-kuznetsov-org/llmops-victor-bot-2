# Databricks notebook source
# Vector index over pdf_chunks (not run yet)
import yaml
from databricks.vector_search.client import VectorSearchClient

cfg = yaml.safe_load(open("../../project_config.yml"))["dev"]
catalog, schema = cfg["catalog"], cfg["schema"]

# COMMAND ----------

vsc = VectorSearchClient()
index = vsc.create_delta_sync_index(
    endpoint_name=cfg["vector_search_endpoint"],
    index_name=f"{catalog}.{schema}.pdf_index",
    source_table_name=f"{catalog}.{schema}.pdf_chunks",
    pipeline_type="TRIGGERED",
    primary_key="chunk_id",
    embedding_source_column="text",
    embedding_model_endpoint_name=cfg["embedding_endpoint"],
)

# COMMAND ----------

# TODO: sync the index and query it
