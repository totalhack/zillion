from datetime import datetime, timedelta
import hashlib
import re
import struct
import time

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as rest
    from qdrant_client.http.models import Distance, VectorParams
    from langchain.chains import LLMChain
    from langchain.embeddings.openai import OpenAIEmbeddings
    from langchain.llms import OpenAI, OpenAIChat
    from langchain.prompts import PromptTemplate
    from langchain.vectorstores import Qdrant
    from pydantic import Extra
except ImportError:
    pass

from sqlalchemy import and_
from sqlalchemy.schema import CreateTable
from tlbx import raiseifnot, st, json, rgetkey

from zillion.core import (
    dbg,
    info,
    warn,
    error,
    zillion_config,
    nlp_installed,
    RollupTypes,
    FieldTypes,
)

LLM_MAX_TOKENS = -1
LLM_REQUEST_TIMEOUT = 20
OPENAI_DAVINCI_MODEL_NAME = "text-davinci-003"
OPENAI_VECTOR_SIZE = 1536
MIN_FIELD_SIMILARITY_SCORE = 0.8
DEFAULT_VECTOR_SIZE = OPENAI_VECTOR_SIZE
DEFAULT_WAREHOUSE_COLLECTION_NAME = "default_warehouse_fields"

embeddings_api = None


def hash_text(text):
    """Hash a string to a 32-character hex string""" ""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


if nlp_installed:
    from zillion.model import (
        EmbeddingsCache as EmbeddingsCacheTable,
        zillion_engine,
    )

    class EmbeddingsCache:
        """
        A cache for embeddings. This is a wrapper around a database table
        that stores embeddings for text. It also provides a cache for
        embeddings that have been retrieved from the database.

        **Parameters:**

        * **table** - (*SQLAlchemy Table*) The database table to use for the cache
        * **model** - (*str*) The model to use for the embeddings
        * **binary** - (*bool, optional) Whether embeddings are store in binary format
        * **binary_size** - (*int, optional) The size of the vector for converting to/from binary

        """

        def __init__(
            self,
            table,
            model,
            binary=True,
            binary_size=DEFAULT_VECTOR_SIZE,
        ):
            self.table = table
            self.model = model
            self.binary = binary
            self.binary_size = binary_size
            self.conn = zillion_engine.connect()
            self.init_cache()

        @classmethod
        def get_text_hash(cls, text):
            """Get the hash of a text string"""
            return hash_text(text)

        @property
        def cache(self):
            if not hasattr(self, "_cache"):
                self._cache = {}
            return self._cache

        @cache.setter
        def cache(self, value):
            self._cache = value

        def decode(self, blob):
            """Decode a binary blob into a list of floats"""
            return struct.unpack("f" * self.binary_size, blob)

        def encode(self, values):
            """Encode a list of floats into a binary blob"""
            return struct.pack("f" * self.binary_size, *values)

        def init_cache(self):
            """Initialize the in-memory cache"""
            stmt = self.table.select()
            result = self.conn.execute(stmt).fetchall()
            for row in result:
                text_hash = row["text_hash"]
                vector = row["vector"]
                if self.binary:
                    vector = self.decode(vector)
                self.cache[(text_hash, self.model)] = vector
            info(f"Initialized embeddings cache with {len(self.cache)} records")

        def _get_key(self, text):
            """Get the in-memory cache key. Matches the unique/primary key of
            the cache database table"""
            return (self.get_text_hash(text), self.model)

        def __getitem__(self, key):
            """Get an embedding from the cache. If it's not in the cache,
            it will be retrieved from the database.

            **Parameters:**

            * **key** - (*str*) The text to get the embedding for

            **Returns:**

            * (*list*) The embedding vector

            """
            cache_key = self._get_key(key)
            text_hash, _ = cache_key
            value = self.cache.get(cache_key)
            if value is None:
                stmt = self.table.select().where(
                    and_(
                        self.table.c.text_hash == text_hash,
                        self.table.c.model == self.model,
                    )
                )
                result = self.conn.execute(stmt).fetchone()
                if result:
                    value = result["vector"]
                    if self.binary:
                        value = self.decode(value)
                    self.cache[cache_key] = value
            return value

        def __setitem__(self, key, value):
            """Set an embedding in the cache. If it's not in the cache,
            it will be added to the database.

            **Parameters:**

            * **key** - (*str*) The text to set the embedding for
            * **value** - (*list*) The embedding vector

            """
            record = self.__getitem__(key)
            cache_key = self._get_key(key)
            text_hash, _ = cache_key
            if self.binary:
                vector = self.encode(value)

            if record:
                stmt = (
                    self.table.update()
                    .where(
                        and_(
                            self.table.c.text_hash == text_hash,
                            self.table.c.model == self.model,
                        )
                    )
                    .values(vector=vector, text=key)
                )
            else:
                stmt = self.table.insert().values(
                    text_hash=text_hash, model=self.model, text=key, vector=vector
                )

            self.conn.execute(stmt)
            self.cache[cache_key] = value

        def __delitem__(self, key):
            """Delete an embedding from the cache

            **Parameters:**

            * **key** - (*str*) The text to delete the embedding for

            """
            cache_key = self._get_key(key)
            text_hash, _ = cache_key
            stmt = self.table.delete().where(self.table.c.text_hash == text_hash)
            self.conn.execute(stmt)
            if cache_key in self.cache:
                del self.cache[cache_key]

    class OpenAIEmbeddingsCached(OpenAIEmbeddings):
        """OpenAI Embeddings with a cache for faster retrieval and
        to avoid extra API calls"""

        class Config:
            """Configuration for this pydantic object."""

            extra = Extra.allow

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._cache = EmbeddingsCache(
                EmbeddingsCacheTable,
                self.query_model_name,
                binary=True,
                binary_size=OPENAI_VECTOR_SIZE,
            )

        def embed_query(self, query):
            """Embed a query and cache the result"""
            res = self._cache[query]
            if res:
                return res
            embedding = super().embed_query(query)
            self._cache[query] = embedding
            return embedding

        def embed_documents(self, documents):
            """Embed documents and cache the results"""
            uncached = [d for d in documents if not self._cache[d]]
            info(f"Embedding {len(uncached)}/{len(documents)} uncached documents")
            if uncached:
                embeddings = super().embed_documents(uncached)
                for document, embedding in zip(uncached, embeddings):
                    self._cache[document] = embedding
            return [self._cache[d] for d in documents]

    class QdrantCustom(Qdrant):
        """Qdrant with custom ID generation and bulk embedding"""

        @classmethod
        def get_id(cls, text):
            """Get the hash of a text string to use as an ID"""
            return hash_text(text)

        def add_texts(self, texts, metadatas=None, bulk_embedder=None):
            """Add texts to Qdrant. If a bulk embedder is provided, it will be
            used to embed the texts in bulk. Otherwise, the texts will be
            embedded one at a time.

            **Parameters:**

            * **texts** - (*list*) List of texts to add to Qdrant
            * **metadatas** - (*list, optional*) List of metadata dictionaries to add to Qdrant
            * **bulk_embedder - (*callable, optional*) A function that takes a list of texts and
            returns a list of embeddings

            **Returns:**

            * **ids** - (*list*) List of IDs for the texts added to Qdrant

            """
            ids = [self.get_id(text) for text in texts]
            if bulk_embedder:
                vectors = bulk_embedder(texts)
            else:
                vectors = [self.embedding_function(text) for text in texts]
            payloads = self._build_payloads(
                texts,
                metadatas,
                self.content_payload_key,
                self.metadata_payload_key,
            )

            start = time.time()
            self.client.upsert(
                collection_name=self.collection_name,
                points=rest.Batch.construct(
                    ids=ids, vectors=[list(v) for v in vectors], payloads=payloads
                ),
            )
            info(f"Added {len(texts)} texts to Qdrant in {time.time() - start:.2f}s")
            return ids

        def similarity_search_with_score(self, query, k=4, **kwargs):
            """Search for similar texts to a query and return the results with scores

            **Parameters:**

            * **query** - (*str*) The query text to search for
            * **k** - (*int, optional*) The number of results to return
            * **kwargs** - (*dict, optional*) Additional keyword arguments to pass to Qdrant

            **Returns:**

            * **results** - (*list*) List of tuples of the form (document, score)

            """
            embedding = self.embedding_function(query)
            results = self.client.search(
                collection_name=self.collection_name,
                query_vector=list(embedding),
                with_payload=True,
                limit=k,
                **kwargs,
            )
            return [
                (
                    self._document_from_scored_point(
                        result, self.content_payload_key, self.metadata_payload_key
                    ),
                    result.score,
                )
                for result in results
            ]

    class EmbeddingsAPI:
        """API for embedding texts and querying Qdrant"""

        def __init__(self):
            key = zillion_config["OPENAI_API_KEY"]
            self.embeddings = OpenAIEmbeddingsCached(openai_api_key=key)
            # We delay connecting to Qdrant until we need it. This way
            # we can init the API globally without Qdrant necessarily running.
            self.client = None

        def connect(self):
            """Connect to Qdrant"""
            host = zillion_config["QDRANT_HOST"]
            info(f"Connecting to Qdrant host: {host}...")
            if host == ":memory:":
                self.client = QdrantClient(location=host)
            elif host.startswith("http") or host in [
                "localhost",
                "127.0.0.1",
                "host.docker.internal",
                "qdrant",  # Docker container name
            ]:
                self.client = QdrantClient(host=host, port=6333, prefer_grpc=True)
            else:
                self.client = QdrantClient(path=host)

        def ensure_client(self):
            """Make sure we have a client. If not, connect to Qdrant."""
            if not self.client:
                self.connect()

        def embed_documents(self, rows):
            """Embed a list of texts"""
            return self.embeddings.embed_documents(rows)

        def embed_query(self, query):
            """Embed a query"""
            return self.embeddings.embed_query(query)

        def recreate_collection(
            self,
            collection_name,
            vector_size=None,
            distance=Distance.COSINE,
            sample=None,
            **kwargs,
        ):
            """Create or recreate a collection in Qdrant

            **Parameters:**

            * **collection_name** - (*str*) Name of the collection
            * **vector_size** - (*int, optional*) Size of the vector. If not provided, will be inferred
            from the sample.
            * **distance** - (*Distance, optional*) Distance metric to use. Defaults to cosine.
            * **sample** - (*str, optional*) A sample text to use to infer the vector size.

            **Returns:**

            * **collection** - (*QdrantCollection*) The collection

            """
            info(f"Recreating collection {collection_name}...")
            if sample and vector_size is None:
                vector_size = len(self.embed_documents([sample])[0])
            elif vector_size is None:
                vector_size = DEFAULT_VECTOR_SIZE

            self.ensure_client()
            self.client.recreate_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=vector_size, distance=distance),
                **kwargs,
            )
            return self.get_collection(collection_name)

        def create_collection_if_necessary(
            self,
            collection_name,
            vector_size=None,
            distance=Distance.COSINE,
            sample=None,
            **kwargs,
        ):
            """Create a collection if it doesn't already exist. If it does exist,
            just return the collection.

            **Parameters:**

            * **collection_name** - (*str*) Name of the collection
            * **vector_size** - (*int, optional*) Size of the vector. If not provided, will be inferred
            from the sample.
            * **distance** - (*Distance, optional*) Distance metric to use. Defaults to cosine.
            * **sample** - (*str, optional*) A sample text to use to infer the vector size.

            **Returns:**

            * **collection** - (*QdrantCollection*) The collection

            """
            try:
                collection = self.get_collection(collection_name)
                if collection:
                    info(
                        f"Collection {collection_name} already exists. Skipping creation."
                    )
                    return collection
            except Exception as e:
                if "not found" not in str(e).lower():
                    raise e

            return self.recreate_collection(
                collection_name,
                vector_size=vector_size,
                distance=distance,
                sample=sample,
                **kwargs,
            )

        def add_texts(
            self, collection_name, texts, metadatas=None, force_recreate=False
        ):
            """Add texts to Qdrant. See QdrantCustom.add_texts for details."""
            self.ensure_client()
            if force_recreate:
                self.recreate_collection(
                    collection_name, sample=texts[0], metadatas=metadatas
                )
            else:
                self.create_collection_if_necessary(collection_name, sample=texts[0])

            qdrant = QdrantCustom(
                self.client, collection_name, self.embeddings.embed_query
            )
            qdrant.add_texts(
                texts,
                metadatas=metadatas,
                bulk_embedder=self.embeddings.embed_documents,
            )

        def similarity_search_with_score(self, collection_name, query, **kwargs):
            """Search for similar texts to a query and return the results with scores.
            See QdrantCustom.similarity_search_with_score for details."""
            self.ensure_client()
            qdrant = QdrantCustom(
                self.client, collection_name, self.embeddings.embed_query
            )
            return qdrant.similarity_search_with_score(query, **kwargs)

        def get_collection(self, name):
            """Get a collection by name"""
            self.ensure_client()
            return self.client.get_collection(collection_name=name)

        def delete_collection(self, name):
            """Delete a collection by name"""
            self.ensure_client()
            return self.client.delete_collection(collection_name=name)

        def get_embeddings(
            self, collection_name, with_payload=True, with_vectors=False
        ):
            """Get all embeddings in a collection

            **Parameters:**

            * **collection_name** - (*str*) Name of the collection
            * **with_payload** - (*bool, optional*) Whether to include the payload. Defaults to True.
            * **with_vectors** - (*bool, optional*) Whether to include the vectors. Defaults to False.

            **Returns:**

            * **result** - (*list*) List of embeddings

            """
            self.ensure_client()
            collection = self.get_collection(collection_name)
            raiseifnot(collection, f"No collection found: {collection_name}")
            result = []
            offset = None
            while True:
                points, next_page_offset = self.client.scroll(
                    collection_name=collection_name,
                    with_payload=with_payload,
                    with_vectors=with_vectors,
                    limit=500,
                    offset=offset,
                )
                result.extend([dict(x) for x in points])
                if next_page_offset is None:
                    break
                offset = next_page_offset
            return result

        def delete_embeddings(self, collection_name, texts):
            """Delete embeddings by text

            **Parameters:**

            * **collection_name** - (*str*) Name of the collection
            * **texts** - (*list*) List of texts to delete

            """
            ids = [QdrantCustom.get_id(text) for text in texts]
            self.ensure_client()
            self.client.delete(
                collection_name=collection_name,
                points_selector=rest.PointIdsList(points=ids),
            )

        def upsert_embedding(self, collection_name, text, payload):
            """Upsert an embedding

            **Parameters:**

            * **collection_name** - (*str*) Name of the collection
            * **text** - (*str*) Text to upsert
            * **payload** - (*dict*) Payload to upsert

            **Returns:**

            * **result** - (*dict*) Result of the upsert

            """
            self.ensure_client()
            qdrant = QdrantCustom(
                self.client, collection_name, self.embeddings.embed_query
            )
            return qdrant.add_texts([text], metadatas=[payload])


if nlp_installed:
    embeddings_api = EmbeddingsAPI()


def field_name_to_embedding_text(name):
    """Convert a field name to a format for embedding"""
    return name.replace("_", " ").lower()


def get_warehouse_collection_name(warehouse):
    """Get the collection name for a warehouse's embeddings. If it is set on
    the config, use that. Otherwise try to use a warehouse name or fall back
    to a default name."""
    meta = warehouse.meta or {}
    if meta.get("nlp", {}).get("collection_name", None):
        return meta["nlp"]["collection_name"]
    if not warehouse.name:
        warn(
            f"Warehouse has no name. Using default embeddings collection name: {DEFAULT_WAREHOUSE_COLLECTION_NAME}"
        )
        return DEFAULT_WAREHOUSE_COLLECTION_NAME
    return warehouse.name


def warehouse_field_nlp_enabled(warehouse, field_def):
    """Check if NLP is enabled for a field. If it is disabled at any level,
    it will be considered disabled."""

    wh_meta = warehouse.meta or {}
    field_meta = field_def.meta or {}

    # 1) Check disabled groups at warehouse level
    if rgetkey(wh_meta, "nlp.field_disabled_groups", None) and "group" in field_meta:
        if field_meta["group"] in wh_meta["nlp"]["field_disabled_groups"]:
            return False

    # 2) Check disabled fields by regex at warehouse level
    if rgetkey(wh_meta, "nlp.field_disabled_patterns", None):
        for pattern in wh_meta["nlp"]["field_disabled_patterns"]:
            if re.match(pattern, field_def.name):
                return False

    # 3) Check field level settings
    return rgetkey(field_meta, "nlp.enabled", True) is not False


def init_warehouse_embeddings(warehouse, force_recreate=False):
    """
    Initialize embeddings for the warehouse.

    **Parameters:**

    * **warehouse** - (Warehouse) The warehouse to initialize embeddings for.
    * **force_recreate** - (*bool, optional*) If True, force the embeddings
    collection to be recreated from scratch.

    **Returns:**

    (*Embeddings*) - The initialized EmbeddingsAPI object.

    """
    collection_name = get_warehouse_collection_name(warehouse)
    fields = warehouse.get_fields()

    texts = []
    metadatas = []
    count = 0
    for name, fdef in fields.items():
        if not warehouse_field_nlp_enabled(warehouse, fdef):
            continue

        count += 1
        settings = (fdef.meta or {}).get("nlp", {}) or {}

        if settings.get("embedding_text", None):
            # Allow overriding the default embedding text
            emb_texts = settings["embedding_text"]
            if isinstance(emb_texts, str):
                emb_texts = [emb_texts]
        else:
            emb_texts = [field_name_to_embedding_text(name)]

        for emb_text in emb_texts:
            texts.append(emb_text)
            metadatas.append({"name": name, "field_type": fdef.field_type})

    start = time.time()
    info(
        f"Initializing {count}/{len(fields)} fields in embedding collection {collection_name}..."
    )
    embeddings_api.add_texts(
        collection_name=collection_name,
        texts=texts,
        metadatas=metadatas,
        force_recreate=force_recreate,
    )
    info(f"Done in {time.time() - start:3f} seconds.")
    return collection_name


def get_openai_class(model=None):
    """Get the OpenAI class to use for a given model."""
    model = model or zillion_config["OPENAI_MODEL"]
    return OpenAI if model == OPENAI_DAVINCI_MODEL_NAME else OpenAIChat


def get_openai_model_context_size(model):
    """Logic copied from langchain since no util exposed"""
    if model == "text-davinci-003":
        return 4097
    elif model == "text-curie-001":
        return 2048
    elif model == "text-babbage-001":
        return 2048
    elif model == "text-ada-001":
        return 2048
    elif model == "code-davinci-002":
        return 8000
    elif model == "code-cushman-001":
        return 2048
    else:
        return 4097


def build_llm(model=None, max_tokens=None, request_timeout=LLM_REQUEST_TIMEOUT):
    """Build an LLM using langchain and the OpenAI API.

    **Parameters:**

    * **model** - (str, optional) The OpenAI model to use. Defaults to the model specified in the zillion config.
    * **max_tokens** - (int) The maximum number of tokens to generate.
    * **request_timeout** - (int) The maximum number of seconds to wait for a response from the OpenAI API.

    **Returns:**

    (*llm*) - A langchain OpenAI LLM

    """
    model = model or zillion_config["OPENAI_MODEL"]
    key = zillion_config["OPENAI_API_KEY"]
    raiseifnot(model and key, "Missing OpenAI API key or model name in zillion config")
    max_tokens = max_tokens or LLM_MAX_TOKENS
    dbg(f"Building OpenAI {model} chain with max_tokens={max_tokens}")
    openai_class = get_openai_class(model)
    return openai_class(
        model_name=model,
        temperature=0,
        max_tokens=max_tokens,
        request_timeout=request_timeout,
        max_retries=1,
        openai_api_key=key,
    )


def build_chain(
    prompt,
    model=None,
    max_tokens=LLM_MAX_TOKENS,
    request_timeout=LLM_REQUEST_TIMEOUT,
    llm=None,
):
    """Build a chain using langchain and the OpenAI API.

    **Parameters:**

    * **prompt** - (PromptTemplate) The prompt to use.
    * **model** - (str, optional) The OpenAI model to use. Defaults to the model specified in the zillion config.
    * **max_tokens** - (int) The maximum number of tokens to generate.
    * **request_timeout** - (int) The maximum number of seconds to wait for a response from the OpenAI API.
    * **llm** - (llm, optional) The LLM to use. Defaults to a new LLM built using the OpenAI API.

    **Returns:**

    (*LLMChain*) - A langchain LLMChain object.

    """
    llm = llm or build_llm(
        model=model, max_tokens=max_tokens, request_timeout=request_timeout
    )
    return LLMChain(llm=llm, prompt=prompt)


TEXT_TO_REPORT_NO_FIELDS = """You are an expert SQL analyst that takes natural language input and outputs metrics, dimensions, criteria, ordering, and limit settings in JSON format.
If a relative date is specified, such as "yesterday", replace it with the actual date. The current date is: {current_date}

Generic Example 1:

Input: revenue and sales by date for the last 30 days. Rows with more than 5 sales.
Output:
{{
  "metrics": ["revenue", "sales"],
  "dimensions": ["date"],
  "criteria": [
    ["date", ">=", "{thirty_days_ago}"]
  ],
  "row_filters": [
    ["sales", ">", 5]
]
}}

Generic Example 2:

Input: show me the top 10 campaigns by revenue yesterday for ad engine Google. Include totals.
Output:
{{
  "metrics": ["revenue"],
  "dimensions": ["campaign"],
  "criteria": [
    ["date", "=", "{yesterday}"],
    ["ad_engine", "=", "Google"]
  ],
  "limit": 10,
  "order_by": [
    ["revenue", "desc"]
  ],
  "rollup": "totals"
}}

----
Complete the following. Use JSON format and include no other commentary.
Input: {query}
JSON Output:"""


TEXT_TO_REPORT_ALL_FIELDS = """You are an expert SQL analyst that takes natural language input and outputs metrics, dimensions, criteria, ordering, and limit settings in JSON format.
If a relative date is specified, such as "yesterday", replace it with the actual date. The current date is: {current_date}

Supported metrics:
{metrics}

Supported dimensions:
{dimensions}

Generic Example 1:

Input: revenue and sales by date for the last 30 days. Rows with more than 5 sales.
JSON Output:
{{
  "metrics": ["revenue", "sales"],
  "dimensions": ["date"],
  "criteria": [
    ["date", ">=", "{thirty_days_ago}"]
  ],
  "row_filters": [
    ["sales", ">", 5]
]
}}

Generic Example 2:

Input: show me the top 10 campaigns by revenue yesterday for ad engine Google. Include totals.
JSON Output:
{{
  "metrics": ["revenue"],
  "dimensions": ["campaign"],
  "criteria": [
    ["date", "=", "{yesterday}"],
    ["ad_engine", "=", "Google"]
  ],
  "limit": 10,
  "order_by": [
    ["revenue", "desc"]
  ],
  "rollup": "totals"
}}

----
Complete the following. Use JSON format and include no other commentary. Use only supported metrics and dimensions.
Input: {query}
JSON Output:"""

TEXT_TO_REPORT_DIMENSION_FIELDS = """You are an expert SQL analyst that takes natural language input and outputs metrics, dimensions, criteria, ordering, and limit settings in JSON format.
If a relative date is specified, such as "yesterday", replace it with the actual date. The current date is: {current_date}

Supported dimensions:
{dimensions}

Generic Example 1:

Input: revenue and sales by date for the last 30 days. Rows with more than 5 sales.
JSON Output:
{{
  "metrics": ["revenue", "sales"],
  "dimensions": ["date"],
  "criteria": [
    ["date", ">=", "{thirty_days_ago}"]
  ],
  "row_filters": [
    ["sales", ">", 5]
]
}}

Generic Example 2:

Input: show me the top 10 campaigns by revenue yesterday for ad engine Google. Include totals.
JSON Output:
{{
  "metrics": ["revenue"],
  "dimensions": ["campaign"],
  "criteria": [
    ["date", "=", "{yesterday}"],
    ["ad_engine", "=", "Google"]
  ],
  "limit": 10,
  "order_by": [
    ["revenue", "desc"]
  ],
  "rollup": "totals"
}}

----
Complete the following. Use JSON format and include no other commentary.
Input: {query}
JSON Output:"""


def parse_text_to_report_json_output(output):
    """Parse the output of a chain that produces Zillion args as JSON

    **Parameters:**

    * **output** - (str) The output of the chain, expected to be valid JSON.

    **Returns:**

    (*dict*) - A dict of Zillion report params. Any empty values will be removed.

    """
    if not output:
        return None
    try:
        return {k: v for k, v in json.loads(output).items() if v}
    except Exception as e:
        error(f"Error parsing JSON:\n{output}")
        raise


# TODO - share code for configs below

PROMPT_CONFIGS = dict(
    no_fields=dict(
        prompt_text=TEXT_TO_REPORT_NO_FIELDS,
        input_variables=["query", "current_date", "yesterday", "thirty_days_ago"],
        context_func=lambda wh: dict(
            current_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            yesterday=str(datetime.now().date() - timedelta(days=1)),
            thirty_days_ago=str(datetime.now().date() - timedelta(days=30)),
        ),
        parser=parse_text_to_report_json_output,
    ),
    all_fields=dict(
        prompt_text=TEXT_TO_REPORT_ALL_FIELDS,
        input_variables=[
            "query",
            "current_date",
            "yesterday",
            "thirty_days_ago",
            "metrics",
            "dimensions",
        ],
        context_func=lambda wh: dict(
            metrics=get_metrics_prompt_str(wh),
            dimensions=get_dimensions_prompt_str(wh),
            current_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            yesterday=str(datetime.now().date() - timedelta(days=1)),
            thirty_days_ago=str(datetime.now().date() - timedelta(days=30)),
        ),
        parser=parse_text_to_report_json_output,
    ),
    dimension_fields=dict(
        prompt_text=TEXT_TO_REPORT_DIMENSION_FIELDS,
        input_variables=[
            "query",
            "current_date",
            "yesterday",
            "thirty_days_ago",
            "dimensions",
        ],
        context_func=lambda wh: dict(
            dimensions=get_dimensions_prompt_str(wh),
            current_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            yesterday=str(datetime.now().date() - timedelta(days=1)),
            thirty_days_ago=str(datetime.now().date() - timedelta(days=30)),
        ),
        parser=parse_text_to_report_json_output,
    ),
)


def get_field_name_variants(name):
    """Get a list of possible close variants of a field name. This is
    an 80/20 hack to help with fuzzy matching of field names.

    **Parameters:**

    * **name** - (str) The name of the field

    **Returns:**

    (*set*) - A set of possible variants of the field name
    """
    res = {name}
    for alt in [
        name.lower(),
        name.title(),
        name.replace(" ", "_"),
        name.replace("_", " "),
    ]:
        if alt not in res:
            res.add(alt)
    return res


# NOTE:
# If we ever support fuzzy matching to known dimension values,
# could use a library like: https://github.com/seatgeek/thefuzz


def get_field_fuzzy(warehouse, name, field_type=None):
    """Try to do a fuzzy match to warehouse fields. The field names
    from the LLM are not always exact matches to the warehouse field.

    **Parameters:**

    * **warehouse** - (Warehouse) The warehouse to look for a field match
    * **name** - (str) The name of the field to match
    * **field_type** - (str, optional) The type of field to match. If not
    passed, will match any field type.

    **Returns:**

    (*str*) - The name of the field that was matched, or the original name
    """

    has_field_func = warehouse.has_field
    if field_type == FieldTypes.METRIC:
        has_field_func = warehouse.has_metric
    if field_type == FieldTypes.DIMENSION:
        has_field_func = warehouse.has_dimension

    # Check ~exact matches
    alts = get_field_name_variants(name)
    for alt in alts:
        if has_field_func(alt) and warehouse_field_nlp_enabled(
            warehouse, warehouse.get_field(alt)
        ):
            return alt

    # Check embeddings
    collection_name = warehouse._get_embeddings_collection_name()
    raiseifnot(collection_name, "No embeddings collection name found")
    res = embeddings_api.similarity_search_with_score(collection_name, name)
    if field_type:
        res = [r for r in (res or []) if r[0].metadata["field_type"] == field_type]

    if not res:
        return name

    best, score = res[0]
    if score >= MIN_FIELD_SIMILARITY_SCORE:
        info(f"Found fuzzy match for {name}: {best.metadata['name']} / {score}")
        return best.metadata["name"]
    else:
        warn(f"No good match found for '{name}': {res}")

    # No good match found but guess the original name
    return name


def map_warehouse_report_params(warehouse, report):
    """Map an LLM report params dict to the warehouse's field names.

    **Parameters:**

    * **warehouse** - (Warehouse) The warehouse to map the report params to
    * **report** - (dict) The report params dict

    **Returns:**

    (*dict*) - A new report params dict with the warehouse's field names

    """
    res = {}
    for k, v in report.items():
        if k == "metrics":
            res[k] = [
                get_field_fuzzy(warehouse, m, field_type=FieldTypes.METRIC) for m in v
            ]
        elif k == "dimensions":
            res[k] = [
                get_field_fuzzy(warehouse, d, field_type=FieldTypes.DIMENSION)
                for d in v
            ]
        elif k == "criteria":
            res[k] = [
                (
                    get_field_fuzzy(warehouse, c, field_type=FieldTypes.DIMENSION),
                    op,
                    val,
                )
                for c, op, val in v
            ]
        elif k == "row_filters":
            res[k] = [(get_field_fuzzy(warehouse, f), op, val) for f, op, val in v]
        elif k == "rollup":
            # TODO should we semantic match to valid values?
            # i.e. "summary" -> "totals
            if v and v.lower() in ["totals", "summary"]:
                res[k] = RollupTypes.TOTALS
            elif v and v.lower() == "all":
                res[k] = RollupTypes.ALL
        elif k == "order_by":
            res[k] = [(get_field_fuzzy(warehouse, f), d) for f, d in v]
        elif k == "limit":
            res[k] = int(v) if v is not None else None
        else:
            raise ValueError(f"Unexpected key {k}")
    return res


def get_fields_prompt_str(warehouse, fields):
    """Get a string representing names/types of given fields"""
    res = []
    for name, fdef in fields.items():
        if not warehouse_field_nlp_enabled(warehouse, fdef):
            continue
        if getattr(fdef, "formula", None):
            type_str = "numeric"
        else:
            type_str = str(fdef.type).lower() if fdef.type else "unknown"
        res.append(f"{name} ({type_str})")
    return "\n".join(res)


def get_metrics_prompt_str(warehouse):
    return get_fields_prompt_str(warehouse, warehouse.get_metrics())


def get_dimensions_prompt_str(warehouse):
    return get_fields_prompt_str(warehouse, warehouse.get_dimensions())


class MaxTokensException(Exception):
    pass


def text_to_report_params(query, warehouse=None, prompt_version="no_fields"):
    """Convert a natural language input to Zillion report params

    **Parameters:**

    * **query** - (str) The natural language query to convert.
    * **warehouse** - (Warehouse, optional) The warehouse to map the report params to
    * **prompt_version** - (str) The version of the prompt to use

    **Returns:**

    (*dict*) - A dict of Zillion report params. The field names extracted are
    not guaranteed to exist in any warehouse or datasource and need to be
    analyzed separately.

    """
    prompt_config = PROMPT_CONFIGS[prompt_version]
    context = prompt_config["context_func"](warehouse)
    context["query"] = query
    prompt = PromptTemplate(
        input_variables=prompt_config["input_variables"],
        template=prompt_config["prompt_text"],
    )

    llm = build_llm(max_tokens=-1)
    text_to_report_chain = build_chain(prompt, llm=llm)

    prompt_tokens = text_to_report_chain.llm.get_num_tokens(prompt.format(**context))
    max_tokens = get_openai_model_context_size(text_to_report_chain.llm.model_name)
    if (prompt_tokens + 500) >= max_tokens:
        raise MaxTokensException(f"Prompt is too long: {prompt_tokens} tokens")

    llm_start = time.time()
    output = text_to_report_chain.run(**context).strip(". -\n\r")
    info(f"LLM took {time.time() - llm_start:.3f}s")
    return prompt_config["parser"](output)


NLP_TABLE_RELATIONSHIP_PROMPT = """Given the following tables, what are the suggested foreign key relationships starting from these tables?
Rules:
- If there isn't a good option you must skip that table and output nothing.
- Only include relationships between the tables given below. Do not reference any other tables not in the list.
- Ignore self-referencing relationships (i.e. a table with a column that references itself)

{table_defs}

List the output relationships in child column -> parent column format with no other explanation or output.
Include schema and table names in the column names if possible. For example, if the table was called "main.users" and the column was called "id", the column format must be "main.users.id".
Output:"""


def parse_nlp_table_relationships(output):
    """
    Parse the output of the NLP table relationships prompt.

    **Parameters:**

    * **output** - (*str*) The output of the prompt

    **Returns:**

    (*dict*) - Map child columns to parent columns

    """
    if not output:
        return {}
    child_parent = {}
    for row in output.strip().split("\n"):
        if not row:
            continue
        if "->" not in row:
            warn(f"Invalid row in NLP table relationships output: {row}")
            continue
        child_column, parent_column = [x.strip() for x in row.split("->")]
        child_parent[child_column] = parent_column
    return child_parent


def get_nlp_table_relationships(metadata, table_names):
    """
    Get the NLP table relationships for the given tables. Note: if a table has
    a composite primary key it will be skipped.

    **Parameters:**

    * **metadata** - (*SQLAlchemy Metadata*) The metadata for the database
    * **table_names** - (*list of str*) The names of the tables to get the relationships for

    **Returns:**

    (*dict*) - Map child columns to parent columns

    """
    if not table_names:
        return {}

    table_defs = []

    def get_column_str(c):
        return f"{c.name} ({c.type}) primary_key:{c.primary_key}"

    for table_name in table_names:
        table = metadata.tables[table_name]
        if len(table.primary_key) > 1:
            warn(f"Skipping table {table_name} with composite primary key")
            continue
        table_defs.append(
            f"Table: {table_name}\n"
            "Fields:\n"
            f"{chr(10).join(get_column_str(c) for c in table.columns)}"
        )

    table_defs_str = "\n\n".join(table_defs)

    prompt = PromptTemplate(
        input_variables=["table_defs"],
        template=NLP_TABLE_RELATIONSHIP_PROMPT,
    )
    relationship_chain = build_chain(prompt)

    llm_start = time.time()
    output = relationship_chain.run(table_defs_str).strip(". -\n\r")
    info(output)
    info(f"LLM took {time.time() - llm_start:.3f}s")
    return parse_nlp_table_relationships(output)


NLP_TABLE_COLUMN_PROMPT = """For each column in the following table definition, list the following comma separated:

* The column name
* Whether the column is a metric or dimension
* The aggregation type for the metric (sum or mean), or "NULL" if it is a dimension
* The rounding for "mean" metrics, "NULL" if it is a dimension or "sum" metric

Example output rows:
id,dimension,NULL,NULL
revenue,metric,sum,2
cpc,metric,mean,2

Table definition:
{create_table}
"""


def parse_nlp_table_info(output):
    """Parse the output from the LLM to get the table info

    **Parameters:**

    * **output** - (*str*) The output from the LLM

    **Returns:**

    (*dict*) - A mapping of column names to properties

    """
    res = {}
    for row in output.strip().split("\n"):
        name, type, aggregation, rounding = [x.strip() for x in row.split(",")]
        res[name] = dict(
            type=type,
            aggregation=aggregation if aggregation != "NULL" else None,
            rounding=int(rounding) if rounding != "NULL" else None,
        )
    return res


table_info_chain = None


def get_nlp_table_info(table):
    """Build a langchain chain to get the table info from the LLM

    **Parameters:**

    * **table** - (*SQLAlchemy table*) The table to analyze

    **Returns:**

    (*dict*) - A mapping of column names to properties

    """
    raiseifnot(table.bind, "Table must be bound to an engine")
    create_table = str(CreateTable(table).compile(table.bind)).strip()

    global table_info_chain
    if not table_info_chain:
        prompt = PromptTemplate(
            input_variables=["create_table"], template=NLP_TABLE_COLUMN_PROMPT
        )
        table_info_chain = build_chain(prompt)

    llm_start = time.time()
    output = table_info_chain.run(create_table).strip(". -\n\r")
    info(output)
    info(f"LLM took {time.time() - llm_start:.3f}s")
    return parse_nlp_table_info(output)
