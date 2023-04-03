from datetime import datetime, timedelta
import hashlib
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
from tlbx import raiseifnot, st, json

from zillion.core import (
    info,
    warn,
    zillion_config,
    nlp_installed,
    RollupTypes,
    FieldTypes,
)

LLM_MAX_TOKENS = -1
LLM_REQUEST_TIMEOUT = 20
OPENAI_DAVINCI_MODEL_NAME = "text-davinci-003"
OPENAI_VECTOR_SIZE = 1536
MIN_FIELD_SIMILARITY_SCORE = 0.9
DEFAULT_VECTOR_SIZE = OPENAI_VECTOR_SIZE
DEFAULT_WAREHOUSE_COLLECTION_NAME = "default_warehouse_fields"


def hash_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


if nlp_installed:
    from zillion.model import (
        EmbeddingsCache as EmbeddingsCacheTable,
        zillion_engine,
    )

    class EmbeddingsCache:
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
            return struct.unpack("f" * self.binary_size, blob)

        def encode(self, values):
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
            """Get the in-memory cache key. Matches the unique/primary key of the cache database table"""
            return (self.get_text_hash(text), self.model)

        def __getitem__(self, key):
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
            cache_key = self._get_key(key)
            text_hash, _ = cache_key
            stmt = self.table.delete().where(self.table.c.text_hash == text_hash)
            self.conn.execute(stmt)
            if cache_key in self.cache:
                del self.cache[cache_key]

    class OpenAIEmbeddingsCached(OpenAIEmbeddings):
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
            res = self._cache[query]
            if res:
                return res
            embedding = super().embed_query(query)
            self._cache[query] = embedding
            return embedding

        def embed_documents(self, documents):
            uncached = [d for d in documents if not self._cache[d]]
            info(f"Embedding {len(uncached)}/{len(documents)} uncached documents")
            if uncached:
                embeddings = super().embed_documents(uncached)
                for document, embedding in zip(uncached, embeddings):
                    self._cache[document] = embedding
            return [self._cache[d] for d in documents]

    class QdrantCustom(Qdrant):
        @classmethod
        def get_id(cls, text):
            return hash_text(text)

        def add_texts(self, texts, metadatas=None, bulk_embedder=None):
            """Set repeatable IDs so we don't reinsert data"""
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

            self.client.upsert(
                collection_name=self.collection_name,
                points=rest.Batch(ids=ids, vectors=vectors, payloads=payloads),
            )
            return ids

        def similarity_search_with_score(self, query, k=4, **kwargs):
            """Allow passing kwargs through to Qdrant"""
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
    def __init__(self):
        key = zillion_config["OPENAI_API_KEY"]
        self.embeddings = OpenAIEmbeddingsCached(openai_api_key=key)
        # We delay connecting to Qdrant until we need it. This way
        # we can init the API globally without Qdrant necessarily running.
        self.client = None

    def connect(self):
        host = zillion_config["QDRANT_HOST"]
        info(f"Connecting to Qdrant at {host}...")
        self.client = QdrantClient(host=host, port=6333, prefer_grpc=True)

    def ensure_client(self):
        if not self.client:
            self.connect()

    def embed_documents(self, rows):
        return self.embeddings.embed_documents(rows)

    def embed_query(self, query):
        return self.embeddings.embed_query(query)

    def create_collection_if_necessary(
        self,
        collection_name,
        vector_size=None,
        distance=Distance.COSINE,
        sample=None,
        **kwargs,
    ):
        try:
            collection = self.get_collection(collection_name)
            if collection:
                info(f"Collection {collection_name} already exists. Skipping creation.")
                return collection
        except Exception as e:
            if "Not found" not in str(e):
                raise e

        info(f"Creating collection {collection_name}...")
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

    def add_texts(self, collection_name, texts, metadatas=None):
        self.ensure_client()
        self.create_collection_if_necessary(collection_name, sample=texts[0])
        qdrant = QdrantCustom(self.client, collection_name, self.embeddings.embed_query)
        qdrant.add_texts(
            texts, metadatas=metadatas, bulk_embedder=self.embeddings.embed_documents
        )

    def similarity_search_with_score(self, collection_name, query, **kwargs):
        self.ensure_client()
        qdrant = QdrantCustom(self.client, collection_name, self.embeddings.embed_query)
        return qdrant.similarity_search_with_score(query, **kwargs)

    def get_collection(self, name):
        self.ensure_client()
        return self.client.get_collection(collection_name=name)

    def delete_collection(self, name):
        self.ensure_client()
        return self.client.delete_collection(collection_name=name)

    def get_embeddings(self, collection_name, with_payload=True, with_vectors=False):
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
        ids = [QdrantCustom.get_id(text) for text in texts]
        self.ensure_client()
        self.client.delete(
            collection_name=collection_name,
            points_selector=rest.PointIdsList(points=ids),
        )

    def upsert_embedding(self, collection_name, text, payload):
        self.ensure_client()
        qdrant = QdrantCustom(self.client, collection_name, self.embeddings.embed_query)
        return qdrant.add_texts([text], metadatas=[payload])


embeddings_api = None
if nlp_installed:
    embeddings_api = EmbeddingsAPI()


def field_name_to_embedding_text(name):
    return name.replace("_", " ").lower()


def get_warehouse_collection_name(warehouse):
    meta = warehouse.meta or {}
    if meta.get("embeddings", {}).get("collection_name", None):
        return meta["embeddings"]["collection_name"]
    if not warehouse.name:
        warn(
            f"Warehouse has no name. Using default embeddings collection name: {DEFAULT_WAREHOUSE_COLLECTION_NAME}"
        )
        return DEFAULT_WAREHOUSE_COLLECTION_NAME
    return warehouse.name


def init_warehouse_embeddings(warehouse):
    """
    Initialize embeddings for the warehouse.

    **Parameters:**

    * **warehouse** - (Warehouse) The warehouse to initialize embeddings for.

    **Returns:**

    (*Embeddings*) - The initialized EmbeddingsAPI object.

    """
    collection_name = get_warehouse_collection_name(warehouse)
    fields = warehouse.get_fields()

    texts = []
    metadatas = []
    for name, fdef in fields.items():
        emb_text = field_name_to_embedding_text(name)
        if fdef.meta and fdef.meta.get("embedding_text", None):
            # Allow overriding the default embedding text
            emb_text = fdef.meta["embedding_text"]
        texts.append(emb_text)
        metadatas.append({"name": name, "field_type": fdef.field_type})

    start = time.time()
    info(
        f"Initializing {len(fields)} Warehouse embeddings in collection {collection_name}..."
    )
    embeddings_api.add_texts(
        collection_name=collection_name, texts=texts, metadatas=metadatas
    )
    info(f"Done in {time.time() - start:3f} seconds.")
    return collection_name


def build_chain(
    prompt, model=None, max_tokens=LLM_MAX_TOKENS, request_timeout=LLM_REQUEST_TIMEOUT
):
    """Build a chain using langchain and the OpenAI API.

    **Parameters:**

    * **prompt** - (PromptTemplate) The prompt to use.
    * **model** - (str, optional) The OpenAI model to use. Defaults to the model specified in the zillion config.
    * **max_tokens** - (int) The maximum number of tokens to generate.
    * **request_timeout** - (int) The maximum number of seconds to wait for a response from the OpenAI API.

    **Returns:**

    (*LLMChain*) - A langchain LLMChain object.

    """
    model = model or zillion_config["OPENAI_MODEL"]
    key = zillion_config["OPENAI_API_KEY"]
    raiseifnot(model and key, "Missing OpenAI API key or model name in zillion config")
    max_tokens = max_tokens or LLM_MAX_TOKENS
    info(f"Building OpenAI {model} chain with max_tokens={max_tokens}")
    openai_class = OpenAI if model == OPENAI_DAVINCI_MODEL_NAME else OpenAIChat
    llm = openai_class(
        model_name=model,
        temperature=0,
        max_tokens=max_tokens,
        request_timeout=request_timeout,
        max_retries=1,
        openai_api_key=key,
    )
    return LLMChain(llm=llm, prompt=prompt)


TEXT_TO_REPORT_V1 = """You are an expert SQL analyst that takes natural language input and outputs metrics, dimensions, criteria, ordering, and limit settings in JSON format.
If a relative date is specified, such as "yesterday", replace it with the actual date. The current date is: {current_date}

Example inputs and outputs:

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
Input: {query}
JSON Output:"""

TEXT_TO_REPORT_V2 = """We are going to translate natural language queries into a reporting API call. The python function we will call has a signature and docstring as follows:

```python
def run_report(
    metrics=None,
    dimensions=None,
    criteria=None,
    row_filters=None,
    rollup=None,
    pivot=None,
    order_by=None,
    limit=None,
):
    * **metrics** - (*list, optional*) A list of metric names, or dicts in the
    case of AdHocMetrics. These will be the measures of your report, or the
    statistics you are interested in computing at the given dimension grain.
    * **dimensions** - (*list, optional*) A list of dimension names to control
    the grain of the report. You can think of dimensions similarly to the "group
    by" in a SQL query.
    * **criteria** - (*list, optional*) A list of criteria to be applied when
    querying. Each criteria in the list is represented by a 3-item list or
    tuple. See `core.CRITERIA_OPERATIONS` for all supported
    operations. Note that some operations, such as "like", have varying
    behavior by datasource dialect. Some examples:
        * ["field_a", ">", 1]
        * ["field_b", "=", "2020-04-01"]
        * ["field_c", "like", "%example%"]
        * ["field_d", "in", ["a", "b", "c"]]

    * **row_filters** - (*list, optional*) A list of criteria to apply at the
    final step (combined query layer) to filter which rows get returned. The
    format here is the same as for the criteria arg, though the operations are
    limited to the values of `core.ROW_FILTER_OPERATIONS`.
    * **rollup** - (*str or int, optional*) Controls how metrics are rolled up
    / aggregated by dimension depth. If not passed no rollup will be
    computed. If the special value "totals" is passed, only a final tally
    rollup row will be added. If an int, then it controls the maximum depth to
    roll up the data, starting from the most granular (last) dimension of the
    report. Note that the rollup=3 case is like adding a totals row to the
    "=2" case, as a totals row is a rollup of all dimension levels. Setting
    rollup=len(dims) is equivalent to rollup="all". For example, if you ran a
    report with dimensions ["a", "b", "c"]:
        * **rollup="totals"** - adds a single, final rollup row
        * **rollup="all"** - rolls up all dimension levels
        * **rollup=1** - rolls up the first dimension only
        * **rollup=2** - rolls up the first two dimensions
        * **rollup=3** - rolls up all three dimensions
        * Any other non-None value would raise an error

    * **pivot** - (*list, optional*) A list of dimensions to pivot to columns
    * **order_by** - (*list, optional*) A list of (field, asc/desc) tuples that
    control the ordering of the returned result
    * **limit** - (*int, optional*) A limit on the number of rows returned
```

If a relative date is specified, such as "yesterday", replace it with the actual date. The current date is: {current_date}

Query: {query}
Python Arguments:"""


TEXT_TO_REPORT_V3 = """We are going to translate natural language queries into a MySQL query. Assume all fields are in a single table called "mytable".
If a relative date is specified, such as "yesterday", replace it with the actual date. The current date is: {current_date}

Query: {query}
SQL:"""


def parse_text_to_report_v1_output(output):
    """Parse the output of the TEXT_TO_REPORT_V1 chain

    **Parameters:**

    * **output** - (str) The output of the chain, expected to be valid JSON.

    **Returns:**

    (*dict*) - A dict of Zillion report params. Any empty values will be removed.

    """
    if not output:
        return None
    return {k: v for k, v in json.loads(output).items() if v}


def parse_text_to_report_v2_output(output):
    """Parse the output of the TEXT_TO_REPORT_V2 chain, which outputs python arguments"""
    raise NotImplementedError


def parse_text_to_report_v3_output(output):
    """Parse the output of the TEXT_TO_REPORT_V3 chain, would require sqlparse"""
    raise NotImplementedError


PROMPT_CONFIGS = dict(
    v1=dict(
        prompt_text=TEXT_TO_REPORT_V1,
        parser=parse_text_to_report_v1_output,
    ),
    v2=dict(
        prompt_text=TEXT_TO_REPORT_V2,
        parser=parse_text_to_report_v2_output,
    ),
    v3=dict(
        prompt_text=TEXT_TO_REPORT_V3,
        parser=parse_text_to_report_v3_output,
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
    has_field_func = warehouse.has_field
    if field_type == FieldTypes.METRIC:
        has_field_func = warehouse.has_metric
    if field_type == FieldTypes.DIMENSION:
        has_field_func = warehouse.has_dimension

    # Check ~exact matches
    alts = get_field_name_variants(name)
    for alt in alts:
        if has_field_func(alt):
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

    # No good match found but guess the original name
    return name


def map_warehouse_report_params(warehouse, report):
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
            if v and v.lower() == "totals":
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


def text_to_report_params(query, prompt_version="v1"):
    """Convert a natural language input to Zillion report params

    **Parameters:**

    * **query** - (str) The natural language query to convert.
    * **prompt_version** - (str) The version of the prompt to use

    **Returns:**

    (*dict*) - A dict of Zillion report params. The field names extracted are
    not guaranteed to exist in any warehouse or datasource and need to be
    analyzed separately.

    """
    prompt_config = PROMPT_CONFIGS[prompt_version]
    prompt_text = prompt_config["prompt_text"]
    parser = prompt_config["parser"]

    context = dict(
        query=query,
        current_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        yesterday=str(datetime.now().date() - timedelta(days=1)),
        thirty_days_ago=str(datetime.now().date() - timedelta(days=30)),
    )
    prompt = PromptTemplate(
        input_variables=["query", "current_date", "yesterday", "thirty_days_ago"],
        template=prompt_text,
    )

    chain = build_chain(
        prompt,
        model=OPENAI_DAVINCI_MODEL_NAME,
        max_tokens=1000,
        request_timeout=LLM_REQUEST_TIMEOUT,
    )
    llm_start = time.time()
    output = chain.run(**context).strip(". -\n\r")
    info(f"LLM took {time.time() - llm_start:.3f}s")
    return parser(output)


NLP_RELATIONSHIP_TABLE_PROMPT = """Given the following tables, what are the suggested foreign key relationships?
Rules:
- If there isn't a good option just skip that table and output nothing.
- Only include relationships between the tables given below. Do not reference any other tables.
- Ignore any self-referencing keys (i.e. a table with a column that references itself)

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
    Get the NLP table relationships for the given tables.

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
        table_defs.append(
            f"Table: {table_name}\n"
            "Fields:\n"
            f"{chr(10).join(get_column_str(c) for c in table.columns)}"
        )

    table_defs_str = "\n\n".join(table_defs)
    prompt = PromptTemplate(
        input_variables=["table_defs"],
        template=NLP_RELATIONSHIP_TABLE_PROMPT,
    )
    chain = build_chain(prompt)
    llm_start = time.time()
    output = chain.run(table_defs_str).strip(". -\n\r")
    info(output)
    info(f"LLM took {time.time() - llm_start:.3f}s")
    return parse_nlp_table_relationships(output)


NLP_TABLE_PROMPT_TEMPLATE = """For each column in the following table definition, list the following comma separated:

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


def get_nlp_table_info(table):
    """Build a langchain chain to get the table info from the LLM

    **Parameters:**

    * **table** - (*SQLAlchemy table*) The table to analyze

    **Returns:**

    (*dict*) - A mapping of column names to properties

    """
    raiseifnot(table.bind, "Table must be bound to an engine")
    create_table = str(CreateTable(table).compile(table.bind)).strip()
    prompt = PromptTemplate(
        input_variables=["create_table"], template=NLP_TABLE_PROMPT_TEMPLATE
    )
    chain = build_chain(prompt)
    llm_start = time.time()
    output = chain.run(create_table).strip(". -\n\r")
    info(output)
    info(f"LLM took {time.time() - llm_start:.3f}s")
    return parse_nlp_table_info(output)
