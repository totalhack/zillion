def raise_not_installed(*args, **kwargs):
    raise ImportError("langchain is not installed. Did you install zillion[nlp]?")


try:
    from langchain.chains import LLMChain
    from langchain.llms import OpenAI, OpenAIChat
    from langchain.prompts import PromptTemplate
except ImportError:
    PromptTemplate = raise_not_installed
    LLMChain = None
    OpenAI = None
    OpenAIChat = None

from tlbx import raiseifnot

from zillion.core import info, zillion_config

LLM_MAX_TOKENS = -1
LLM_REQUEST_TIMEOUT = 20
OPENAI_DAVINCI_MODEL_NAME = "text-davinci-003"


def build_chain(prompt, max_tokens=LLM_MAX_TOKENS, request_timeout=LLM_REQUEST_TIMEOUT):
    """Build a chain using langchain and the OpenAI API.

    **Parameters:**

    * **prompt** - (str) A `PromptTemplate` object.
    * **max_tokens** - (int) The maximum number of tokens to generate.
    * **request_timeout** - (int) The maximum number of seconds to wait for a response from the OpenAI API.

    **Returns:**

    (*LLMChain*) - A langchain LLMChain object.

    """
    model = zillion_config["OPENAI_MODEL"]
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
