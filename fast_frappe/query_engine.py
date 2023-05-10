import json
import time
from typing import Optional, Union
from fastapi import APIRouter
# import frappe
# from typing import List
# from frappe_prompt.frappe_prompt.doctype.weaviatedocnode.weaviatedocnode import WeaviateDocNode
import pandas as pd
from pydantic import BaseModel
from llama_index import GPTVectorStoreIndex, Response, StorageContext
from llama_index.vector_stores import WeaviateVectorStore
from functools import wraps
router = APIRouter()



class WeaviateDocNode(Document):
	client = weaviate.Client("http://weaviate:8080")


def log_execution_time(func):
    # logger = frappe.logger("antler")
    # logger = frappe.logger("antler", allow_site=True, file_count=50)
    # logger.setLevel(frappe.log.DEBUG)

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        # logger.info(f"Execution time of '{func.__name__}': {elapsed_time:.4f} seconds")
        return result

    return wrapper


def llama_index_custom_qa(question: str, client) -> Response:
    """
    Custom QA function that uses the OpenAI API to answer questions.
    """
    vector_store = WeaviateVectorStore(weaviate_client=client, class_prefix='AntlerDocument')
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    # index = GPTVectorStoreIndex.from_documents([], storage_context=storage_context, service_context=)
    start_time_lookup_vector = time.perf_counter()
    index = GPTVectorStoreIndex.from_documents([], storage_context=storage_context)
    end_time_lookup_vector = time.perf_counter()
    response = index.as_query_engine(streaming=True, similarity_top_k=1).query(question)
    end_time_synthesis = time.perf_counter()

    vector_lookup_time = end_time_lookup_vector - start_time_lookup_vector
    synthesis_time = end_time_synthesis - end_time_lookup_vector

    # logger = frappe.logger("antler", allow_site=True, file_count=50)
    print(f"Vector lookup time: {vector_lookup_time:.4f} seconds")
    print(f"Synthesis time: {synthesis_time:.4f} seconds")

    # logger.info(f"Vector lookup time: {vector_lookup_time:.4f} seconds")
    # logger.info(f"Synthesis time: {synthesis_time:.4f} seconds")
    return index.index_id, response


# def parse_llama_index_response(response: Response) -> tuple(str, dict):
# def parse_llama_index_response(response: Response):
#     """
#     Parse the response from the llama index into a custom pandas dataframe.
#     """
#     # df = pd.DataFrame(response.data)
#     if response.response:
#         df = pd.DataFrame(response.source_nodes).node.apply(pd.Series)
#         df['ref_doc_id'] = df.relationships.apply(lambda x: x['1'])
#         selected_cols = ['text', 'doc_id', 'ref_doc_id']
#         _custom_response = df[selected_cols]
#         # .to_dict(orient='records')
#         custom_group = {}
#         for ref_doc_id, _df in pd.DataFrame(_custom_response).groupby('ref_doc_id'):
#             custom_group[ref_doc_id] = _df[['text', 'doc_id']].to_dict(orient='records')
#         # return response.response, custom_res
#         return json.dumps({'response': response.response, 'custom_response': custom_group})
#     else:
#         return None

class CustomResponse(BaseModel):
    response: Optional[bool]
    custom_response: Optional[Union[list, None]]

@router.get('/api/v1/parse_llama_index_response', response_model=CustomResponse)
def parse_llama_index_response(response: Response) -> CustomResponse:
    """
    Parse the response from the llama index into a custom pandas dataframe.
    """
    if response.response:
        df = pd.DataFrame(response.source_nodes).node.apply(pd.Series)
        df['ref_doc_id'] = df.relationships.apply(lambda x: x['1'])
        selected_cols = ['text', 'doc_id', 'ref_doc_id']
        _custom_response = df[selected_cols]
        custom_group = []
        for ref_doc_id, _df in pd.DataFrame(_custom_response).groupby('ref_doc_id'):
            _custom_group = {}
            _custom_group['ref_doc_id'] = ref_doc_id
            _custom_group['nodes'] = _df[['text', 'doc_id']].to_dict(orient='records')
            custom_group.append(_custom_group)
        return {'response': response.response, 'custom_response': custom_group}
    else:
        return {'response': None, 'custom_response': None}



@router.get('/api/v1/parse_llama_index_response')
def parse_llama_index_response(response):
    """
    Parse the response from the llama index into a custom pandas dataframe.
    """
    # df = pd.DataFrame(response.data)
    if response.response:
        df = pd.DataFrame(response.source_nodes).node.apply(pd.Series)
        df['ref_doc_id'] = df.relationships.apply(lambda x: x['1'])
        selected_cols = ['text', 'doc_id', 'ref_doc_id']
        _custom_response = df[selected_cols]
        # .to_dict(orient='records')
        custom_group = []
        for ref_doc_id, _df in pd.DataFrame(_custom_response).groupby('ref_doc_id'):
            _custom_group = {}
            _custom_group['ref_doc_id'] = ref_doc_id
            _custom_group['nodes'] = _df[['text', 'doc_id']].to_dict(orient='records')
            custom_group.append(_custom_group)
        # return response.response, custom_res
        return json.dumps({'response': response.response, 'custom_response': custom_group})
    else:
        return json.dumps({'response': None, 'custom_response': None})


# @log_execution_time()
# @frappe.whitelist()
async def get_llama_index_response(question: str = 'compare and contrast sport teams in New York City and Boston'):
    """
    Get the response from the llama index.
    """
    client = WeaviateDocNode.client
    index_id, response = await llama_index_custom_qa(question, client)
    # print()
    return parse_llama_index_response(response)
    # return json.dumps(response.response)
    # start_time = time.time()
    # while True:
    #     # Perform your operation here
    #     # ...

    #     # Check if the timeout has been reached
    #     if time.time() - start_time >= 2:
    #         print("Timeout reached")
    #         break
    # return question


# @frappe.whitelist()
def get_llama_index_response(question: str = 'compare and contrast sport teams in New York City and Boston'):
    """
    Get the response from the llama index.
    """
    client = WeaviateDocNode.client
    response = llama_index_custom_qa(question, client)
    # print()
    return parse_llama_index_response(response)
