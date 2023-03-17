import frappe
from fastapi import Depends, APIRouter
router = APIRouter()

# from restipie.custom_api_core.request import api
# from restipie.custom_api_core import response

# @api("GET", '/v1/api/test3')
def handle_pull(*args, **kwargs):
    '''Handle the pull request from the client'''
    return args, kwargs


def tmp():
    from restipie.custom_api_core.request import api
    return api

# @request.api("GET", "/v1/api/replicache_pull")
# @request.api("GET", '/v1/api/test2')
# def handle_pull(*args, **kwargs):
#     '''Handle the pull request from the client'''
#     return args, kwargs
