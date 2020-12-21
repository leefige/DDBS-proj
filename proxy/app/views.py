from django.http import HttpResponse
from proxy import Proxy
import json

proxy = Proxy("mongo", "hd-name")

def index(request):
    return HttpResponse("Hello, world.")

def query(request, collection:str, id:str):
    res = proxy.query_collection(collection, {'id': id})
    return HttpResponse(json.dumps(res))

def user_read(request, id:str):
    res = proxy.query_user_read({'id': id})
    return HttpResponse(json.dumps(res))

def top(request, temporal:str):
    res = proxy.retrieve_top_5(temporal)
    return HttpResponse(json.dumps(res))
