import json

from django.http import HttpResponse, Http404
from django.shortcuts import render

from proxy import Proxy

proxy = Proxy("mongo", "hd-name")


def index(request):
    return HttpResponse("Hello, world.")


def query(request, collection: str, id: str):
    res = proxy.query_collection(collection, {'id': id})
    res = [json.dumps(item) for item in res]
    return HttpResponse('<br/><br/>'.join(res))


def user_read(request, id: str):
    res = proxy.query_user_read({'id': id})
    if len(res) > 0:
        res = res[0]
        reads = res['read_list']
        res.pop('read_list')
        reads = [json.dumps(item) for item in reads]
        nos = list(range(1, len(reads) + 1))
        reads = zip([str(i) for i in nos], reads)
        reads = ['. '.join(item) for item in reads]
        output = '<br/><br/>'.join(reads)
        output = '<br/><br/><br/><br/>'.join(
            ("User:<br/><br/>" + json.dumps(res), "Reading list:<br/><br/>" + output))
        return HttpResponse(output)
    else:
        return HttpResponse("No record found")


def update(request, collection: str, id: str, updatement: str):
    updatement = json.loads(updatement)
    res = proxy.update_one(collection, {'id': id}, {"$set": updatement})
    return HttpResponse(f"Found {res[0]}, updated {res[1]}")


def top(request, temporal: str):
    res = proxy.retrieve_top_5(temporal)
    res = [json.dumps(item) for item in res]
    nos = list(range(1, len(res) + 1))
    nos = [str(i) for i in nos]
    res = zip(nos, res)
    res = ['. '.join(item) for item in res]
    return HttpResponse('<br/><br/>'.join(res))


def detail(request, id: str, file_type: str, file_name: str):
    proxy.query_collection('article', {'id': id})
    context = {
        'file': f"article{id[1:]}/{file_name}"
    }
    if file_type == 'text':
        with open(f"/var/tmp/proxy/article{id[1:]}/text_{id}.txt") as fin:
            res = fin.read()
        return HttpResponse(res)
    elif file_type == 'image':
        return render(request, 'app/image.html', context)
    elif file_type == 'video':
        return render(request, 'app/video.html', context)
    else:
        raise Http404("Unknown file type")
