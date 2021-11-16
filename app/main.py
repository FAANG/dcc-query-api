from fastapi import FastAPI, Query
from typing import Optional, List
from elasticsearch import Elasticsearch
from decouple import config
from app.utils import generate_request_body, Index, \
    parse_fields, serialize_record, generate_csv_file, DEFAULT_COLUMNS
import json
from fastapi.responses import FileResponse
from elasticsearch import RequestsHttpConnection
import os

app = FastAPI()
NODE = config('NODE')
ES_USER = os.getenv('ES_USER')
ES_PASSWORD = os.getenv('ES_PASSWORD')
es = Elasticsearch([NODE], connection_class=RequestsHttpConnection, \
    http_auth=(ES_USER, ES_PASSWORD), use_ssl=True, verify_certs=False)

@app.get("/search")
def search_mulitple_indices(
    indices: List[Index] = Query(None),
    _source: Optional[str] = Query(None,
        description='Provide comma-separated fields to fetch. \
            For example, organism.text,sex.text'),
    size: Optional[int] = 10, \
    from_: Optional[int] = 0, \
    sort: Optional[str] = Query(None,
        description='Provide fields to sort by, in the format: \
            field1:<asc|desc>,field2:<asc|desc>. For example, \
                organism.text:asc,sex.text:desc'), \
    filters: Optional[List[str]] = Query([], regex=".+=(.+,?)+",
        description='Each filter condition should have the format: \
            field_name=value1,value2. For example, \
                organism.text=Sus scrofa,Gallus gallus'),
    aggs: Optional[List[str]] = Query([], regex=".+=.+", 
        description='Each aggregation item should have the format: \
            agg_name=agg_field. For example, \
                organisms=organism.text'),
    q: Optional[str] = None):
    data = es.search(index=indices, _source=_source,\
        size=size, from_=from_, sort=sort, q=q,\
        body=generate_request_body(filters, aggs))
    return data

@app.get("/columns")
def get_columns_for_all_indices():
    indices = DEFAULT_COLUMNS.keys()
    res = {}
    for index in indices:
        mapping_file = "mapping/" + index + ".json"
        f = open(mapping_file)
        data = json.load(f)
        f.close()
        res[index] = {
            'columns': parse_fields(data, [], []),
            'defaults': DEFAULT_COLUMNS[index]
        }
    return res

@app.get("/download")
def download_as_CSV(
    indices: List[Index] = Query(None),
    _source: Optional[str] = Query(None,
        description='Provide comma-separated fields to fetch. \
            For example, organism.text,sex.text'),
    sort: Optional[str] = Query(None,
        description='Provide fields to sort by, in the format: \
            field1:<asc|desc>,field2:<asc|desc>. For example, \
                organism.text:asc,sex.text:desc'), \
    filters: Optional[List[str]] = Query([], regex=".+=(.+,?)+",
        description='Each filter condition should have the format: \
            field_name=value1,value2. For example, \
                organism.text=Sus scrofa,Gallus gallus'),
    q: Optional[str] = None):
    SIZE = 1000000
    data = es.search(index=indices, _source=_source, size=SIZE, sort=sort, \
        q=q, body=generate_request_body(filters, []))
    records = list(map(lambda rec: rec['_source'], data['hits']['hits']))
    records = list(map(lambda rec: serialize_record(rec, rec, []), records))
    generate_csv_file(records, _source.split(','))
    return FileResponse('data.csv', media_type='text/csv',filename='data.csv')
