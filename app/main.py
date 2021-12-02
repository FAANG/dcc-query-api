from fastapi import FastAPI, Query
from typing import Optional, List
from elasticsearch import Elasticsearch, RequestsHttpConnection
from decouple import config
from app.utils import generate_request_body, Index, \
    parse_fields, serialize_record, generate_csv_file, \
    perform_join, process, remove_nested_fields, DEFAULT_COLUMNS
import json
from fastapi.responses import FileResponse
import os

app = FastAPI()
NODE = config('NODE')
ES_USER = os.getenv('ES_USER')
# ES_USER = config('ES_USER') 
ES_PASSWORD = os.getenv('ES_PASSWORD')
# ES_PASSWORD = config('ES_PASSWORD')
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
        size=size, from_=from_, sort=sort, q=q, track_total_hits=True,\
        body=generate_request_body(filters, aggs))
    count = data['hits']['total']['value']
    records = list(map(lambda rec: process(rec), data['hits']['hits']))
    records = list(map(lambda rec: serialize_record(rec, rec, []), records))
    records = list(map(lambda rec: remove_nested_fields(rec, _source), records))
    return {
        'data': records,
        'count': count
    }

@app.get("/join_search")
def fetch_all_records(
    index1: Index = '',
    index2: Index = '',
    source1: Optional[str] = Query(None,
        description='Provide comma-separated fields to fetch. \
            For example, organism.text,sex.text'),
    source2: Optional[str] = Query(None,
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
    '''
    Joins records fetched from index1 and index2
    index1 LEFT JOIN index2
    '''
    # tables = {index1: [], index2: []}
    # fields = {index1: source1, index2: source2}
    # for index in tables.keys():
    #     data = []
    #     count = 0
    #     while True:
    #         res = es.search(index=index, _source=fields[index],\
    #             size=10000, from_=0, track_total_hits=True)
    #         count += 10000
    #         records = list(map(lambda rec: process(rec), res['hits']['hits']))
    #         data += records
    #         if count > res['hits']['total']['value']:
    #             break
    #     tables[index] = list(map(lambda rec: serialize_record(rec, rec, []), data))
    #     tables[index] = list(map(lambda rec: remove_nested_fields(rec, fields[index1] + ',' + fields[index2]), tables[index]))
    # indices = index1 + '-' + index2
    # data = perform_join(tables[index1], tables[index2], indices)
    # count = len(data)
    indices = index1 + '-' + index2
    source = source1 + ',' + source2
    data = es.search(index=indices, _source=source,\
        size=size, from_=from_, sort=sort, q=q, track_total_hits=True,\
        body=generate_request_body(filters, aggs))
    count = data['hits']['total']['value']
    records = list(map(lambda rec: process(rec), data['hits']['hits']))
    records = list(map(lambda rec: serialize_record(rec, rec, []), records))
    records = list(map(lambda rec: remove_nested_fields(rec, source), records))
    return {
        'data': records,
        'count': count
    }

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
        q=q, body=generate_request_body(filters, []), track_total_hits=True)
    records = list(map(lambda rec: rec['_source'], data['hits']['hits']))
    records = list(map(lambda rec: serialize_record(rec, rec, []), records))
    records = list(map(lambda rec: remove_nested_fields(rec, _source), records))
    generate_csv_file(records, _source.split(','))
    return FileResponse('data.csv', media_type='text/csv',filename='data.csv')
