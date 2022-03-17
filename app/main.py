import math
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Text
from elasticsearch import Elasticsearch, RequestsHttpConnection, exceptions
from decouple import config
from app.utils import generate_request_body, Index,\
    parse_fields, generate_delimited_file, flatten_json,\
    perform_join, update_record, es_fetch_records, process,\
    get_organism_biosampleId, DEFAULT_COLUMNS
import json
from fastapi.responses import FileResponse
import os

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NODE = config('NODE')
# ES_USER = os.getenv('ES_USER')
ES_USER = config('ES_USER')
# ES_PASSWORD = os.getenv('ES_PASSWORD')
ES_PASSWORD = config('ES_PASSWORD')
es = Elasticsearch([NODE], connection_class=RequestsHttpConnection,
                   http_auth=(ES_USER, ES_PASSWORD), use_ssl=True, verify_certs=False)

@app.get("/search")
def search_mulitple_indices(
    indices: List[Index] = Query(None),
    _source: Optional[str] = Query(None,
                                   description='Provide comma-separated fields to fetch. \
            For example, organism.text,sex.text'),
    size: Optional[int] = 10,
    from_: Optional[int] = 0,
    sort: Optional[str] = Query(None,
                                description='Provide fields to sort by, in the format: \
            field1:<asc|desc>,field2:<asc|desc>. For example, \
                organism.text:asc,sex.text:desc'),
    filters: Optional[List[str]] = Query([], regex=".+=(.+,?)+",
                                         description='Each filter condition should have the format: \
            field_name=value1,value2. For example, \
                organism.text=Sus scrofa,Gallus gallus'),
    aggs: Optional[List[str]] = Query([], regex=".+=.+",
                                      description='Each aggregation item should have the format: \
            agg_name=agg_field. For example, \
                organisms=organism.text'),
    q: Optional[str] = None):
    data = es.search(index=indices, _source=_source, \
                     size=size, from_=from_, sort=sort, q=q, track_total_hits=True, \
                     body=generate_request_body(filters, aggs))
    count = data['hits']['total']['value']
    records = list(map(lambda rec: process(rec), data['hits']['hits']))
    records = list(map(lambda rec: flatten_json(rec), records))
    return {
        'data': records,
        'count': count
    }


@app.get("/join_search")
def fetch_all_records(
    index1: Index = '',
    index2: Index = '',
    _source: Optional[str] = Query(None,
                                   description='Provide comma-separated fields to fetch. \
            For example, organism.text,sex.text'),
    size: Optional[int] = 10,
    from_: Optional[int] = 0,
    sort: Optional[str] = Query(None,
                                description='Provide fields to sort by, in the format: \
            field1:<asc|desc>,field2:<asc|desc>. For example, \
                organism.text:asc,sex.text:desc'),
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
    try:
        data = es.search(index=indices, _source=_source,
                         size=size, from_=from_, sort=sort, q=q, track_total_hits=True,
                         body=generate_request_body(filters, aggs))
        count = data['hits']['total']['value']
        records = list(map(lambda rec: process(rec), data['hits']['hits']))
        records = list(map(lambda rec: flatten_json(rec), records))
        return {
            'data': records,
            'count': count
        }
    except exceptions.NotFoundError:
        raise HTTPException(status_code=400, detail="Indices cannot be combined")


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
def download_delimited_file(
    indices: List[Index] = Query(None),
    _source: Optional[str] = Query(None,
                                   description='Provide comma-separated fields to fetch. \
            For example, organism.text,sex.text'),
    sort: Optional[str] = Query(None,
                                description='Provide fields to sort by, in the format: \
            field1:<asc|desc>,field2:<asc|desc>. For example, \
                organism.text:asc,sex.text:desc'),
    filters: Optional[List[str]] = Query([], regex=".+=(.+,?)+",
                                         description='Each filter condition should have the format: \
            field_name=value1,value2. For example, \
                organism.text=Sus scrofa,Gallus gallus'),
    aggs: Optional[List[str]] = Query([], regex=".+=.+",
                                      description='Each aggregation item should have the format: \
            agg_name=agg_field. For example, \
                organisms=organism.text'),
    file_format: Optional[str] = Query(None),
    q: Optional[str] = None):

    recordset = es_fetch_records(indices=indices,
                                 source_fields=_source,
                                 sort=sort,
                                 query_param=q,
                                 filters=filters,
                                 aggregates=aggs,
                                 es=es)

    records = list(map(lambda rec: flatten_json(rec), recordset))
    return generate_delimited_file(records, _source.split(','), file_format)


@app.get("/downloadDatasetFiles")
def download_dataset_file(
    indices: List[Index] = Query(None),
    _source: Optional[str] = Query(None,
                                   description='Provide comma-separated fields to fetch. \
            For example, organism.text,sex.text'),
    sort: Optional[str] = Query(None,
                                description='Provide fields to sort by, in the format: \
            field1:<asc|desc>,field2:<asc|desc>. For example, \
                organism.text:asc,sex.text:desc'),
    filters: Optional[List[str]] = Query([], regex=".+=(.+,?)+",
                                         description='Each filter condition should have the format: \
            field_name=value1,value2. For example, \
                organism.text=Sus scrofa,Gallus gallus'),
    aggs: Optional[List[str]] = Query([], regex=".+=.+",
                                      description='Each aggregation item should have the format: \
            agg_name=agg_field. For example, \
                organisms=organism.text'),
    file_format: Optional[str] = Query(None),
    accession: Optional[str] = None):
    dataset_data = es.search(index=indices,
                             size=1, from_=0, sort=sort, q=f"accession:{accession}", track_total_hits=True, \
                             body=generate_request_body(filters, aggs))

    if int(dataset_data['hits']['total']['value']) > 0:
        dataset_record = list(map(lambda rec: process(rec), dataset_data['hits']['hits']))
        dataset_record = list(map(lambda rec: flatten_json(rec), dataset_record))

        file_id_arr = dataset_record[0]['file.fileId'].split(",\n")

        start_from = 0
        every_nth = 200
        run_times = math.ceil((len(file_id_arr))/every_nth)
        complete_recordset = []
        for i in range(run_times):
            file_id_subarr = file_id_arr[start_from:every_nth]
            file_id_string = ','.join(file_id_subarr)

            source_fields_list = [x.strip() for x in _source.split(',')]
            source_fields_str = ','.join(source_fields_list) + ',specimen.material.text,specimen.derivedFrom'

            recordset = es_fetch_records(indices=['file-specimen'],
                                         source_fields=source_fields_str,
                                         sort=sort,
                                         query_param=f"file.filename:{file_id_string}",
                                         filters=filters,
                                         aggregates=aggs,
                                         es=es)
            records = list(map(lambda rec: get_organism_biosampleId(rec, es), recordset))
            records = list(map(lambda rec: flatten_json(rec), records))
            records = list(map(lambda rec: update_record(rec, accession), records))
            complete_recordset += records
            start_from = every_nth
            every_nth = every_nth + 200

        return generate_delimited_file(complete_recordset, source_fields_list, file_format)