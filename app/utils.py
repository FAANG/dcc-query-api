from enum import Enum
import csv
import copy
from fastapi.responses import FileResponse


class Index(str, Enum):
    file = "file"
    organism = "organism"
    specimen = "specimen"
    dataset = "dataset"
    article = "article"
    analysis = "analysis"
    experiment = "experiment"
    protocol_files = "protocol_files"
    protocol_samples = "protocol_samples"
    protocol_analysis = "protocol_analysis"
    file_specimen = "file-specimen"
    dataset_specimen = "dataset-specimen"


DEFAULT_COLUMNS = {
    'file': [
        'filename',
        'study.accession',
        'experiment.accession',
        'species.text',
        'experiment.assayType',
        'experiment.target',
        'specimen',
        'run.instrument',
        'experiment.standardMet',
        'paperPublished'
    ],
    'organism': [
        'biosampleId',
        'sex.text',
        'organism.text',
        'breed.text',
        'standardMet',
        'paperPublished'
    ],
    'specimen': [
        'biosampleId',
        'material.text',
        'cellType.text',
        'organism.sex.text',
        'organism.organism.text',
        'organism.breed.text',
        'standardMet',
        'paperPublished',
    ],
    'dataset': [
        'accession',
        'title',
        'species.text',
        'archive',
        'assayType',
        'standardMet',
        'paperPublished'
    ],
    'article': [
        'title',
        'year',
        'datasetSource',
        'journal'
    ],
    'analysis': [
        'accession',
        'datasetAccession',
        'title',
        'organism.text',
        'assayType',
        'analysisType',
        'standardMet'
    ],
    'experiment': [
        'accession',
        'assayType',
        'experimentTarget',
        'standardMet'
    ],
    'protocol_files': [
        'name',
        'experimentTarget',
        'assayType',
    ],
    'protocol_samples': [
        'protocolName',
        'key',
        'universityName',
        'protocolDate'
    ],
    'protocol_analysis': [
        'analysisType',
        'protocolName',
        'key',
        'universityName',
        'protocolDate'
    ]
}


def generate_request_body(filters, aggs):
    body = {}
    # generate query for filtering
    filter_values = []
    not_filter_values = []
    for filter in filters:
        # each filter is of the following format- 
        # organism.text=Sus scrofa,Gallus gallus, ...
        filter = filter.split('=')
        key = filter[0]
        val = filter[1].split(',')
        if val[0] != 'false':
            filter_values.append({"terms": {key: val}})
        else:
            not_filter_values.append({"terms": {key: ["true"]}})
    filter_val = {}
    if filter_values:
        filter_val['must'] = filter_values
    if not_filter_values:
        filter_val['must_not'] = not_filter_values
    if filter_val:
        body['query'] = {"bool": filter_val}

    # generate query for aggregations
    agg_values = {}
    for agg in aggs:
        # each agg is of the following format- 
        # agg_name=field to aggregate on
        key, val = agg.split('=')
        # size determines number of aggregation buckets returned
        agg_values[key] = {"terms": {"field": val, "size": 25}}
        if key == 'paper_published':
            # aggregations for missing paperPublished field
            agg_values["paper_published_missing"] = {
                "missing": {"field": "paperPublished"}}

    body['aggs'] = agg_values
    return body


def parse_fields(data, mapping, properties):
    data = data['properties']
    for prop in data:
        properties.append(prop)
        if 'properties' in data[prop]:
            mapping = parse_fields(data[prop], mapping, properties)
        else:
            mapping.append('.'.join(properties))
        properties.pop()
    return mapping


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:
            for a in x:
                if type(a) is dict or type(a) is list:
                    flatten(a, name)
                else:
                    k = name[:-1]
                    if k in out:
                        out[k] = out[k] + ",\n" + ','.join(x)
                    else:
                        out[k] = ','.join(x)
                    return
        else:
            k = name[:-1]
            if k in out:
                out[k] = out[k] + ",\n" + x
            else:
                out[k] = x

    flatten(y)
    return out


def update_record(record, accession):
    updated_rec = {**record, **{'study': accession}}
    return updated_rec


def delete_extra_fields(record, fields):
    updated_record = {}
    for prop in record.keys():
        if prop in fields:
            updated_record[prop] = record[prop]
    return updated_record


def generate_delimited_file(records, columns, file_format):
    records = list(map(lambda rec: delete_extra_fields(rec, columns), records))
    headers = {}

    allowed_formats = {
        'csv': ',',
        'tsv': '\t'
    }
    delim = allowed_formats[file_format] if file_format in allowed_formats else allowed_formats['csv']
    file_ext = file_format if file_format in allowed_formats else 'csv'

    for col in columns:
        column = col.split('.')
        if len(column) > 1:
            del column[0]
        if 'text' in column:
            column.remove('text')
        column = ' '.join(column)
        headers[col] = column

    with open(f'data.{file_ext}', 'w') as f:
        dict_writer = csv.DictWriter(f, fieldnames=columns, delimiter=delim)
        dict_writer.writerow(headers)
        dict_writer.writerows(records)
    return FileResponse(f'data.{file_ext}', media_type=f'text/{file_ext}', filename=f'data.{file_ext}')


def perform_join(records1, records2, indices):
    # sample values
    spec = {
        'file-specimen': ['specimen', 'biosampleId'],
        'specimen-file': ['biosampleId', 'specimen']
    }
    if indices in spec:
        combined_records = []
        for rec1 in records1:
            for rec2 in records2:
                if rec1[spec[indices][0]] == rec2[spec[indices][1]]:
                    rec = copy.deepcopy(rec1)
                    for key in rec2.keys():
                        if key != 'index' and key != spec[indices][1]:
                            rec[key] = rec2[key]
                    combined_records.append(rec)
        return combined_records
    return records1 + records2


def process(record):
    rec = record['_source']
    rec['index'] = record['_index']
    if rec['index'] == 'file':
        rec['filename'] = record['_id']
    return rec


def es_fetch_records(indices, source_fields, sort, query_param, filters, aggregates, es):
    count = 0
    recordset = []

    while True:
        res = es.search(index=indices, _source=source_fields, size=50000, from_=count,
                        sort=sort, q=query_param, track_total_hits=True,
                        body=generate_request_body(filters, aggregates))
        count += 50000
        records = list(map(lambda rec: process(rec), res['hits']['hits']))
        recordset += records
        if count > res['hits']['total']['value']:
            break
    return recordset


def get_organism_biosampleId(record, es):
    if 'specimen.material.text' in record and record['specimen.material.text'] == 'pool of specimens':
        if 'specimen.derivedFrom' in record:
            derivedFrom_list = [x.strip() for x in record['specimen.derivedFrom'].split(',')]
            organism_biosampleId = list(map(lambda specimen_id: specimen_organism_biosampleId(specimen_id, es), derivedFrom_list))
            record['specimen.organism.biosampleId'] = list(set(organism_biosampleId))
    return record


def specimen_organism_biosampleId(specimen_id, es):
    specimen_data = es.search(index=['specimen'], _source='biosampleId,material.text,organism.biosampleId',
                              size=1, from_=0, q=f"biosampleId:{specimen_id}", track_total_hits=True)

    source_data = specimen_data['hits']['hits'][0]['_source']
    if source_data['material']['text'] == 'specimen from organism':
        return source_data['organism']['biosampleId']

