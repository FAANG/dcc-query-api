from enum import Enum
import csv
import copy


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


def delete_extra_fields(record, fields):
    updated_record = {}
    for prop in record.keys():
        if prop in fields:
            updated_record[prop] = record[prop]
    return updated_record


def generate_delimited_file(records, columns, delim, file_ext):
    records = list(map(lambda rec: delete_extra_fields(rec, columns), records))
    headers = {}
    for col in columns:
        column = col.split('.')
        if 'text' in column:
            column.remove('text')
        column = ' '.join(column)
        headers[col] = column

    with open(f'data.{file_ext}', 'w') as f:
        dict_writer = csv.DictWriter(f, fieldnames=columns, delimiter=delim)
        dict_writer.writerow(headers)
        dict_writer.writerows(records)


def perform_join(records1, records2, indices):
    # sample values
    spec = {
        'file-specimen': ['specimen','biosampleId'],
        'specimen-file': ['biosampleId','specimen']
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
