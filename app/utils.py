from enum import Enum
import csv

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

DEFAULT_COLUMNS = {
        'file': [
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

def serialize_record(parentRecord, currentRecord, parentProps):
    for prop in currentRecord.copy():
        parentProps.append(prop)
        if type(currentRecord[prop]) is list and len(currentRecord[prop]):
            currentRecord[prop] = flatten_list(currentRecord[prop])
        if type(currentRecord[prop]) is dict:
            serialize_record(parentRecord, currentRecord[prop], parentProps)
        else:
            serailisedProp = '.'.join(parentProps)
            parentRecord[serailisedProp] = currentRecord[prop]
        parentProps.pop()
    return parentRecord

def flatten_list(items):
    if type(items[0]) is dict:
        result = {}
        for i in range(len(items)):
            for prop in items[i]:
                if prop in result:
                    result[prop] = result[prop] + ',\n' + items[i][prop]
                else:
                    result[prop] = items[i][prop]
        return result
    else:
        return ',\n'.join(items)

def delete_extra_fields(record, fields):
    updated_record = {}
    for prop in record.keys():
        if prop in fields:
            updated_record[prop] = record[prop]
    return updated_record

def generate_csv_file(records, columns):
    records = list(map(lambda rec: delete_extra_fields(rec, columns), records))
    with open('data.csv', 'w') as csvfile: 
        dict_writer = csv.DictWriter(csvfile, columns)
        dict_writer.writeheader()
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
                if rec1[spec[0]] == rec2[spec[1]]:
                    rec = dict(rec1.items() + rec2.items())
                    # key of joined record is the key of left table record
                    # delete key of right table record
                    del rec[spec[1]] 
                    combined_records.append(rec)
        return combined_records
    return records1 + records2