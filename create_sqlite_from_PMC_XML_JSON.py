#!/usr/bin/env python

from Bio import Entrez

import xml.etree.ElementTree as ET
import json
import argparse
import sqlite3
import tarfile
import time
from crossref.restful import Works

start_time = time.time()

def get_args():
    print('Getting data from arguments.')
    version = 0.01
    parser = argparse.ArgumentParser(description='Stores articles in sqlite database',
                                    add_help=True)
    parser.add_argument('-v', '--version', action='version', version=version)
    parser.add_argument('--database', dest='db',
                        metavar='literature.db', type=str,
                        help='Database name',
                        required=True)
    parser.add_argument('--pmc_gz', dest='gz_file',
                        metavar='PMC000XXXXX_article_data.{json,xml}.tar.gz', type=str,
                        help='BioC PMC articles in JSON or XML format (gz)', ...
                        required=True)
    parser.add_argument('--doi_to_keep', dest='doi_file',
                        metavar='doi.txt', type=str,
                        help='DOI list for selected journals',
                        required=False)
    parser.add_argument('--min_year', dest='min_year',
                        metavar='2005', type=int,
                        help='Consider only articles published this year or after',
                        required=False)
    parser.add_argument('--log_file', dest='log_file',
                        metavar='log.txt', type=str,
                        help='Log file',
                        required=False)
    parser.add_argument('--buffer_size', dest='buffer_size',
                        metavar='400', type=int,
                        help='Buffer size (number of inserts in batch)',
                        required=False, default=400)

    args = parser.parse_args()
    database = args.db
    pmc_file = args.gz_file
    min_year = args.min_year
    buffer_size = args.buffer_size

    if args.doi_file:
        doi_list_file = args.doi_file
    else:
        doi_list_file = ''

    if args.log_file:
        log_file = args.log_file
    else:
        log_file = ''

    return database, pmc_file, doi_list_file, min_year, log_file, buffer_size

def set_database(db):
    # Create sqlite3 database if it does not exist
    conn = sqlite3.connect(db)
    c = conn.cursor()
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS pcw_literature
                (pmid integer, title text, year integer, doi text,
                journal_name text, first_author text, abstract text, 
                content text, methods text, results text)''')
    conn.commit()
    return conn, c

def get_first_author_name(infos):
    if 'name_0' in infos:
        author_infos = infos['name_0'].split(";")
        first_author_infos = author_infos[0].split(":")
        first_author = first_author_infos[1]
    else:
        first_author = ''
    return first_author

# LABEL: Reading Args
db, pmc_file, doi_list_file, min_year, log_file, max_buffer_size = get_args()

doi_list_to_keep = {}

sections_in_db = []

if doi_list_file:
    with open(doi_list_file) as f:
        dois = [doi.rstrip() for doi in f]
        
        # print(dois)

        for list_doi in dois:
            suffix = list_doi.split("/")
            prefix = suffix.pop(0)
            suffix = "/".join(suffix)

            if prefix in doi_list_to_keep.keys():
                doi_list_to_keep[prefix].append(suffix)
            else:
                doi_list_to_keep[prefix] = [suffix]

# LABEL: Configure Database Connection
# print(f'Configuring database: {db}')
db_conn, db_cursor = set_database(db)

#print(f'Uncompressing gzip file: {pmc_file}')
#gzip.decompress(gzip.open(pmc_file, 'rb'))

report_stats = {"missing_pmid":0,
                "missing_doi":0,
                "normal_cases_pmid":0,
                "normal_cases_doi":0,
                "filtered_doi_prefix":0}

data_buffer = []

with tarfile.open(pmc_file, 'r:gz') as tar:
    for member in tar.getmembers():

        if len(data_buffer) >= max_buffer_size:
            db_cursor.executemany('INSERT INTO pcw_literature VALUES (?,?,?,?,?,?,?,?,?,?)', data_buffer)
            db_conn.commit()
            # print(f'{len(data_buffer)} articles successfully added to the database.')
            data_buffer = []

        f = tar.extractfile(member)
        content = f.read()
        first_few_chars = content[:100].decode('utf-8')  
        if '<article' in first_few_chars:  
            root = ET.fromstring(content)
            
            # Extract pmid
	pmid_node = root.find(".//article-id[@pub-id-type='pmid']")
	pmid = int(pmid_node.text) if pmid_node is not None else 0

	# Extract title
	title_node = root.find(".//article-meta//article-title")
	title = title_node.text if title_node is not None else ''

	# Extract year
	year_node = root.find(".//pub-date/year")
	year = int(year_node.text) if year_node is not None else 0

	# Extract doi
	doi_node = root.find(".//article-id[@pub-id-type='doi']")
	doi = doi_node.text if doi_node is not None else ''

	# Extract first_author 
	first_author_node = root.find(".//contrib-group/contrib[@contrib-type='author']/name")
		if first_author_node is not None:
    			first_name = first_author_node.find('given-names').text if first_author_node.find('given-names') is not None else ''
    			last_name = first_author_node.find('surname').text if first_author_node.find('surname') is not None else ''
    			first_author = f"{first_name} {last_name}"
		else:
    first_author = ''

	# Extract abstract
	abstract_node = root.find(".//abstract")
	abstract_text = abstract_node.text if abstract_node is not None else ''

	# Extract content (considering 'body' as the main article content)
	content_node = root.find(".//body")
	content_text = ET.tostring(content_node, encoding='utf-8', method='text').decode('utf-8') if content_node is not None else ''
	# Extract Methods section
	methods_node = root.find(".//sec[title='Methods']")  # Adjust the XPath if the title differs
	methods_text = ET.tostring(methods_node, encoding='utf-8', method='text').decode('utf-8') if methods_node is not None else ''

	# Extract Results section
	results_node = root.find(".//sec[title='Results']")  # Adjust the XPath if the title differs
	results_text = ET.tostring(results_node, encoding='utf-8', method='text').decode('utf-8') if results_node is not None else ''

        else:
            data_json = json.loads(content)
            pas = data_json['documents'][0]['passages']
            if "article-id_pmid" in pas[0]['infons'].keys():
                pmid = pas[0]['infons']['article-id_pmid']
                report_stats['normal_cases_pmid']+=1
            else:
                pmid = 0
                report_stats['missing_pmid']+=1

            title = pas[0]['text']
            if "year" in pas[0]['infons']:
                year = int(pas[0]['infons']['year'])
            else:
                year = 0

            if min_year:
                if year < min_year:
                    continue

            if "article-id_doi" in pas[0]['infons']:
                doi = pas[0]['infons']['article-id_doi']
                report_stats['normal_cases_doi']+=1
            else:
                doi = ''
                report_stats['missing_doi']+=1

            first_author = get_first_author_name(pas[0]['infons'])

            abs = ''
            article_text = ''
            section_title = ''

            for p in pas:
                if 'section_type' not in p['infons'].keys():
                        print(f'section_type does not exist for DOI: {doi}')
                else:
                    if(p['infons']['section_type'] not in ["ABSTRACT", "ACK_FUND", "COMP_INT",
                                                           "REF", "ABBR", "REVIEW_INFO",
                                                           "SUPPL", "TABLE", "TITLE", "APPENDIX",
                                                           "AUTH_CONT", "CASE", "KEYWORD"]):

                        if p['infons']['section_type'] not in sections_in_db:
                            sections_in_db.append(p['infons']['section_type'])

                        if section_title != p['infons']['section_type']:
                            article_text += p['infons']['section_type'] + '\n\n'
                            section_title = p['infons']['section_type']
                        article_text += p['text'] + '\n\n'
                    elif p['infons']['section_type'] == "ABSTRACT":
                        abs += p['text'] + '\n\n' # When it finds the abstract, it appends it to a string
            
            if doi_list_file:
                if doi:
                    suffix = doi.split("/")
                    prefix = suffix.pop(0)
                    suffix = "/".join(suffix)

                    if prefix in doi_list_to_keep.keys():
                        for doi_journal in doi_list_to_keep[prefix]:
                            if suffix.startswith(doi_journal):
                                works = Works()
                                works_res = works.doi(doi)
                                time.sleep(0)
                                if 'container-title' in works_res.keys():
                                    journal_name = works_res['container-title'][0]
                                else:
                                    journal_name = ''
                                report_stats["filtered_doi_prefix"]+=1
                                data_buffer.append((pmid, title, year, doi, journal_name, first_author, abstract_text, content_text, methods_text, results_text))

            else:
                #Precisa inserir o journal_name
                journal_name = ' '
                data_buffer.append((pmid,title,year,doi,journal_name,first_author,abs,article_text))

        if report_stats['normal_cases_pmid'] % 1000 == 0:
            print('Seconds: ', time.time() - start_time, 'Normal cases: ', report_stats['normal_cases_pmid'])
        if (report_stats['missing_pmid'] % 1000 == 0) and (report_stats['missing_pmid'] != 0):
            print('Seconds: ', time.time() - start_time, 'Missing pmid: ', report_stats['missing_pmid'])

    if len(data_buffer) > 0:
        db_cursor.executemany('INSERT INTO pcw_literature VALUES (?,?,?,?,?,?,?,?,?,?)', data_buffer)
        db_conn.commit()
        # print(f'{len(data_buffer)} articles successfully added to the database.')
        data_buffer = []

if log_file:
    log_file_obj = open(log_file, "a")
    
    for section in sections_in_db:
        log_file_obj.write(f"{section}\n")

    log_file_obj.close()
            
print('normal cases: ',report_stats['normal_cases_pmid'])
print('missing pmid: ',report_stats['missing_pmid'])

print('normal cases: ',report_stats['normal_cases_doi'])
print('missing doi: ',report_stats['missing_doi'])

print('filtered prefix doi: ',report_stats['filtered_doi_prefix'])
