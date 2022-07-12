import csv
import json
from redis import ReadOnlyError
import ujson
import os

REPEAT_SUBJECT_COLS = ['BindingDB Target Chain  Sequence', 'PDB ID(s) of Target Chain', 'UniProt (SwissProt) Recommended Name of Target Chain', 'UniProt (SwissProt) Entry Name of Target Chain', 'UniProt (SwissProt) Primary ID of Target Chain', 'UniProt (SwissProt) Secondary ID(s) of Target Chain', 'UniProt (SwissProt) Alternative ID(s) of Target Chain', 'UniProt (TrEMBL) Submitted Name of Target Chain', 'UniProt (TrEMBL) Entry Name of Target Chain', 'UniProt (TrEMBL) Primary ID of Target Chain', 'UniProt (TrEMBL) Secondary ID(s) of Target Chain', 'UniProt (TrEMBL) Alternative ID(s) of Target Chain']
BASE_COLS = ['BindingDB Reactant_set_id', 'Ligand SMILES', 'Ligand InChI', 'Ligand InChI Key', 'BindingDB MonomerID', 'BindingDB Ligand Name', 'Target Name Assigned by Curator or DataSource', 'Target Source Organism According to Curator or DataSource', 'Ki (nM)', 'IC50 (nM)', 'Kd (nM)', 'EC50 (nM)', 'kon (M-1-s-1)', 'koff (s-1)', 'pH', 'Temp (C)', 'Curation/DataSource', 'Article DOI', 'PMID', 'PubChem AID', 'Patent Number', 'Authors', 'Institution', 'Link to Ligand in BindingDB', 'Link to Target in BindingDB', 'Link to Ligand-Target Pair in BindingDB', 'Ligand HET ID in PDB', 'PDB ID(s) for Ligand-Target Complex', 'PubChem CID', 'PubChem SID', 'ChEBI ID of Ligand', 'ChEMBL ID of Ligand', 'DrugBank ID of Ligand', 'IUPHAR_GRAC ID of Ligand', 'KEGG ID of Ligand', 'ZINC ID of Ligand', 'Number of Protein Chains in Target (>1 implies a multichain complex)']
COLUMN_DATA = ujson.load(open("./mappings.json"))['columns']

def append_field(doc: dict, key: str, value: any):
  key_path = COLUMN_DATA[key]['location']
  keys = key_path.split('.')
  key_ref = doc
  for i in keys[:len(keys)-1]:
    key_ref = key_ref[i]
  key_ref[keys[-1]].append(value)

def set_field(doc: dict, key: str, value: any):
  key_path = COLUMN_DATA[key]['location']
  keys = key_path.split('.')
  key_ref = doc
  for i in keys[:len(keys)-1]:
    key_ref = key_ref[i]
  key_ref[keys[-1]] = value

def get_field(doc: dict, key: str):
  key_path = COLUMN_DATA[key]['location']
  keys = key_path.split('.')
  val = doc
  try:
    for i in keys:
      val = val[i]
    return val
  except KeyError:
    return None

def special_copy(base_dict):
  ret = {}
  ret['subject'] = base_dict['subject'].copy()
  ret['object'] = base_dict['object'].copy()
  ret['relation'] = base_dict['relation'].copy()
  ret['subject']['uniprot'] = {}
  return ret

def process_field(field_name: str, value: str):
  field_type = COLUMN_DATA[field_name]['type']
  if field_type == "int":
    return int(value)
  if field_type == "split_comma":
    return value.split(',')
  if field_type == "split_semicolon":
    return value.split('; ')
  return value

def read_csv(file: str, delim: str):
  with open(file, encoding='utf-8') as csv_file:
    reader = csv.reader(csv_file, delimiter=delim)
    first_line = True
    for row in reader:
      if len(row) == 0:
        continue
      if first_line:
        first_line = False
        continue
      else:
        base = {'object': {}, 'subject': {}, 'relation': {}}
        for j in range(37):
          if row[j] != None and row[j] != '' and row[j] != 'NULL':
            val = process_field(BASE_COLS[j], row[j])
            set_field(base, BASE_COLS[j], val)
        
        repeats = int(row[36]) # Number of Protein Chains in Target
        pos = 37
        for j in range(repeats):
          info_1 = special_copy(base)
          info_2 = special_copy(base)
          info_1['subject']['uniprot']['type'] = 'swissprot'
          info_2['subject']['uniprot']['type'] = 'trembl'
          for k in range(12):
            if row[pos] != None and row[pos] != '':
              val = process_field(REPEAT_SUBJECT_COLS[k], row[pos])
              if COLUMN_DATA[REPEAT_SUBJECT_COLS[k]]['uniprot_type'] == 'swissprot':
                set_field(info_1, REPEAT_SUBJECT_COLS[k], val)
              elif COLUMN_DATA[REPEAT_SUBJECT_COLS[k]]['uniprot_type'] == 'trembl':
                set_field(info_2, REPEAT_SUBJECT_COLS[k], val)
              else:
                set_field(info_1, REPEAT_SUBJECT_COLS[k], val)
                set_field(info_2, REPEAT_SUBJECT_COLS[k], val)
            pos += 1
          yield info_1
          yield info_2

def arrayify(obj: dict[str, any]):
  for col in COLUMN_DATA:
    if COLUMN_DATA[col]['has_duplicates'] != True:
      continue
    if COLUMN_DATA[col]['uniprot_type'] != 'all' and COLUMN_DATA[col]['uniprot_type'] != obj['subject']['uniprot']['type']:
      continue
    if get_field(obj, col) != None:
      f = get_field(obj, col)
      if isinstance(f, list):
        f = f.copy()
      set_field(obj, col, [f])
  
  obj['relation'] = [obj['relation']]
  return obj

def merge(main: dict[str, any], other: dict[str, any]):
  for col in COLUMN_DATA:
    if COLUMN_DATA[col]['has_duplicates'] != True:
      continue
    m_field = get_field(main, col)
    o_field = get_field(other, col)
    if m_field == None and o_field == None:
      continue
    if m_field == None:
      set_field(main, col, [o_field])
    if o_field == None:
      continue
    if o_field not in m_field:
      append_field(main, col, o_field)
  
  main['relation'].append(other['relation'])

def load_data(data_folder):
  docs = {}
  row_num = 0
  for row in read_csv(os.path.join(data_folder, './BindingDB_All.tsv'), '\t'):
    #print(row['subject']['uniprot'])
    try:
      entry_name = row['subject']['uniprot']['id']
      primary_id = row['subject']['uniprot']['accession']
    except KeyError:
      continue

    if entry_name == None or primary_id == None:
      continue

    if '_HUMAN' not in entry_name or primary_id == '':
      continue

    row['_id'] = f"{row['object']['monomer_id']}-{primary_id}"
    row['predicate'] = 'physically interacts with'

    if row['_id'] in docs:
      merge(docs[row['_id']], row)
    else:
      docs[row['_id']] = arrayify(row)

    # if row_num >= 15000:
    #   break
    row_num += 1

  for doc_id in docs:
    yield docs[doc_id]

def main():
  from time import time

  cnt = 0
  c = 0
  tim = time()

  for row in load_data('./'):
    if (row["_id"] == "6221-P24941"):
      print('Writing record to file')
      with open("record.json", "w") as r:
        json.dump(row, r, indent=2)

  print(cnt)
  print(c)
  print("Time to execute: ", time()-tim)

if __name__ == '__main__':
  main()


