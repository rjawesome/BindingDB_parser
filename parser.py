import csv
import json
import os

REPEAT_SUBJECT_COLS = ['BindingDB Target Chain  Sequence', 'PDB ID(s) of Target Chain', 'UniProt (SwissProt) Recommended Name of Target Chain', 'UniProt (SwissProt) Entry Name of Target Chain', 'UniProt (SwissProt) Primary ID of Target Chain', 'UniProt (SwissProt) Secondary ID(s) of Target Chain', 'UniProt (SwissProt) Alternative ID(s) of Target Chain', 'UniProt (TrEMBL) Submitted Name of Target Chain', 'UniProt (TrEMBL) Entry Name of Target Chain', 'UniProt (TrEMBL) Primary ID of Target Chain', 'UniProt (TrEMBL) Secondary ID(s) of Target Chain', 'UniProt (TrEMBL) Alternative ID(s) of Target Chain']
BASE_COLS = ['BindingDB Reactant_set_id', 'Ligand SMILES', 'Ligand InChI', 'Ligand InChI Key', 'BindingDB MonomerID', 'BindingDB Ligand Name', 'Target Name Assigned by Curator or DataSource', 'Target Source Organism According to Curator or DataSource', 'Ki (nM)', 'IC50 (nM)', 'Kd (nM)', 'EC50 (nM)', 'kon (M-1-s-1)', 'koff (s-1)', 'pH', 'Temp (C)', 'Curation/DataSource', 'Article DOI', 'PMID', 'PubChem AID', 'Patent Number', 'Authors', 'Institution', 'Link to Ligand in BindingDB', 'Link to Target in BindingDB', 'Link to Ligand-Target Pair in BindingDB', 'Ligand HET ID in PDB', 'PDB ID(s) for Ligand-Target Complex', 'PubChem CID', 'PubChem SID', 'ChEBI ID of Ligand', 'ChEMBL ID of Ligand', 'DrugBank ID of Ligand', 'IUPHAR_GRAC ID of Ligand', 'KEGG ID of Ligand', 'ZINC ID of Ligand', 'Number of Protein Chains in Target (>1 implies a multichain complex)']
RELATION_COLS = {'Ki (nM)', 'IC50 (nM)', 'Kd (nM)', 'EC50 (nM)', 'kon (M-1-s-1)', 'koff (s-1)', 'pH', 'Temp (C)', 'Curation/DataSource', 'Article DOI', 'PMID', 'PubChem AID', 'Patent Number', 'Authors', 'Institution', 'Link to Ligand in BindingDB', 'Link to Ligand-Target Pair in BindingDB', 'Ligand HET ID in PDB', 'PDB ID(s) for Ligand-Target Complex'}
EXTRA_SUBJECT_COLS = {'Target Name Assigned by Curator or DataSource', 'Target Source Organism According to Curator or DataSource', 'Link to Target in BindingDB'}

INT_FIELDS = {"BindingDB Reactant_set_id", "BindingDB MonomerID",  "PubChem CID", "PubChem SID", "Number of Protein Chains in Target (>1 implies a multichain complex)"}
SPLIT_FIELDS = {"PDB ID(s) of Target Chain", "UniProt (SwissProt) Secondary ID(s) of Target Chain", "UniProt (SwissProt) Alternative ID(s) of Target Chain", "UniProt (TrEMBL) Secondary ID(s) of Target Chain", "UniProt (TrEMBL) Alternative ID(s) of Target Chain"}

def process_field(field_name: str, value: str):
  if field_name in INT_FIELDS:
    return int(value)
  if field_name in SPLIT_FIELDS:
    return value.split(',')
  
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
          if row[j] != None and row[j] != '':
            val = process_field(BASE_COLS[j], row[j])
            if BASE_COLS[j] in RELATION_COLS:
              base['relation'][BASE_COLS[j]] = val
            elif BASE_COLS[j] in EXTRA_SUBJECT_COLS:
              base['subject'][BASE_COLS[j]] = val
            else:
              base['object'][BASE_COLS[j]] = val
        
        repeats = int(row[36]) # Number of Protein Chains in Target
        pos = 37
        for j in range(repeats):
          info = base.copy()
          for k in range(12):
            if row[pos] != None and row[pos] != '':
              val = process_field(REPEAT_SUBJECT_COLS[k], row[pos])
              info['subject'][REPEAT_SUBJECT_COLS[k]] = val
            pos += 1
          yield info

def load_data(data_folder):
  for row in read_csv(os.path.join(data_folder, './BindingDB_All.tsv'), '\t'):
    try:
      entry_name = row['subject']['UniProt (SwissProt) Entry Name of Target Chain']
      primary_id = row['subject']['UniProt (SwissProt) Primary ID of Target Chain']
      # species = row['object']['Target Source Organism According to Curator or DataSource']
    except KeyError:
      continue

    if entry_name == None or primary_id == None:
      continue

    if '_HUMAN' not in entry_name or primary_id == '':
      continue

    yield row

def main():
  from time import time

  cnt = 0
  tim = time()

  for row in load_data('./'):
    if cnt == 2:
      with open("record.json", "w") as r:
        json.dump(row, r, indent=2)
      break

    cnt += 1
    if cnt % 100000 == 0:
      print(f"---{cnt}")

  print(cnt)
  print("Time to execute: ", time()-tim)

if __name__ == '__main__':
  main()


