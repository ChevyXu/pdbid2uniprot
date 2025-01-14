import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from six.moves.urllib.request import urlopen
from argparse import ArgumentParser
from functools import lru_cache

# Command-line argument parser
parser = ArgumentParser()
parser.add_argument('--input', type=str, required=True, help="Input file path")
parser.add_argument('--pdb_col', type=str, required=True, help="Column name for PDB IDs")
parser.add_argument('--chain_col', type=str, required=True, help="Column name for Chain IDs")
parser.add_argument('--no_header', action='store_true', help="Indicate if input has no header")
parser.add_argument('--threads', type=int, default=10, help="Number of threads for concurrency")
args = parser.parse_args()

# Input parameters
input_file = args.input
header = not args.no_header
pdb_col = args.pdb_col
chain_col = args.chain_col
num_threads = args.threads

# Load input data
if input_file.endswith('.csv'):
    input_df = pd.read_csv(input_file)
else:
    input_df = pd.read_csv(input_file, sep='\t', header=None if not header else 0)
    input_df.columns = [pdb_col, chain_col]

# Initialize result storage
results = []

# Cache function to avoid duplicate API requests
@lru_cache(maxsize=None)
def fetch_pdb_to_uniprot(pdb):
    """Fetch PDB to UniProt mapping using PDBe API."""
    try:
        response = urlopen(f'https://www.ebi.ac.uk/pdbe/api/mappings/uniprot/{pdb}')
        return json.loads(response.read().decode('utf-8'))
    except:
        return None

@lru_cache(maxsize=None)
def fetch_uniprot_to_gene(uniprot_id):
    """Fetch gene name using UniProt API."""
    try:
        response = urlopen(f'https://rest.uniprot.org/uniprotkb/{uniprot_id}.json')
        data = json.loads(response.read().decode('utf-8'))
        return data.get('genes', [{}])[0].get('geneName', {}).get('value', 'N/A')
    except:
        return 'N/A'

def process_row(row):
    """Process a single row to map PDB, chain to UniProt and gene name."""
    pdb = row[pdb_col]
    chain = row[chain_col]
    print(f"Processing {pdb} {chain}...")

    # Fetch PDB -> UniProt mapping
    pdb_data = fetch_pdb_to_uniprot(pdb.lower())
    if not pdb_data or pdb.lower() not in pdb_data:
        return {'pdb': pdb, 'chain': chain, 'uniprot': 'Not Found', 'gene_name': 'Not Found'}

    # Find UniProt ID for the given chain
    uniprot_ids = []
    for uniprot, details in pdb_data[pdb.lower()]['UniProt'].items():
        for mapping in details.get('mappings', []):
            if mapping['chain_id'] == chain:
                uniprot_ids.append(uniprot)

    if not uniprot_ids:
        return {'pdb': pdb, 'chain': chain, 'uniprot': 'Not Found', 'gene_name': 'Not Found'}

    # Fetch gene name for the first UniProt ID (if multiple IDs, take the first one)
    uniprot_id = uniprot_ids[0]
    gene_name = fetch_uniprot_to_gene(uniprot_id)
    return {'pdb': pdb, 'chain': chain, 'uniprot': uniprot_id, 'gene_name': gene_name}

# Run the mapping process concurrently
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    future_to_row = {executor.submit(process_row, row): row for _, row in input_df.iterrows()}
    for future in as_completed(future_to_row):
        results.append(future.result())

# Convert results to DataFrame
result_df = pd.DataFrame(results)

# Save to output file
output_file = input_file + '_gene_mapping.csv' if input_file.endswith('.csv') else input_file + '_gene_mapping.tsv'
sep = ',' if output_file.endswith('.csv') else '\t'
result_df.to_csv(output_file, sep=sep, index=False)
print(f"Results saved to {output_file}.")

