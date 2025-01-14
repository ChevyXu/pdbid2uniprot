> This script serves for convert PDBID like "5hxb_A" to uniprot ID and Gene Name.

## Features:
#### Concurrent API Requests:
The script uses ThreadPoolExecutor to process rows concurrently, significantly speeding up execution when handling thousands of rows.

#### Caching:
Both PDB-to-UniProt and UniProt-to-Gene mappings are cached using lru_cache, reducing redundant API requests.

#### Graceful Error Handling:
Handles cases where PDB or chain IDs are not found, logging them as Not Found.

#### Thread Control:
The number of threads can be adjusted via the --threads argument to optimize performance based on system resources.

## Usage Examples:
### Standard CSV Input:
```bash
python pdb_to_gene_mapping.py --input pdb_chain_table.csv --pdb_col PDB_ID --chain_col CHAIN_ID --threads 20
```
### Tab-Delimited Input:
```bash
python pdb_to_gene_mapping.py --input pdb_chain_table --pdb_col 0 --chain_col 1 --no_header --threads 15
```
