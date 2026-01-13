# Portals ETL Mapper

Tool to generate automated documentation and compare data from ETL projects (consultor-turbo and portal-turbo).

## Architecture

This project follows a simplified Domain-Driven Design (DDD) architecture:

- **shared/**: Generic shared code (utils, base models)
- **domains/**: Isolated domains by ETL project
  - `consultor_turbo/`: Consultor Turbo domain
  - `portal_turbo/`: Portal Turbo domain
- **infrastructure/**: External resources (Confluence, etc)
- **scripts/**: Entry scripts (CLI)
- **config/**: Configurations

## Structure

```
portals_etl_mapper/
├── shared/              # Shared code
├── domains/             # Isolated domains
│   ├── consultor_turbo/
│   └── portal_turbo/
├── infrastructure/      # External infrastructure
├── scripts/            # CLI scripts
├── config/             # Configurations
└── output/             # Generated outputs
```

## Installation

```bash
cd /home/burning_pixel_cwb/workstation/laborit/portals_etl_mapper
pip install -r requirements.txt
```

## Usage

### Generate documentation for a specific project

```bash
python scripts/generate_docs.py --project consultor_turbo
# or
python scripts/generate_docs.py --project portal_turbo
```

### Generate documentation for all projects

```bash
python scripts/generate_docs.py --all
```

### Compare S3 vs ETL

```bash
# For a specific project
python scripts/compare_s3_etl.py --project consultor_turbo
# or
python scripts/compare_s3_etl.py --project portal_turbo

# For all projects
python scripts/compare_s3_etl.py --all
```

### Unify consolidated files

```bash
python scripts/unify_consolidated.py
```

### Run complete pipeline (documentation + S3 comparison)

```bash
python scripts/run_pipeline.py
```

## Output Structure

Generated files are saved in:
- `output/consultor_turbo/` - Consultor Turbo documentation
- `output/portal_turbo/` - Portal Turbo documentation

Each directory contains:
- `MAPAS_CATALOG.md` - Maps catalog
- `FIELD_MAPPINGS_TABLE.md` - Field mapping table
- `CONFIGURATION_SUMMARY.json` - JSON summary
- `MAPAS_CATALOG.csv` - Catalog in CSV
- `csv_*/` - Individual CSVs per configuration

### S3 vs ETL Comparison Results

Comparison results are saved in:
- `output/{project}/resultados_comparacao/` - Results per project
  - `*_comparado.csv` - Individual comparison CSVs
  - `RESUMO_COMPARACAO.csv` - Comparison summary
  - `COMPARACAO_CONSOLIDADA.csv` - Project consolidated CSV
  - `ARQUIVOS_ETL_SEM_S3.csv` - Missing files report

And also:
- `output/COMPARACAO_CONSOLIDADA/COMPARACAO_CONSOLIDADA.csv` - Unified file from all projects

## Architecture

Each domain (`consultor_turbo/`, `portal_turbo/`) is completely isolated:
- Has its own services
- Has its own controller
- Can evolve independently

Shared code is in `shared/` and contains only generic utilities.

## Configuration

### S3 Paths

To use S3 vs ETL comparison, you need to:

1. **Place S3 CSV files in folders within each domain:**
   - `domains/consultor_turbo/s3_input/` - Consultor Turbo S3 files
   - `domains/portal_turbo/s3_input/` - Portal Turbo S3 files

   **Full path:**
   ```
   /home/burning_pixel_cwb/workstation/laborit/portals_etl_mapper/domains/consultor_turbo/s3_input/
   /home/burning_pixel_cwb/workstation/laborit/portals_etl_mapper/domains/portal_turbo/s3_input/
   ```

2. **Or edit `config/projects.yaml`** to point to other paths if preferred.

### Step by step:

1. **Generate documentation first** (to create ETL CSVs):
   ```bash
   python scripts/generate_docs.py --all
   ```

2. **Download S3 CSV files** and place them in the corresponding `s3_input/` folders

3. **Run the comparison:**
   ```bash
   python scripts/compare_s3_etl.py --all
   ```

ETL CSVs are automatically generated when you run `generate_docs.py` and are located in:
- `output/consultor_turbo/csv_consultor_tables/`
- `output/portal_turbo/csv_portal_tables/`

# laborit_etl_mapper
# laborit_etl_mapper
