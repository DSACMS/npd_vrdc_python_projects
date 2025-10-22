# VRDC Entity Looper

A Python library for entity-level analysis across multiple medical claims benefit settings in the Virtual Research Data Center (VRDC) environment.

## Overview

The VRDC Entity Looper provides a structured approach to handling entity identifiers across different medical benefit settings and time periods. It simplifies the complex task of mapping entity fields, generating SQL queries, and iterating through time ranges for comprehensive entity analysis.

## Key Components

### 1. MonthRange Class
A simple container for time ranges using basic arithmetic operations (no complex date libraries required).

**Features:**
- Handles `start_year`, `start_month`, `end_year`, `end_month` parameters
- Provides iteration over months with proper year rollover (2023-12 → 2024-01)
- Simple arithmetic-based month calculations
- Validation for invalid month ranges

### 2. VRDCEntityMapper Class
Maps entity identifiers across 7 medical benefit settings with proper field mappings and SQL generation.

**Supported Benefit Settings:**
- `bcarrier` (Part B Carrier)
- `dme` (Durable Medical Equipment)
- `inpatient` (Inpatient claims)
- `outpatient` (Outpatient claims)
- `snf` (Skilled Nursing Facility)
- `hospice` (Hospice care)
- `hha` (Home Health Agency)

**Entity Levels Supported:**
- **Tax ID / EIN**: Tax identifiers (sometimes missing)
- **CCN**: Provider certification numbers (sometimes missing)
- **Organizational NPI**: National Provider Identifiers for organizations
- **Personal NPI**: National Provider Identifiers for individuals

## Database and Table Naming Conventions

The system automatically handles VRDC naming conventions:

- **Database Format**: `rif{year}` (e.g., `rif2025`, `rif2019`)
- **Claim Tables**: `{setting}_claims_{month:02d}` (e.g., `bcarrier_claims_05`)
- **Line Tables**: 
  - Non-institutional: `{setting}_line_{month:02d}` (bcarrier, dme)
  - Institutional: `{setting}_revenue_{month:02d}` (inpatient, outpatient, snf, hospice, hha)

## Key Features

- **Flexible Time Range Handling**: Simple month/year arithmetic with proper year boundary handling
- **Comprehensive Entity Mapping**: Complete field mappings per VRDC specifications with institutional format differences
- **SQL Generation**: Ready-to-use SQL SELECT statements with proper table prefixes and aliasing
- **Smart Field Handling**: Correctly excludes unavailable fields and handles special aliasing cases

## Installation and Usage

### Basic Usage

```python
from vrdc.entity_looper import MonthRange, VRDCEntityMapper

# Create a time range
month_range = MonthRange(start_year=2023, start_month=1, end_year=2023, end_month=6)

# Get available settings
settings = VRDCEntityMapper.get_all_settings()
print(settings)  # ['bcarrier', 'dme', 'inpatient', 'outpatient', 'snf', 'hospice', 'hha']

# Generate SQL for a specific setting
sql = VRDCEntityMapper.get_sql_select_list(setting='bcarrier')
print(sql)
```

### Time Range Iteration

```python
# Iterate through multiple months and settings
for result in VRDCEntityMapper.iterate_month_range(
    month_range=month_range, 
    settings=['bcarrier', 'dme']
):
    print(f"Processing {result['database']}.{result['claim_table']}")
    print(f"SQL: {result['sql']}")
```

### Entity Level Analysis

```python
# Get all settings that have Tax ID fields
tax_settings = VRDCEntityMapper.get_level_fields(level='tax_id')
print(tax_settings.keys())  # ['bcarrier', 'dme', 'inpatient', 'outpatient']

# Get detailed field breakdown for a setting
summary = VRDCEntityMapper.get_setting_summary(setting='inpatient')
print(summary)
```

### Real-World Example

```python
# Comprehensive entity analysis for Q4 2023
q4_2023 = MonthRange(start_year=2023, start_month=10, end_year=2023, end_month=12)

# Focus on settings with Tax IDs for organizational analysis
tax_settings = list(VRDCEntityMapper.get_level_fields(level='tax_id').keys())

# Get processing scope
summary = VRDCEntityMapper.get_month_range_summary(
    month_range=q4_2023, 
    settings=tax_settings
)
print(f"Total combinations to process: {summary['total_combinations']}")

# Process each combination
for result in VRDCEntityMapper.iterate_month_range(
    month_range=q4_2023, 
    settings=tax_settings
):
    # Each result contains:
    # - database: 'rif2023'
    # - claim_table: 'bcarrier_claims_10'
    # - line_table: 'bcarrier_line_10'
    # - sql: Ready-to-use SELECT statement
    # - fields: Complete field mappings
    
    process_entity_data(result)
```

## File Structure

```
vrdc/
├── entity_looper.py              # Main classes (MonthRange, VRDCEntityMapper)
├── excercise_entity_looper.py    # Usage demonstrations
└── old_code/                     # Legacy code

tests/
├── test_entity_looper.py         # Full pytest test suite
├── test_entity_looper_simple.py  # Standalone test runner (no pytest required)
└── __init__.py

docs/
└── VRDC_columns.md              # Detailed field specifications
```

## Testing

The project includes comprehensive tests to validate functionality:

### Run with pytest (if installed):
```bash
python -m pytest tests/test_entity_looper.py -v
```

### Run without pytest:
```bash
python tests/test_entity_looper_simple.py
```

### Run usage demonstrations:
```bash
cd vrdc
python excercise_entity_looper.py
```

## Institutional vs Non-Institutional Differences

The system correctly handles format differences:

| Feature | Non-Institutional (bcarrier, dme) | Institutional (inpatient, outpatient, snf, hospice, hha) |
|---------|-----------------------------------|-----------------------------------------------------------|
| Line Tables | `{setting}_line_XX` | `{setting}_revenue_XX` |
| Tax ID Field | Available in all | Not available in SNF, Hospice, HHA |
| ORDRG_PHYSN_NPI | N/A | Not available in Inpatient, SNF |
| RNDRNG_PHYSN_NPI | Standard naming | Special aliasing (claim_ vs cline_) |

## Use Cases

This library is designed for:

- **Entity Resolution**: Linking providers across different benefit settings
- **Coverage Analysis**: Understanding which entity identifiers are available where
- **Data Pipeline Development**: Building ETL processes for entity data
- **Research Analysis**: Supporting longitudinal studies across benefit settings
- **Quality Assessment**: Validating entity data completeness across time periods

---

For detailed field specifications and mappings, see [`docs/VRDC_columns.md`](docs/VRDC_columns.md).

For comprehensive usage examples, run the demonstration script:
```bash
cd vrdc && python excercise_entity_looper.py
