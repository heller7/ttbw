# TTBW Migration Script

This script processes table tennis tournament data from various sources and generates regional ranking reports. It handles player eligibility, tournament results, and QTTR ratings.

## Configuration

The script now uses a YAML configuration file (`config.yaml`) to manage all configurable parameters. This makes it easy to:

- Add new tournaments without modifying code
- Change age class mappings
- Modify district configurations
- Set custom output directories
- Configure API endpoints

## Usage

### Basic Usage
```bash
python ttbw_migration.py
```

### Custom Configuration File
```bash
python ttbw_migration.py my_config.yaml
```

## Configuration File Structure

### Default Birth Year
```yaml
default_birth_year: 2014
```
The birth year to use when a player's birth year is not available.

### Age Classes
```yaml
age_classes:
  2006: 19
  2007: 19
  2008: 19
  # ... more mappings
```
Maps birth years to age class numbers.

### Tournaments
```yaml
tournaments:
  Tournament_Name:
    tournament_id: 123456
    points: 1000
```
Each tournament has a unique ID and point value for ranking calculations.

### Districts
```yaml
districts:
  District_Name:
    region: 1
    short_name: "DN"
```
Each district belongs to a region and has a short name abbreviation.

### Output Configuration
```yaml
output:
  folder: "output"
  csv_delimiter: ";"
```
- `folder`: Directory where CSV reports will be saved
- `csv_delimiter`: Character used to separate CSV fields

### API Configuration
```yaml
api:
  nuliga_base_url: "https://ttbw.click-tt.de/cgi-bin/WebObjects/nuLigaTTDE.woa/wa/"
  # ... other API settings
```
API endpoints and federation settings (usually don't need to change).

## Adding New Tournaments

To add a new tournament:

1. Open `config.yaml`
2. Add a new entry under `tournaments`:
   ```yaml
   New_Tournament_Name:
     tournament_id: 123456
     points: 1000
   ```
3. Save the file and run the script

## Adding New Age Classes

To add new age classes:

1. Open `config.yaml`
2. Add new mappings under `age_classes`:
   ```yaml
   age_classes:
     2015: 9
     2016: 7
   ```

## Dependencies

Install required packages:
```bash
pip install -r requirements.txt
```

## Files

- `ttbw_migration.py` - Main script
- `config.yaml` - Configuration file
- `config_example.yaml` - Example configuration with comments
- `requirements.txt` - Python dependencies
- `README.md` - This documentation

## Output

The script generates CSV files in the configured output directory:
- `region1.csv` - Region 1 rankings
- `region2.csv` - Region 2 rankings
- etc.

Each CSV contains player rankings with tournament results and QTTR ratings.
