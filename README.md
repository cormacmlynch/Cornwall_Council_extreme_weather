# Cornwall Council Extreme Weather Impact Assessment

Data processing and visualisation for the extreme weather impact assessment for Cornwall Council.

## Usage

Run from the project root directory:

```bash
python cc_main.py [--module {train_delays,nhs_111}] [--force-rebuild]
```

### Arguments

| Argument | Description |
|---|---|
| `--module train_delays` | Run only the Network Rail train delays module |
| `--module nhs_111` | Run only the NHS 111 IUC calls module |
| `--module rnli` | Run only the RNLI lifeboat launches module|
| *(omit `--module`)* | Run all modules |
| `--force-rebuild` | Re-process raw data even if cleaned files already exist |

### Examples

```bash
# Run all modules (uses cached processed data if available)
python cc_main.py

# Run only train delays, forcing a rebuild from raw files
python cc_main.py --module train_delays --force-rebuild

# Run only NHS 111
python cc_main.py --module nhs_111
```

## Outputs

All plots are saved to the `plots/` directory.

### Train delays (`train_delays`)

| File | Description |
|---|---|
| `plots/all_delays.png` | Stacked bar chart of monthly delay minutes (weather vs non-weather related) across the full dataset |
| `plots/delays_2026_1.png` | Daily delay minutes for January 2026, annotated with Storm Goretti, Storm Ingrid, and Storm Chandra |
| `plots/delays_2024_11.png` | Daily delay minutes for November 2024, annotated with Storm Bert |
| `plots/delays_2022_7.png` | Daily delay minutes for July 2022, annotated with the 2022 heatwave peak |

Processed data is cached at `network_rail/processed_data/cleaned_nr_data.csv`.

### NHS 111 IUC calls (`nhs_111`)

| File | Description |
|---|---|
| `plots/calls_2018_3.png` | Daily IUC calls for March 2018, annotated with the Beast from the East |
| `plots/calls_2022_7.png` | Daily IUC calls for July 2022, annotated with the 2022 heatwave peak |
| `plots/calls_2024_11.png` | Daily IUC calls for November 2024, annotated with Storm Bert |
| `plots/calls_2026_1.png` | Daily IUC calls for January 2026, annotated with Storm Goretti, Storm Ingrid, and Storm Chandra |

Processed data is cached at `nhs/processed_data/iuc/cleaned_iuc_data.csv`.

### RNLI Lifeboat launches (`rnli`)

| File | Description |
|---|---|
| `plots/rnli_callouts_2022_7.png` | Daily lifeboat launches for July 2022, annotated with the 2022 heatwave peak |
| `plots/rnli_callouts_2024_11.png` | Daily lifeboat launches for November 2024, annotated with Storm Bert |

Processed data is cached at `nhs/processed_data/iuc/cleaned_iuc_data.csv`.
