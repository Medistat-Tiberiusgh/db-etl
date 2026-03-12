# Data processing pipeline that filters Socialstyrelsen csv files and unifies total
# prescriptions, patient counts, and rates per 1,000 inhabitants into one unified table
# based on year, region, and demographics.
# All human drugs (7-character ATC codes, excluding veterinary Q-prefix) are included.
# Narcotic class is added where available via a left join with the narcotic mapping.
# Also generates lookup tables for drugs, regions, genders, and age groups.

# Requires "atc_narkotika_mapping.csv" that results from running "narcotics_extractor.py"

# https://www.socialstyrelsen.se/statistik-och-data/statistik/for-utvecklare/
# (CSV Statistikdatabasen – Läkemedel 2006–2024)

import pandas as pd
import os

# Filsökvägar
EXPEDIERINGAR = "läkemedel - data - antal expedieringar - 2006-2024.csv"
PATIENTER     = "läkemedel - data - antal patienter - 2006-2024.csv"
PER_1000      = "läkemedel - data - expedieringar_1000 invånare - 2006-2024.csv"
MAPPING       = "atc_narkotika_mapping.csv"
ATC_META      = "läkemedel - meta - ATC.csv"
REGIONS_META  = "läkemedel - meta - regioner.csv"
GENDERS_META  = "läkemedel - meta - kön.csv"
AGES_META     = "läkemedel - meta - åldrar.csv"

RESULTS_DIR = "results2"
os.makedirs(RESULTS_DIR, exist_ok=True)

OUTPUT_PRESCRIPTIONS = f"{RESULTS_DIR}/prescription_data.csv"
OUTPUT_DRUGS         = f"{RESULTS_DIR}/drugs.csv"
OUTPUT_REGIONS       = f"{RESULTS_DIR}/regions.csv"
OUTPUT_GENDERS       = f"{RESULTS_DIR}/genders.csv"
OUTPUT_AGE_GROUPS    = f"{RESULTS_DIR}/age_groups.csv"

KEYS = ['År', 'Region', 'ATC-kod', 'Kön', 'Ålder']

# --- Narkotikamappning ---
print("Laddar narkotikamappning...")
narcotic_map = pd.read_csv(MAPPING)
print(f"  {len(narcotic_map)} narkotika-ATC-koder laddade")

# --- Datafiler ---
def load_file(filepath, value_name):
    print(f"Laddar {filepath}...")
    chunks = []
    for i, chunk in enumerate(pd.read_csv(filepath, sep=';', encoding='utf-8-sig', chunksize=500_000)):
        filtered = chunk[
            (chunk['ATC-kod'].str.len() == 7) &
            (~chunk['ATC-kod'].str.startswith('Q'))
        ]
        chunks.append(filtered)
        if i % 10 == 0:
            print(f"  ...chunk {i}, {sum(len(c) for c in chunks):,} rader behållna")
    df = pd.concat(chunks, ignore_index=True)
    df = df.rename(columns={'Värde': value_name})
    print(f"  {len(df):,} rader totalt")
    return df[KEYS + [value_name]]

expedieringar = load_file(EXPEDIERINGAR, 'num_prescriptions')
patienter     = load_file(PATIENTER, 'num_patients')
per_1000      = load_file(PER_1000, 'per_1000')

# --- Lookup-tabeller ---
print("Genererar lookup-tabeller...")

atc_meta = pd.read_csv(ATC_META, sep=';', encoding='utf-8-sig')
atc_meta.columns = ['atc', 'name']
atc_meta = atc_meta[
    (atc_meta['atc'].str.len() == 7) &
    (~atc_meta['atc'].str.startswith('Q'))
]
drugs = atc_meta.merge(narcotic_map, on='atc', how='left')
drugs = drugs.dropna(subset=['name'])
drugs = drugs[drugs['name'].str.strip() != '']
drugs.to_csv(OUTPUT_DRUGS, index=False)
print(f"  {OUTPUT_DRUGS}: {len(drugs)} rader")

# --- Joina och spara prescriptions ---
print("Joinар...")
df = expedieringar.merge(patienter, on=KEYS).merge(per_1000, on=KEYS)
df = df.rename(columns={
    'År':       'year',
    'Region':   'region',
    'ATC-kod':  'atc',
    'Kön':      'gender',
    'Ålder':    'age_group',
})
valid_atc = set(drugs['atc'])
df = df[df['atc'].isin(valid_atc)]
print(f"Sparar {OUTPUT_PRESCRIPTIONS} med {len(df):,} rader...")
df.to_csv(OUTPUT_PRESCRIPTIONS, index=False)

regions = pd.read_csv(REGIONS_META, sep=';', encoding='utf-8-sig')
regions.columns = ['id', 'name']
regions.to_csv(OUTPUT_REGIONS, index=False)
print(f"  {OUTPUT_REGIONS}: {len(regions)} rader")

genders = pd.read_csv(GENDERS_META, sep=';', encoding='utf-8-sig')
genders.columns = ['id', 'name']
genders.to_csv(OUTPUT_GENDERS, index=False)
print(f"  {OUTPUT_GENDERS}: {len(genders)} rader")

age_groups = pd.read_csv(AGES_META, sep=';', encoding='utf-8-sig')
age_groups.columns = ['id', 'name']
age_groups.to_csv(OUTPUT_AGE_GROUPS, index=False)
print(f"  {OUTPUT_AGE_GROUPS}: {len(age_groups)} rader")

print("\nKlart! Genererade filer:")
for f in [OUTPUT_PRESCRIPTIONS, OUTPUT_DRUGS, OUTPUT_REGIONS, OUTPUT_GENDERS, OUTPUT_AGE_GROUPS]:
    print(f"  {f}")