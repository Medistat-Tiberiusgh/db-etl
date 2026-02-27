# Script used for mapping ATC code to its narcotic class from Läkemedelsverket (Nationellt produktregister för läkemedel, NPL)
# https://www.lakemedelsverket.se/sv/e-tjanster-och-hjalpmedel/substans-och-produktregister/npl#hmainbody1

import os
import xml.etree.ElementTree as ET
import csv

PRODUCTDATA = "Productdata"
NS = {
    'npl': 'urn:schemas-npl:instance:12',
    'mpa': 'urn:schemas-npl:mpa:12'
}

NARCOTIC_LABELS = {
    '1': 'II', '3': 'III', '4': 'IV', '5': 'V'
}

seen = {}
count = 0

for filename in os.listdir(PRODUCTDATA):
    if not filename.endswith('.xml'):
        continue

    count += 1
    if count % 500 == 0:
        print(f"Bearbetar fil {count}...")

    try:
        tree = ET.parse(os.path.join(PRODUCTDATA, filename))
        root = tree.getroot()

        atc = root.find('.//mpa:atc-code-lx', NS)
        narcotic = root.find('.//mpa:narcotic-class-lx', NS)

        if atc is not None and narcotic is not None:
            atc_code = atc.get('v')
            narcotic_val = narcotic.get('v')

            if (
                narcotic_val in NARCOTIC_LABELS
                and atc_code
                and not atc_code.startswith('Q')
                and len(atc_code) == 7
            ):
                seen[atc_code] = NARCOTIC_LABELS[narcotic_val]

    except ET.ParseError:
        print(f"Kunde inte läsa: {filename}")

with open('atc_narkotika_mapping.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['atc', 'narcotic_class'])
    writer.writeheader()
    for atc_code, cls in sorted(seen.items()):
        writer.writerow({'atc': atc_code, 'narcotic_class': cls})

print(f"Klart! {len(seen)} unika narkotikaklassade ATC-koder (nivå 5, humanläkemedel) sparade.")