import os
import re
import sys
import json
import urllib.request
import pandas as pd


def check_environment():
    print("--- Kontrola prostredia ---")

    # 1. Kontrola knižníc
    try:
        import pandas as pd
        print("[OK] Knižnica pandas je dostupná.")
    except ImportError:
        print("[ERROR] Chýba knižnica 'pandas'. Nainštalujte ju: pip install pandas")
        sys.exit(1)

    # 2. Test zápisu v aktuálnom priečinku
    test_file = 'write_test.tmp'
    try:
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        print("[OK] Právo na zápis overené.")
    except Exception as e:
        print(f"[ERROR] Nemáte právo na zápis v tomto priečinku: {e}")
        sys.exit(1)

    print("---------------------------\n")


def get_patient_id(accession_number):
    """
    Zistí ID pacienta pomocou HTTP požiadavky.
    V lokálnom prostredí (kde server nie je dostupný) zlyhanie ošetrí a vráti prázdny reťazec.
    """
    url = f"http://172.16.55.182:8080/api/accession_numbers/{accession_number}/patient"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'PythonScript'})
        with urllib.request.urlopen(req, timeout=2) as response:
            if response.status == 200:
                data = json.loads(response.read().decode('utf-8'))
                patient_id = data.get('ID')
                if patient_id is not None:
                    return str(patient_id)
    except Exception:
        pass
    return ""


def parse_dicom_dump(file_path):
    metadata = {}
    # Zoznam tagov pre všetky modality
    patterns = {
        # Spoločné
        'series_id': r'\(0020,000e\).*?\[(.*?)\]',  # Series Instance UID
        'series_date': r'\(0008,0021\).*?\[(.*?)\]',  # Series Date
        'laterality': r'\(0020,0060\).*?\[(.*?)\]',  # Laterality
        'device': r'\(0008,1090\).*?\[(.*?)\]',  # Manufacturer's Model Name
        'manufacturer': r'\(0008,0070\).*?\[(.*?)\]',  # Manufacturer
        'sw_version': r'\(0018,1020\).*?\[(.*?)\]',  # Software Versions
        'color_space': r'\(0028,0004\).*?\[(.*?)\]',  # Photometric Interpretation
        'pixel_spacing': r'\(0028,0030\).*?\[(.*?)\]',  # Pixel Spacing
        'image_type': r'\(0008,0008\).*?\[(.*?)\]',  # Image Type
        'width': r'\(0028,0011\)(?:\s+\w+\s+)([^#\s\r\n]+)',  # Columns
        'height': r'\(0028,0010\)(?:\s+\w+\s+)([^#\s\r\n]+)',  # Rows
        'depth': r'\(0028,0100\)(?:\s+\w+\s+)([^#\s\r\n]+)',  # Bits Allocated
        'channels': r'\(0028,0002\)(?:\s+\w+\s+)([^#\s\r\n]+)',  # Samples per Pixel
        'channel_res': r'\(0028,0101\)(?:\s+\w+\s+)([^#\s\r\n]+)',  # Bits Stored
        'compression': r'\(0002,0010\).*?=\s*([^#\s\r\n]+)',  # Transfer Syntax UID (upravené pre správne vytiahnutie hodnoty)
        'annotations': r'\(0008,103e\).*?\[(.*?)\]',  # Series Description

        # Štúdia level tagy
        'modality': r'\(0008,0060\).*?\[(.*?)\]',  # Modality
        'body_region': r'\(0018,0015\).*?\[(.*?)\]',  # Body Part Examined
        'imaging_procedure': r'\(0008,1030\).*?\[(.*?)\]',  # Study Description
        'reason': r'\(0040,1002\).*?\[(.*?)\]',  # Reason for the Requested Procedure
        'study_date': r'\(0008,0020\).*?\[(.*?)\]',  # Study Date
        'institution': r'\(0008,0080\).*?\[(.*?)\]',  # Institution Name

        # CT & DX špecifické
        'kvp': r'\(0018,0060\).*?\[(.*?)\]',  # KVP
        'ma': r'\(0018,1151\).*?\[(.*?)\]',  # X-Ray Tube Current
        'exposure_time': r'\(0018,1150\).*?\[(.*?)\]',  # Exposure Time
        'pitch': r'\(0018,0093\).*?\[(.*?)\]',  # Spiral Pitch Factor
        'filter': r'\(0018,1160\).*?\[(.*?)\]',  # Filter Type
        'kernel': r'\(0018,1210\).*?\[(.*?)\]',  # Convolution Kernel
        'fov': r'\(0018,1100\).*?\[(.*?)\]',  # Reconstruction Diameter
        'thickness': r'\(0018,0050\).*?\[(.*?)\]',  # Slice Thickness
        'contrast': r'\(0018,0010\).*?\[(.*?)\]',  # Contrast/Bolus Agent
        'z_spacing': r'\(0018,0088\).*?\[(.*?)\]',  # Spacing Between Slices
        'mas': r'\(0018,1152\).*?\[(.*?)\]',  # Exposure
        'orientation': r'\(0020,0020\).*?\[(.*?)\]',  # Patient Orientation

        # MR špecifické
        'seq_name': r'\(0018,0024\).*?\[(.*?)\]',  # Sequence Name
        'mag_field': r'\(0018,0087\).*?\[(.*?)\]',  # Magnetic Field Strength
        'acq_type': r'\(0018,0023\).*?\[(.*?)\]',  # MR Acquisition Type
        'tr': r'\(0018,0080\).*?\[(.*?)\]',  # Repetition Time
        'te': r'\(0018,0081\).*?\[(.*?)\]',  # Echo Time
        'freq': r'\(0018,0084\).*?\[(.*?)\]',  # Imaging Frequency
        'flip': r'\(0018,1314\).*?\[(.*?)\]',  # Flip Angle
        'ti': r'\(0018,0082\).*?\[(.*?)\]',  # Inversion Time
        'coil': r'\(0018,1250\).*?\[(.*?)\]'  # Receive Coil Name
    }

    metadata = {key: "N/A" for key in patterns.keys()}
    metadata['file_size'] = 0

    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                for key, pattern in patterns.items():
                    match = re.search(pattern, content)
                    metadata[key] = match.group(1).strip() if match else "N/A"
            metadata['file_size'] = os.path.getsize(file_path)
    except Exception as e:
        print(f"Chyba pri spracovaní {file_path}: {e}")

    return metadata


def process_all_imaging(root_path):
    check_environment()

    all_items = [f for f in os.listdir(root_path) if os.path.isdir(os.path.join(root_path, f))]
    total_folders = len(all_items)

    print(f"Nájdených {total_folders} zložiek na spracovanie.\n")

    for index, accession in enumerate(all_items, 1):
        folder = os.path.join(root_path, accession)
        print(f"[{index}/{total_folders}] Analyzujem: {accession}")

        clean_accession = accession.replace('#', '')
        patient_id = get_patient_id(clean_accession)

        dumps = [f for f in os.listdir(folder) if f.endswith('.dump')]
        if not dumps: continue

        series_buckets = {}
        study_info = {'procs': set(), 'reasons': set(), 'uids': set(),
                      'insts': set(), 'date': ""}

        for d in dumps:
            data = parse_dicom_dump(os.path.join(folder, d))
            if data['imaging_procedure'] != "N/A": study_info['procs'].add(data['imaging_procedure'])
            if data['reason'] != "N/A": study_info['reasons'].add(data['reason'])
            if data['series_id'] != "N/A": study_info['uids'].add(data['series_id'])
            if data['institution'] != "N/A": study_info['insts'].add(data['institution'])
            if not study_info['date'] and data['study_date'] != "N/A": study_info['date'] = data['study_date']

            sid = data['series_id']
            if sid not in series_buckets: series_buckets[sid] = []
            series_buckets[sid].append(data)

        # PRIEBEŽNÁ PRÍPRAVA A ZÁPIS IMAGING STUDY
        sd = study_info['date']
        study_row = {
            'imaging study identifier': clean_accession,
            'belongs to person': patient_id,
            'study date': f"{sd[6:8]}.{sd[4:6]}.{sd[0:4]}" if len(sd) == 8 else "N/A",
            'imaging procedure': ", ".join(sorted(study_info['procs'])),
            'reason for imaging procedure': ", ".join(sorted(study_info['reasons'])),
            'dicom series count': len(study_info['uids']),
            'dicom images count': len(dumps),
            'affiliated institution': ", ".join(sorted(study_info['insts']))
        }

        pd.DataFrame([study_row]).to_csv(
            'ImagingStudy.csv',
            mode='a',
            index=False,
            sep=';',
            header=not os.path.exists('ImagingStudy.csv'),
            encoding='utf-8-sig'
        )

        # PRIEBEŽNÁ PRÍPRAVA A ZÁPIS SÉRII (upravené poradie a názvy stĺpcov)
        for sid, files in series_buckets.items():
            f = files[0]
            common = {
                'ID serie': sid,
                'belongs to imaging study': clean_accession,
                'DICOM images count': len(files),
                'imaging modality': f['modality'],
                'series date': f['series_date'],
                'body region': f['body_region'],
                'laterality': f['laterality'],
                'imaging device': f['device'],
                'manufacturer': f['manufacturer'],
                'software version': f['sw_version'],
                'color space': f['color_space'],
                'pixel spacing': f['pixel_spacing'],
                'image type': f['image_type'],
                'file format': 'DICOM',
                'file size': f['file_size'],
                'image width': f['width'],
                'image height': f['height'],
                'image depth': f['depth'],
                'number of channels': f['channels'],
                'channel resolution': f['channel_res'],
                'compression method': f['compression'],
                'annotations available': f['annotations']
            }

            if f['modality'] == 'CT':
                common.update({
                    'Tube voltage (kVp)': f['kvp'],
                    'Tube current (mA)': f['ma'],
                    'Exposure (mAs)': f['mas'],
                    'Exposure time (ms)': f['exposure_time'],
                    'Spiral pitch factor': f['pitch'],
                    'Filter type': f['filter'],
                    'Convolution kernel': f['kernel'],
                    'Field of view': f['fov'],
                    'Slice thickness': f['thickness'],
                    'Imaging injection': f['contrast'],
                    'Z-axis spacing': f['z_spacing'],
                    'Patient orientation': f['orientation']
                })
            elif f['modality'] == 'MR':
                common.update({
                    'Sequence name': f['seq_name'],
                    'Magnetic field strength': f['mag_field'],
                    'MR acquisition type': f['acq_type'],
                    'Repetition time': f['tr'],
                    'Echo time': f['te'],
                    'Imaging frequency': f['freq'],
                    'Flip angle': f['flip'],
                    'Inversion time': f['ti'],
                    'Receive coil name': f['coil'],
                    'Field of view': f['fov'],
                    'Slice thickness': f['thickness'],
                    'Imaging injection': f['contrast'],
                    'Patient orientation': f['orientation'],
                    'Pixel spacing': f['pixel_spacing'],
                    'Z-axis spacing': f['z_spacing']
                })
            elif f['modality'] == 'DX' or f['modality'] == 'CR':
                common.update({
                    'Patient orientation': f['orientation'],
                    'Tube voltage (kVp)': f['kvp'],
                    'Exposure (mAs)': f['mas'],
                    'Exposure Time (ms)': f['exposure_time']
                })

            safe_m = str(f['modality']).replace('/', '_').replace('\\', '_')
            filename = f'Series_{safe_m}.csv'
            pd.DataFrame([common]).to_csv(
                filename,
                mode='a',
                index=False,
                sep=';',
                header=not os.path.exists(filename),
                encoding='utf-8-sig'
            )

    print("\nAnalýza dokončená. Všetky dáta boli priebežne uložené do CSV.")


# --- SPUSTENIE SKRIPTU ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("\n[CHYBA] Nezadaná cesta k dátam!")
        print(f"Použitie: python {os.path.basename(__file__)} \"C:\\cesta\\k\\datam\"")
        sys.exit(1)

    input_path = sys.argv[1]
    process_all_imaging(input_path)