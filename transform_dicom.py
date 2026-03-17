import os
import re
import pandas as pd


def parse_dicom_dump(file_path):
    metadata = {}
    # Zoznam tagov pre všetky modality
    patterns = {
        # Spoločné
        'series_id': r'\(0020,000e\).*?\[(.*?)\]',  # Series Instance UID
        'modality': r'\(0008,0060\).*?\[(.*?)\]',  # Modality
        'series_date': r'\(0008,0021\).*?\[(.*?)\]',  # Series Date
        'body_region': r'\(0018,0015\).*?\[(.*?)\]',  # Body Part Examined
        'laterality': r'\(0020,0060\).*?\[(.*?)\]',  # Laterality
        'device': r'\(0008,1090\).*?\[(.*?)\]',  # Manufacturer's Model Name
        'manufacturer': r'\(0008,0070\).*?\[(.*?)\]',  # Manufacturer
        'sw_version': r'\(0018,1020\).*?\[(.*?)\]',  # Software Versions
        'color_space': r'\(0028,0004\).*?\[(.*?)\]',  # Photometric Interpretation
        'pixel_spacing': r'\(0028,0030\).*?\[(.*?)\]',  # Pixel Spacing
        'image_type': r'\(0008,0008\).*?\[(.*?)\]',  # Image Type
        'width': r'\(0028,0011\).*?\[(.*?)\]',  # Columns
        'height': r'\(0028,0010\).*?\[(.*?)\]',  # Rows
        'depth': r'\(0028,0100\).*?\[(.*?)\]',  # Bits Allocated
        'channels': r'\(0028,0002\).*?\[(.*?)\]',  # Samples per Pixel
        'channel_res': r'\(0028,0101\).*?\[(.*?)\]',  # Bits Stored
        'compression': r'\(0002,0010\).*?\[(.*?)\]',  # Transfer Syntax UID
        'annotations': r'\(0008,103e\).*?\[(.*?)\]',  # Series Description

        # Štúdia level tagy
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

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            for key, pattern in patterns.items():
                match = re.search(pattern, content)
                metadata[key] = match.group(1).strip() if match else "N/A"
        metadata['file_size'] = os.path.getsize(file_path)
    except:
        pass
    return metadata


def process_all_imaging(root_path):
    study_level = []
    series_level = []

    for accession in os.listdir(root_path):
        folder = os.path.join(root_path, accession)
        if not os.path.isdir(folder): continue

        dumps = [f for f in os.listdir(folder) if f.endswith('.dump')]
        if not dumps: continue

        series_buckets = {}
        study_info = {'modalities': set(), 'regions': set(), 'procs': set(), 'reasons': set(), 'uids': set(),
                      'insts': set(), 'date': ""}

        for d in dumps:
            data = parse_dicom_dump(os.path.join(folder, d))
            # Plnenie štúdie
            if data['modality'] != "N/A": study_info['modalities'].add(data['modality'])
            if data['body_region'] != "N/A": study_info['regions'].add(data['body_region'])
            if data['imaging_procedure'] != "N/A": study_info['procs'].add(data['imaging_procedure'])
            if data['reason'] != "N/A": study_info['reasons'].add(data['reason'])
            if data['series_id'] != "N/A": study_info['uids'].add(data['series_id'])
            if data['institution'] != "N/A": study_info['insts'].add(data['institution'])
            if not study_info['date'] and data['study_date'] != "N/A": study_info['date'] = data['study_date']

            # Zoskupenie do sérií
            sid = data['series_id']
            if sid not in series_buckets: series_buckets[sid] = []
            series_buckets[sid].append(data)

        # 1. PRÍPRAVA IMAGING STUDY
        sd = study_info['date']
        study_level.append({
            'accession number': accession.replace('#', ''),
            'modalities': ", ".join(sorted(study_info['modalities'])),
            'body region': ", ".join(sorted(study_info['regions'])),
            'imaging procedure': ", ".join(sorted(study_info['procs'])),
            'reason for imaging procedure': ", ".join(sorted(study_info['reasons'])),
            'study date': f"{sd[6:8]}.{sd[4:6]}.{sd[0:4]}" if len(sd) == 8 else "N/A",
            'dicom series count': len(study_info['uids']),
            'dicom images count': len(dumps),
            'affiliated institution': ", ".join(sorted(study_info['insts']))
        })

        # 2. PRÍPRAVA SÉRII
        for sid, files in series_buckets.items():
            f = files[0]
            common = {
                'ID serie': sid, 'accession number': accession.replace('#', ''),
                'modality': f['modality'], 'DICOM images count': len(files),
                'Series date': f['series_date'], 'Body region': f['body_region'],
                'Laterality': f['laterality'], 'Imaging device': f['device'],
                'Manufacturer': f['manufacturer'], 'Software version': f['sw_version'],
                'Color space': f['color_space'], 'Pixel spacing': f['pixel_spacing'],
                'Image type': f['image_type'], 'File format': 'DICOM',
                'File size': f['file_size'], 'Image width': f['width'],
                'Image height': f['height'], 'Image depth': f['depth'],
                'Number of channels': f['channels'], 'Channel resolution': f['channel_res'],
                'Compression method': f['compression'], 'Annotations available': f['annotations']
            }

            # Pridanie špecifických podľa modality
            if f['modality'] == 'CT':
                common.update({'Tube voltage (kVp)': f['kvp'], 'Tube current (mA)': f['ma'],
                               'Exposure time (ms)': f['exposure_time'], 'Spiral pitch factor': f['pitch'],
                               'Filter type': f['filter'], 'Convolution kernel': f['kernel'], 'Field of view': f['fov'],
                               'Slice thickness': f['thickness'], 'Imaging injection': f['contrast'],
                               'Z-axis spacing': f['z_spacing']})
            elif f['modality'] == 'MR':
                common.update({'Sequence name': f['seq_name'], 'Magnetic field strength': f['mag_field'],
                               'MR acquisition type': f['acq_type'], 'Repetition time': f['tr'], 'Echo time': f['te'],
                               'Imaging frequency': f['freq'], 'Flip angle': f['flip'], 'Inversion time': f['ti'],
                               'Receive coil name': f['coil'], 'Field of view': f['fov'],
                               'Slice thickness': f['thickness'], 'Imaging injection': f['contrast']})
            elif f['modality'] == 'DX':
                common.update({'Patient orientation': f['orientation'], 'Tube voltage (kVp)': f['kvp'],
                               'Exposure (mAs)': f['mas'], 'Exposure Time (ms)': f['exposure_time']})

            series_level.append(common)

    # EXPORTY
    pd.DataFrame(study_level).to_excel('ImagingStudy.xlsx', index=False)
    df_s = pd.DataFrame(series_level)
    for m in df_s['modality'].unique():
        df_s[df_s['modality'] == m].dropna(axis=1, how='all').to_excel(f'Series_{m}.xlsx', index=False)


# Spustenie
process_all_imaging('./data')