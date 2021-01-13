import sys
import DicomNiftiConverter as Dcm2NiiCvt
import pandas as pd
import pathlib
import multiprocessing as mp
import os
import pydicom.errors
sys.path.insert(0, '/data/jacky831006/DicomNifti-master/')

if __name__ == '__main__':
#    psrc_dirs = ['/Volumes/WD_BLACK/cgmh/data','/Volumes/WD_BLACK/cgmh/data']
#    parent_dirs = ['/Volumes/WD_BLACK/cgmh/data/NIFTI',
#                   '/Volumes/WD_BLACK/cgmh/data/NIFTI']
    #psrc_dirs = ['/data/home/Imagebank/CGMHTRCT',
    #             '/data/home/Imagebank/CGMHTRABDCT']
    #parent_dirs = ['/data/home/Imagebank/CGMHTRCT/NIFTI',
    #               '/data/home/Imagebank/CGMHTRABDCT/NIFTI']

    psrc_dirs = '/data/jacky831006/spleen_softlink'
    parent_dirs = '/data/jacky831006/spleen_softlink/NIFTI'           
#    lists = ['/Volumes/WD_BLACK/cgmh/data/CGMHTRCT_spleenlist.csv']
#    lists = ['~/NIFTI/CGMHTRCT_spleenlist.csv']
    #lists = ['/data/home/jacky831006/jupyter_data/spleen_only_new.xlsx']
    lists = ['/data/jacky831006/slices_output.csv']

    print('Reading lists...', end=' ')
    df_miss = pd.DataFrame(columns=["source"])
    df_urls = pd.DataFrame(columns = ["source", "target"])
    for list_url in lists:
        df_list = pd.read_csv(list_url, encoding='latin1')
        #df_list = pd.read_excel(list_url,sheet_name=0)
        df_list['spleen_injury'] = 1
        df = df_list.loc[:, ['softdir','spleen_injury']]
        df = df.drop_duplicates()
        #df = df_list.loc[:, ['newID', 'spleen_injury']]
        df.set_index("spleen_injury", inplace=True)
        possible_types = list(dict.fromkeys(df.index.values))
        df_types = []
        for type in possible_types:
            df_types.append(df.loc[type].reset_index())
            # print(df_types[-1].loc[:,:])
            for index, row in df_types[-1].iterrows():
                '''
                id = -1
                if row['newID'].startswith('CGMHTRABD'):
                    id = 1
                elif row['newID'].startswith('CGMHTR'):
                    id = 0
                if id < 0:
                    print(f"Warning: {row['newID']}, {row['spleen_injury']}")
                    continue
                '''
                tgt_url = f"{parent_dirs}/{type}/{row['softdir']}"
                src_url = f"{psrc_dirs}/{row['softdir']}"
                #tgt_url = f"{parent_dirs[id]}/spleen/{type}/{row['newID']}" \
                #          f"/SDY00000/"
                #src_url = f"{psrc_dirs[id]}/{row['newID']}/SDY00000/"
                src_dir = pathlib.Path(src_url)
                if src_dir.exists():
                    a_series = pd.Series([src_url, tgt_url], index=df_urls.columns)
                    df_urls = df_urls.append(a_series, ignore_index=True)
                else:
                    a_series = pd.Series([src_url], index=df_miss.columns)
                    df_miss = df_miss.append(a_series, ignore_index=True)
                # print(f"{a_series}")
                # break
    df_miss.to_csv(r'apollo_source_miss_spleen.csv', index=True, header=True)
    print(f'Done.')
    # print(df_urls)

    converted_list: dict = {}
    fail_list = {}
    ns = mp.Manager().Namespace()

    ns.df_success = pd.DataFrame(columns=["source", "series description"])

    tgt_list = []
    print(f'File lines: {len(df_urls)}')
    for index, row in df_urls.iterrows():
        tgt_dir_str = row['target']
        #print(tgt_dir_str)
        tgt_dir = pathlib.Path(tgt_dir_str)
        converted = False
        dir_existed = False
        if tgt_dir.exists() and tgt_dir.is_dir():
            dir_existed = True
            for entry in tgt_dir.iterdir():
                suffixes: list = entry.suffixes
                if entry.is_file() and len(suffixes) == 2 and suffixes[
                    0].lower() == '.nii' and suffixes[1].lower() == '.gz':
                    converted = True
                    break
        if not converted:
            converted_list.update({index: row['source']})
            tgt_list.append(tgt_dir_str)
            if not dir_existed:
                pathlib.Path(tgt_dir_str).mkdir(mode=0o775, parents=True,
                                                exist_ok=True)
            #print(row['target'])
            #print(row['source'])
        else:
            series_des = ''
            for _root, directories, files in os.walk(row['source']):
                acc = 0
                for file in files:
                    if not file.lower().endswith(".dcm"):
                        continue
                    full_file = os.path.join(_root, file)
                    try:
                        #print(full_file)
                        dicom_set = pydicom.dcmread(full_file, force=True)
                        acc += 1
                        series_des = str(dicom_set.SeriesDescription)
                        if acc >= 4:
                            break
                    except pydicom.errors.InvalidDicomError:
                        print(
                            f'{Dcm2NiiCvt.BColors.WARNING}Error: '
                            f'InvalidDicom - {full_file}'
                            f'{Dcm2NiiCvt.BColors.ENDColor}')
                    except AttributeError as e:
                        series_des = ''
                        if acc >= 4:
                            break
                if acc >= 4:
                    a_series = pd.Series([_root, series_des],
                                         index=ns.df_success.columns)
                    ns.df_success = ns.df_success.append(a_series,
                                                         ignore_index=True)
    ns.df_success.to_csv(r'apollo_convert_success_spleen.csv', index=True, header=True)

    '''
        dir_existed = False
        if tgt_dir.exists() and tgt_dir.is_dir():
            dir_existed = True
        converted_list.update({index: row['source']})
        tgt_list.append(tgt_dir_str)
        if not dir_existed:
            pathlib.Path(tgt_dir_str).mkdir(mode=0o777, parents=True,
                                            exist_ok=True)
    '''


    converted_list_final = {}
    tgt_list_final = []
    series_des_final = []
    fail_list_final = {}
    # print(f'Check duplicate: {len(converted_list)},
    # {len(list(converted_list.values()))}')

    Dcm2NiiCvt.NiiCvt.init(ns, mp.Lock(), mp.Value('i', 0))
    Dcm2NiiCvt.NiiCvt.find_DICOM_dirs(
        list(converted_list.values()), r'apollo_skip_convert_spleen.csv', tgt_list,
        converted_list_final, tgt_list_final, series_des_final, fail_list_final)
    print('Converted directory count: %d' % len(converted_list_final.values()))
    print('Failed list:', end=' ')
    print(list(fail_list_final.keys()), sep=',')
    '''
    print('Converted Final List:')
    for id, item in enumerate(converted_list_final.values()):
        print(f'{item}: {tgt_list_final[id]}')
    '''
    #print(series_des_final)
    #print(tgt_list_final)
    Dcm2NiiCvt.NiiCvt.pl_dcm2nii(converted_list_final, 'apollo_valid_error_spleen.csv',
                                 True, tgt_list_final, series_des_final)

