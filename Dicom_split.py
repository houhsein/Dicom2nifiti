import os
import os.path
import shutil, glob
import re
import pandas as pd
import random
import pydicom as dicom
from pydicom.sequence import Sequence
from pydicom.dataset import Dataset
from string import digits
import numpy as np
from datetime import datetime

def walklevel(some_dir, level=3):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]


# Data dic location

path = "/data/jacky831006/spleen/"

ds=pd.DataFrame(columns=['OriPath','OriID','AccessNum','examdate','examtime','seriesdes','studydes','protocol','newID','SeriesInstanceUID','InstanceNum'])
#newID = "test"
colNum=0
folderDic = dict()
checkList = list()

# Read OriID to newID list
df_source = pd.read_csv('/data/jacky831006/dicom2nifti/1207_spleen.csv')

start = datetime.utcnow().strftime("%Y%m%d%H%M%S")
print(f"Start time:{start}")
print("=========== File list generation ===========", flush=True)
for root, dirs, files in walklevel(path):
    # Check the folder number and transform to SDY00000 so on
    #print(df_source.CT_personID.values)
    # Avoid the Outermost dirs list
    if dirs and not set(dirs).intersection(set(df_source.CT_personID.values)):
        imgdir=root.split("/")[-1]
        print(f"{imgdir}/{dirs} is progressing!", flush=True)
        for i in range(len(dirs)):
            num = "%05d" %i
            folderDic[dirs[i]] = f"SDY{num}"
    #print(folderDic)
    for names in files:
        oprfile=root+"/"+names
        #print(oprfile)
        #imgdir=root.split("/")[-2]
        #folder=root.split("/")[-1]
        #imgdir=str(imgdir)
        dicomset = dicom.dcmread(oprfile)
        
        colNum=colNum+1
        OriID=dicomset.PatientID
        #imgpath=imgdir+'/'+personID+'/'+folder+'/'
        
        AccessNum=dicomset.AccessionNumber
        InstanceNum=dicomset.InstanceNumber
        SeriesInstanceUID=dicomset.SeriesInstanceUID
        
        try:
            examdate=dicomset.SeriesDate     
        except (AttributeError):
            #print('except')
            examdate=""
        try:
            examtime=dicomset.SeriesTime
        except (AttributeError):
            examtime="" 
        try:
            seriesdes=dicomset.SeriesDescription
            '''
            # EX: Body 5.0 Venous./Coronal.3 CE  Coronal 
            # Remove the number after "/"
            if "/" in seriesdes:
                end = seriesdes.split("/")[1]
                remove_digits = str.maketrans('', '', digits)
                transEnd = end.translate(remove_digits)
                seriesdes = seriesdes.split("/")[0] +" "+transEnd
            '''   
        except (AttributeError):
            seriesdes=""
        try:
            studydes=dicomset.StudyDescription
        except (AttributeError):
            studydes=""
        try:
            protocol=dicomset.ProtocolName
        except (AttributeError):
            protocol=""
        newID = df_source[df_source['CT_personID'] == OriID].personID.values[0]
        ds.loc[colNum]=[oprfile,OriID,AccessNum,examdate,examtime,seriesdes,studydes,protocol,newID,SeriesInstanceUID,InstanceNum]
        #name_StudyDescription.append(studydes)
        #name_SeriesDescription.append(seriesdes)
        #Date_num.append(examdate)
    # Sort the ds by SeriesInstanceUID
keyList = ds['SeriesInstanceUID'].unique()
    

df_all = None
folderPath_check = list()
CTfolderNum = 0
df_sel = None
#print(keyList)
# softPath: spleen_softlink/3612837/SDY00001/SRS00281/IMG000.dcm
for i in (keyList):
    # Check CT floder oder
    if df_sel is not None:
        df_check = ds[ds['SeriesInstanceUID']== i].sort_values(["InstanceNum"])
        df_check_str = f"{df_check.OriID.values[0]}/{df_check.AccessNum.values[0]}"
        df_sel_str = f"{df_sel.OriID.values[0]}/{df_sel.AccessNum.values[0]}"
        if df_check_str != df_sel_str:
            CTfolderNum = 0
    df_sel = ds[ds['SeriesInstanceUID']== i].sort_values(["InstanceNum"])

    # CTfloder
    num = "%05d" %CTfolderNum
    CTfloder = f"SRS{num}"
    CTfolderNum += 1
    
    # Add softPath to df_sel 
    #print(df_sel.iloc[0]['AccessNum'])
    #print(CTfolderNum)

    # Sometimes file name is not same as AccessNum
    try:
        folderPath = folderDic[df_sel.iloc[0]['AccessNum']]
    except (KeyError):
        test_input = f"{df_sel.iloc[0]['AccessNum']}nullYBn"
        folderPath = folderDic[test_input]
    except:
        print(f"Error file:{df_sel.iloc[0]['AccessNum']}")
        continue
    img_name=["%03d" %i for i in range(len(df_sel['InstanceNum']))]

    path = f"/data/jacky831006/spleen_softlink/{df_sel.newID.values[0]}/{folderPath}/{CTfloder}/"
    if not os.path.isdir(path):
        os.makedirs(path)
    img_name_list = [f"spleen_softlink/{df_sel.newID.values[0]}/{folderPath}/{CTfloder}/IMG{i}.dcm" for i in img_name]
    img_dir_list = [f"{df_sel.newID.values[0]}/{folderPath}"]*len(img_name)
    df_sel['softPath']=np.array(img_name_list)
    df_sel['softdir']=np.array(img_dir_list)
    df_all = pd.concat([df_all,df_sel],axis=0).reset_index(drop=True)
df_all.to_csv("./slices_output.csv")

print("=========== File soft link generation ===========", flush=True)
#from shutil import copyfile

for i in range(len(df_all)):
    if not os.path.isfile(df_all['softPath'][i]): 
    #print(df_all['OriPath'][i],df_all['softPath'][i])
        #copyfile(df_all['OriPath'][i],df_all['softPath'][i])
        os.symlink(df_all['OriPath'][i],df_all['softPath'][i])

end = datetime.utcnow().strftime("%Y%m%d%H%M%S")
print(f"End time:{end}")
print("All is done")
        
        
