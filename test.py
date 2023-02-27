import pytest
import sys
import os
import json
import os
sys.path.append('../Assignment_02')
import api_codes.nexrad_api as nexrad_api
import api_codes.goes_api as goes_api
from fastapi.testclient import TestClient
from httpx import AsyncClient

client = TestClient(nexrad_api.app)
client_goes=TestClient(goes_api.app)

def test_link_nexrad_streamlit():

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2011', 'month': '10' ,'day': '10' ,'station': 'KBGM' ,'file': 'KBGM20111010_000301_V03.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2011/10/10/KBGM/KBGM20111010_000301_V03.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2011', 'month': '06' ,'day': '12' ,'station': 'KBGM' ,'file': 'KBGM20110612_003045_V03.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2011/06/12/KBGM/KBGM20110612_003045_V03.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2010', 'month': '05' ,'day': '12' ,'station': 'KARX' ,'file': 'KARX20100512_014240_V03.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2010/05/12/KARX/KARX20100512_014240_V03.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2013', 'month': '09' ,'day': '02' ,'station': 'KABX' ,'file': 'KABX20130902_002911_V06.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2013/09/02/KABX/KABX20130902_002911_V06.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2000', 'month': '12' ,'day': '22' ,'station': 'KBIS' ,'file': 'KBIS20001222_090728.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2000/12/22/KBIS/KBIS20001222_090728.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2012', 'month': '02' ,'day': '03' ,'station': 'KCCX' ,'file': 'KCCX20120203_013605_V03.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2012/02/03/KCCX/KCCX20120203_013605_V03.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2015', 'month': '08' ,'day': '04' ,'station': 'KBYX' ,'file': 'KBYX20150804_000940_V06.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2015/08/04/KBYX/KBYX20150804_000940_V06.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2012', 'month': '07' ,'day': '17' ,'station': 'KAPX' ,'file': 'KAPX20120717_013219_V06.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2012/07/17/KAPX/KAPX20120717_013219_V06.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2014', 'month': '09' ,'day': '07' ,'station': 'KAPX' ,'file': 'KAPX20140907_010223_V06.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2014/09/07/KAPX/KAPX20140907_010223_V06.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2008', 'month': '08' ,'day': '19' ,'station': 'KCBW' ,'file': 'KCBW20080819_012424_V03.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2008/08/19/KCBW/KCBW20080819_012424_V03.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '1993', 'month': '11' ,'day': '12' ,'station': 'KLWX' ,'file': 'KLWX19931112_005128.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/1993/11/12/KLWX/KLWX19931112_005128.gz"

    response = client.post(url = "/nexrad_s3_fetchurl",  json = {'year': '2003', 'month': '07' ,'day': '17' ,'station': 'KBOX' ,'file': 'KBOX20030717_014732.gz'})
    assert response.status_code == 200
    url = response.json()['Public S3 URL']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2003/07/17/KBOX/KBOX20030717_014732.gz"


def test_goes18_link_generation():
    response = client_goes.post(url = "/goes_fetch_url",  json = {'station':'ABI-L1b-RadC','year': '2023', 'day': '002' ,'hour': '01' ,'file': 'OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc'})
    assert response.status_code == 200
    url = response.json()['NOAAURL']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/2023/002/01/OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc"

    response = client_goes.post(url = "/goes_fetch_url",  json = {'station':'ABI-L2-ACMM','year': '2023', 'day': '009' ,'hour': '05' ,'file': 'OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc'})
    assert response.status_code == 200
    url = response.json()['NOAAURL']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACMM/2023/009/05/OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc"
    
    response = client_goes.post(url = "/goes_fetch_url",  json = {'station':'ABI-L2-ACTPM','year': '2023', 'day': '009' ,'hour': '04' ,'file': 'OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc'})
    assert response.status_code == 200
    url = response.json()['NOAAURL']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACTPM/2023/009/04/OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc"
    
    response = client_goes.post(url = "/goes_fetch_url",  json = {'station':'ABI-L2-DSIM','year': '2023', 'day': '011' ,'hour': '06' ,'file': 'OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc'})
    assert response.status_code == 200
    url = response.json()['NOAAURL']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DSIM/2023/011/06/OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc"
    
    response = client_goes.post(url = "/goes_fetch_url",  json = {'station':'ABI-L2-ACHTM','year': '2022', 'day': '356' ,'hour': '08' ,'file': 'OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc'})
    assert response.status_code == 200
    url = response.json()['NOAAURL']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTM/2022/356/08/OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc"
    
    response = client_goes.post(url = "/goes_fetch_url",  json = {'station':'ABI-L2-BRFF','year': '2022', 'day': '315' ,'hour': '02' ,'file': 'OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc'})
    assert response.status_code == 200
    url = response.json()['NOAAURL']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-BRFF/2022/315/02/OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc"

def test_nexrad_file_retrival_main():

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KBGM20110612_003045_V03.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2011/06/12/KBGM/KBGM20110612_003045_V03.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KARX20100512_014240_V03.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2010/05/12/KARX/KARX20100512_014240_V03.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KABX20130902_002911_V06.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2013/09/02/KABX/KABX20130902_002911_V06.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KBIS20001222_090728.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2000/12/22/KBIS/KBIS20001222_090728.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KCCX20120203_013605_V03.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2012/02/03/KCCX/KCCX20120203_013605_V03.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KCBW20011213_002358.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2001/12/13/KCBW/KCBW20011213_002358.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KBYX20150804_000940_V06.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2015/08/04/KBYX/KBYX20150804_000940_V06.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KAPX20120717_013219_V06.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2012/07/17/KAPX/KAPX20120717_013219_V06.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KAPX20140907_010223_V06.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2014/09/07/KAPX/KAPX20140907_010223_V06.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KCBW20080819_012424_V03.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2008/08/19/KCBW/KCBW20080819_012424_V03.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KLWX19931112_005128.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/1993/11/12/KLWX/KLWX19931112_005128.gz"

    response = client.post(url = "/nexrad_get_download_link",  json = {"filename": 'KBOX20030717_014732.gz'})
    assert response.status_code == 200
    url = response.json()['Response']
    assert url == "https://noaa-nexrad-level2.s3.amazonaws.com/2003/07/17/KBOX/KBOX20030717_014732.gz"

def test_goes_file_retrival_main():

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/2023/002/01/OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACMM/2023/009/05/OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACTPM/2023/009/04/OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DSIM/2023/011/06/OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTM/2022/356/08/OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-BRFF/2022/315/02/OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L2-ADPM2-M6_G18_s20230061310557_e20230061311015_c20230061311402.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ADPM/2023/006/13/OR_ABI-L2-ADPM2-M6_G18_s20230061310557_e20230061311015_c20230061311402.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadM/2023/003/02/OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L2-ACHTF-M6_G18_s20223532240210_e20223532249518_c20223532252164.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTF/2022/353/22/OR_ABI-L2-ACHTF-M6_G18_s20223532240210_e20223532249518_c20223532252164.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L2-DSRC-M6_G18_s20223180501179_e20223180503552_c20223180508262.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DSRC/2022/318/05/OR_ABI-L2-DSRC-M6_G18_s20223180501179_e20223180503552_c20223180508262.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L2-DMWVM1-M6C08_G18_s20223552050271_e20223552050328_c20223552122197.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DMWVM/2022/355/20/OR_ABI-L2-DMWVM1-M6C08_G18_s20223552050271_e20223552050328_c20223552122197.nc"

    response = client.post(url = "/getfileUrl",  json = {"file_name": 'OR_ABI-L2-ACMC-M6_G18_s20222800931164_e20222800933537_c20222800934574.nc'})
    assert response.status_code == 200
    url = response.json()['message']
    assert url == "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACMC/2022/280/09/OR_ABI-L2-ACMC-M6_G18_s20222800931164_e20222800933537_c20222800934574.nc"
    
    
# def test_goes18_link_generation():
    
#     assert 'https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/2023/002/01/OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc'== main_goes18.create_url('ABI-L1b-RadC','2023','002','01','OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc')
#     assert 'https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACMM/2023/009/05/OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc'==main_goes18.create_url('ABI-L2-ACMM','2023','009','05','OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc')
#     assert 'https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACTPM/2023/009/04/OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc'==main_goes18.create_url('ABI-L2-ACTPM','2023','009','04','OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc')
#     assert 'https://noaa-goes18.s3.amazonaws.com/ABI-L2-DSIM/2023/011/06/OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc'==main_goes18.create_url('ABI-L2-DSIM','2023','011','06','OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc')
#     assert 'https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTM/2022/356/08/OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc'==main_goes18.create_url('ABI-L2-ACHTM','2022','356','08','OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc')
#     assert 'https://noaa-goes18.s3.amazonaws.com/ABI-L2-BRFF/2022/315/02/OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc'==main_goes18.create_url('ABI-L2-BRFF','2022','315','02','OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc')
#     assert 'https://noaa-goes18.s3.amazonaws.com/ABI-L2-ADPM/2023/006/13/OR_ABI-L2-ADPM2-M6_G18_s20230061310557_e20230061311015_c20230061311402.nc'==main_goes18.create_url('ABI-L2-ADPM','2023','006','13','OR_ABI-L2-ADPM2-M6_G18_s20230061310557_e20230061311015_c20230061311402.nc')
#     assert 'https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadM/2023/003/02/OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc'==main_goes18.create_url('ABI-L1b-RadM','2023','003','02','OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc')
#     assert 'https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTF/2022/353/22/OR_ABI-L2-ACHTF-M6_G18_s20223532240210_e20223532249518_c20223532252164.nc'==main_goes18.create_url('ABI-L2-ACHTF','2022','353','22','OR_ABI-L2-ACHTF-M6_G18_s20223532240210_e20223532249518_c20223532252164.nc')


# def test_goes_file_retrival_main():
    
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/2023/002/01/OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc"   == goes_file_retrieval_main.get_file_url("OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACMM/2023/009/05/OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc"       == goes_file_retrieval_main.get_file_url("OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACTPM/2023/009/04/OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc"     == goes_file_retrieval_main.get_file_url("OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DSIM/2023/011/06/OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc"       == goes_file_retrieval_main.get_file_url("OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTM/2022/356/08/OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc"     == goes_file_retrieval_main.get_file_url("OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L2-BRFF/2022/315/02/OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc"        == goes_file_retrieval_main.get_file_url("OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ADPM/2023/006/13/OR_ABI-L2-ADPM2-M6_G18_s20230061310557_e20230061311015_c20230061311402.nc"       == goes_file_retrieval_main.get_file_url("OR_ABI-L2-ADPM2-M6_G18_s20230061310557_e20230061311015_c20230061311402.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadM/2023/003/02/OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc"  == goes_file_retrieval_main.get_file_url("OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTF/2022/353/22/OR_ABI-L2-ACHTF-M6_G18_s20223532240210_e20223532249518_c20223532252164.nc"      == goes_file_retrieval_main.get_file_url("OR_ABI-L2-ACHTF-M6_G18_s20223532240210_e20223532249518_c20223532252164.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DSRC/2022/318/05/OR_ABI-L2-DSRC-M6_G18_s20223180501179_e20223180503552_c20223180508262.nc"        == goes_file_retrieval_main.get_file_url("OR_ABI-L2-DSRC-M6_G18_s20223180501179_e20223180503552_c20223180508262.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DMWVM/2022/355/20/OR_ABI-L2-DMWVM1-M6C08_G18_s20223552050271_e20223552050328_c20223552122197.nc"  == goes_file_retrieval_main.get_file_url("OR_ABI-L2-DMWVM1-M6C08_G18_s20223552050271_e20223552050328_c20223552122197.nc")
#     assert "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACMC/2022/280/09/OR_ABI-L2-ACMC-M6_G18_s20222800931164_e20222800933537_c20222800934574.nc"        == goes_file_retrieval_main.get_file_url("OR_ABI-L2-ACMC-M6_G18_s20222800931164_e20222800933537_c20222800934574.nc")

# def test_nexrad_file_retrival_main():
    
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2011/06/12/KBGM/KBGM20110612_003045_V03.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KBGM20110612_003045_V03.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2010/05/12/KARX/KARX20100512_014240_V03.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KARX20100512_014240_V03.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2013/09/02/KABX/KABX20130902_002911_V06.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KABX20130902_002911_V06.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2000/12/22/KBIS/KBIS20001222_090728.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KBIS20001222_090728.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2012/02/03/KCCX/KCCX20120203_013605_V03.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KCCX20120203_013605_V03.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2001/12/13/KCBW/KCBW20011213_002358.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KCBW20011213_002358.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2015/08/04/KBYX/KBYX20150804_000940_V06.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KBYX20150804_000940_V06.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2012/07/17/KAPX/KAPX20120717_013219_V06.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KAPX20120717_013219_V06.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2014/09/07/KAPX/KAPX20140907_010223_V06.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KAPX20140907_010223_V06.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2008/08/19/KCBW/KCBW20080819_012424_V03.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KCBW20080819_012424_V03.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/1993/11/12/KLWX/KLWX19931112_005128.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KLWX19931112_005128.gz")
#     assert "https://noaa-nexrad-level2.s3.amazonaws.com/2003/07/17/KBOX/KBOX20030717_014732.gz" == nexrad_file_retrieval_main.get_nexrad_file_url("KBOX20030717_014732.gz")
