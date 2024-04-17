import io
import time
import json
import requests
import pandas as pd
import pytz
from datetime import datetime

def codeListing() :

    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    otp_params = {
        'locale': 'ko_KR',
        'share': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT04601'
    }
    headers = {'Referer' : 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
    otp = requests.post(otp_url, params = otp_params, headers = headers).text

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_params = {'code' : otp}
    response = requests.post(down_url, params = down_params, headers = headers)

    data = pd.read_csv(io.BytesIO(response.content), encoding = 'euc-kr', dtype = {'단축코드': 'string'})

    return data

def PDFListing(isuCd, code, name, date) :

    headers = {'Referer' : 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}

    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    otp_params = {
        'locale': 'ko_KR',
        'tboxisuCd_finder_secuprodisu1_0' : f'{code}/{name}',
        'isuCd': f'{isuCd}',
        'isuCd2': f'{isuCd}',
        'codeNmisuCd_finder_secuprodisu1_0': f'{name}',
        'param1isuCd_finder_secuprodisu1_0': "",
        'trdDd': f'{date}',
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT05001'
        }

    otp = requests.post(otp_url, params = otp_params, headers = headers).text

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_params = {'code' : otp}
    response = requests.post(down_url, params = down_params, headers = headers)

    data = pd.read_csv(io.BytesIO(response.content),
                       encoding = 'euc-kr',
                       dtype = {'단축코드' : str})

    return data


def loadData() :

    url = 'https://raw.githubusercontent.com/SEUNGTO/botdata/main/resultDict.json'
    response = requests.get(url)
    resultDict = response.json()

    for code in resultDict.keys() :
        for date in resultDict[code].keys() :
            resultDict[code][date] = pd.DataFrame(resultDict[code][date])

    return resultDict

if __name__ == '__main__' :

    # 코드 불러오기
    codeList = codeListing()
    codeList = codeList[(codeList['기초시장분류'] == '국내') & (codeList['기초자산분류'] == '주식')]

    # 기존 데이터 불러오기
    resultDict = loadData()

    # 오늘 날짜 세팅
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    date = now.strftime('%Y%m%d')

    for isuCd, code, name in zip(codeList['표준코드'], codeList['단축코드'], codeList['한글종목약명']) :
        data = PDFListing(isuCd, code, name, date)
        if code not in resultDict.keys() :
            tmpDict = {}
            tmpDict[date] = data
            resultDict[code] = tmpDict
            time.sleep(0.5)
        else :
            resultDict[code][date] = data
            time.sleep(0.5)

    for code in resultDict.keys() :
        for date in resultDict[code].keys() :
            resultDict[code][date] = resultDict[code][date].to_dict()

    with open('test_resultDict.json', 'w') as f :
        json.dump(resultDict, f)
