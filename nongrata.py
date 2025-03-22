from http.server import HTTPServer
from http.client import HTTPSConnection
import ssl

import json
import pandas as pd
import sqlite3

from api import *
import time
from random import random
import threading

global dataTime
global lastDecisionTime
global updateTime
global registry

dataTime = 0
lastDecisionTime = 0
updateTime = 0

def registrySync():
    global dataTime
    global lastDecisionTime
    global updateTime
    global registry
    while True:
        try:
            # sslContext = ssl._create_unverified_context()
            # connection = HTTPSConnection("reestrs.minjust.gov.ru", 443,\
            # context=sslContext)
            # connection.request("POST", "/rest/registry/39b95df9-9a68-6b6d-e1"\
            # "e3-e6388507067e/values", body='{"offset":0,"limit":9999,"search'\
            # '":"","facets":{},"sort":[]}',
            # headers={"Content-Type":"application/json;charset=utf-8"})
            # response = connection.getresponse().read().decode("utf-8")
            # Для тестирования, чтобы лишний раз не тревожить сервер Минюста:
            response = open("reg.json", "rb").read().decode("utf-8")
            registry = pd.DataFrame.from_dict(json.loads(response)["values"])\
            [["field_1_i","field_2_s","field_4_s","field_5_s","field_7_s"]]\
            .set_index('field_1_i')
            # Последнее техническое обновление реестра, UNIX TS в секундах
            dataTime=int(pd.DataFrame.from_dict(json.loads(response)["values"\
            ])["lastModified_l"].max() // 1000)
            agentTypeCodes = {
                "Физические лица": 1,
                "Иные объединения лиц": 3,
                "Юридические лица": 2,
                "Общественные объединения, действующие без образования юриди"\
                "ческого лица": 4,
                "Иностранные структуры без образования юридического лица": 5
            }
            # Перевод в дни с 01.01.1970 для экономии памяти
            # Неуказанные представляются как некоторый день в году 1752
            registry[["field_4_s", "field_5_s"]] = (pd.to_datetime\
            (registry[["field_4_s", "field_5_s"]].stack(),\
            format='%d.%m.%Y', errors='coerce').astype('int64').unstack() //\
            (1000000000*86400)).astype('int')
            registry["field_7_s"] = registry["field_7_s"].map(agentTypeCodes)\
            .fillna(0).astype(int)
            lastDecisionTime =\
            int(registry[["field_4_s","field_5_s"]].max().max() * 86400)
            print(lastDecisionTime)
            updateTime = int(time.time())
            print(registry.shape[0])
        except Exception as e:
            print("Sync error", e)
        time.sleep(21600)

def registryInfo(**kwargs):
    global dataTime
    global lastDecisionTime
    global updateTime
    global registry
    print(registry.shape[0])
    return {"updateTime": updateTime,
    "dataTime": dataTime,
    "lastDecisionTime": lastDecisionTime,
    "currentTime": int(time.time()),
    "length": registry.shape[0]}

def markText(text: str, staticAsterisk: str = "",
dynamicAsteriskStyle: str = "none", dynamicAsteriskStaticPostfix: str = "",
note: str = "_n_ — аноагент.", pardonedPolicy: str = "warn",
suspiciousPolicy: str = "warn", **kwargs):
    return {"s": "x"}


methods = {
    "registryInfo": registryInfo
}

HTTPAPIRequestHandler.methods = methods

registryUpdate = threading.Thread(target=registrySync, daemon=True)
registryUpdate.start()

HTTPServer(("", 80), HTTPAPIRequestHandler).serve_forever()