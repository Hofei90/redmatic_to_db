from dataclasses import dataclass
from dataclasses_json import dataclass_json
import datetime
import requests
import json


@dataclass_json
@dataclass
class Datum:
    ts: datetime.datetime
    device: str
    value: float
    datenname: str


def sende_daten(url, table, headers, daten):
    if not url.endswith("/"):
        url = f"{url}/"
    url = f"{url}{table}"
    for datensatz in daten:
        datensatz.ts = datensatz.ts.strftime("%Y-%m-%d %H:%M:%S")

    r = requests.post(url, headers=headers, json=[datum.to_dict() for datum in daten])
    print(r.status_code)
    print(r.text)


def status_auswerten(r, daten):
    if not (r.status_code == 200 or r.status_code == 201):
        print(f"Statuscode: {r.status_code}\n Message: {r.text}")
        print(daten)
    else:
        print("Erfolgreich übertragen")
