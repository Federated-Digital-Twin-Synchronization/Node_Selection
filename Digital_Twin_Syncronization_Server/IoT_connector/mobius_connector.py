import requests
import json
import os
from typing import Dict
import logging

class MobiusConnector:
    def __init__(self, url):
        url = os.path.join("http://", url, "Mobius", "Meta-Sejong", "Daeyang_AI_Center", "5F")
        self.url = url
        self._payload = {}
        self._headers = {
            'Accept': 'application/json',
            'X-M2M-RI': '12345',
            'X-M2M-Origin': 'SOrigin',
            'Content-Type': 'application/json;ty=4'
        }

    def send(self, target_path, data: Dict):
        url = os.path.join(self.url, target_path)
        payload = self._payload
        payload["m2m:cin"] = {"con": data}
        payload = json.dumps(payload)
        response = requests.request('POST', url, headers=self._headers, data=payload)
        print(response)
