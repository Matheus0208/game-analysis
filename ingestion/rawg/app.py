"""Coleta de jogos usando a API da RAWG para camada bronze"""

import json
import os
import time
from datetime import datetime, timezone
import requests
import boto3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry