"""Coleta de jogos da API da RAWG para a camada bronze do S3."""

import json
import os
import time
from datetime import datetime, timezone

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.rawg.io/api"
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
STATE_KEY = "control/rawg/details_state.json"


def load_config():
    """Lê o config.json e devolve um dicionário."""
    with open(CONFIG_PATH, encoding="utf=8") as json_file:
        config = json.load(json_file)
    return config


def create_session():
    """Cria uma sessão do requests com novas tentativas automáticas."""
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    return session


def fetch_games_list(session, config, api_key):
    """Busca a lista de jogos, página por página, e devolve uma lista de dicionários."""
    today=datetime.now(timezone.utc).strftime("%d/%m/%Y")
    params = {
        "key": api_key,
        "dates": f"{config['release_start']},{today}",
        "ordering": config['ordering'],
        "page_size": config['page_size'],
    }
    games = []
    for page in range(1, config['max_pages'] + 1):
        params['page'] = page
        response = session.get(f"{BASE_URL}/games", params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        for data in data['results']:
            games.pop("short_screenshots", None)
            games.append(data)

        print(f"Página {page}: {len(data['results'])} jogos")

        if not data.get("next"):
            break

        time.sleep(config['pause_seconds'])

    return games

def fetch_game_details(session, game_id, api_key):
    """Busca os detalhes de um jogo e devolve o dicionário da resposta."""


def save_jsonl(s3, bucket, prefix, records, run_ts):
    """Salva os registros como JSON Lines no S3 e devolve o caminho do arquivo."""


def load_state(s3, bucket):
    """Lê o arquivo de controle do S3."""


def save_state(s3, bucket, state):
    """Grava o dicionário de controle no S3, em STATE_KEY."""


def select_games_for_details(games, state, limit):
    """Devolve os jogos que precisam de detalhes, até o limite."""


def lambda_handler(event, context):
    """Ponto de entrada da Lambda. Só orquestra as funções acima."""


if __name__ == "__main__":
    print(lambda_handler({}, None))