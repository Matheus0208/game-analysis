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
    with open(CONFIG_PATH, encoding="utf-8") as file:
        return json.load(file)


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
    session.mount("https://", adapter)
    return session


def fetch_games_list(session, config, api_key):
    """Busca a lista de jogos, página por página, e devolve uma lista de dicionários."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    params = {
        "key": api_key,
        "dates": f"{config['release_start']},{today}",
        "ordering": config["ordering"],
        "page_size": config["page_size"],
    }

    games = []
    for page in range(1, config["max_pages"] + 1):
        params["page"] = page
        response = session.get(f"{BASE_URL}/games", params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        for game in data["results"]:
            game.pop("short_screenshots", None)
            games.append(game)

        print(f"Página {page}: {len(data['results'])} jogos")

        if not data.get("next"):
            break

        time.sleep(config["pause_seconds"])

    return games

def fetch_game_details(session, game_id, api_key):
    """Busca os detalhes de um jogo e devolve o dicionário da resposta."""
    response =session.get(
        f"{BASE_URL}/games/{game_id}",
        params = {"key": api_key},
        timeout=30,
    )
    response.raise_for_status()

    return response.json()


def save_jsonl(s3, bucket, prefix, records, run_ts):
    """Salva os registros como JSON Lines no S3 e devolve o caminho do arquivo."""
    ingested_at = run_ts.isoformat()
    lines = [
        json.dumps({**record, "_ingest_at": ingested_at}, ensure_ascii=False)
        for record in records
    ]
    body = "\n".join(lines)

    key = f"{prefix}/dt={run_ts:%Y-%m-%d}/run_{run_ts:%Y%m%d%T%H%M%S}.jsonl"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return key


def load_state(s3, bucket):
    """Lê o arquivo de controle do S3."""
    try:
        response = s3.head_object(Bucket=BUCKET, Key=STATES_KEY)
    except s3.exceptions.NoSuchKey:
        print("Controle não encontrado: primeira execução.")
        return {}
    return json.loads(response["Body"].read())


def save_state(s3, bucket, state):
    """Grava o dicionário de controle no S3, em STATE_KEY."""
    s3.put_object(
        Bucket=bucket,
        Key=STATES_KEY,
        Body=json.dumps(state).encode("utf-8"),
        ContentType="application/json",
    )


def select_games_for_details(games, state, limit):
    """Devolve os jogos que precisam de detalhes, até o limite."""
    pending = [
        game for game in games
        if state.get(str(game["id"])) != game.get("update")
    ]
    return pending[:limit]


def lambda_handler(event, context):
    """Ponto de entrada da Lambda. Só orquestra as funções acima."""
    config = load_config()
    api_key = os.environ["RAWG_API_KEY"]
    bucket = os.environ["S3_BUCKET"]

    session = create_session()
    s3 = boto3.client("s3")
    run_ts = datetime.now(timezone.utc)

    games = fetch_games_list(session, config, api_key)
    list_key = save_jsonl(s3, bucket, "bronze/rawg/game_list", games, run_ts)
    print(f"Lista salva: {len(games)} jogos")

    state = load_state(s3, bucket)
    pending = select_games_for_details(games, state, config["max_details_per_run"])
    print(f"Jogos que precisam de detalhes nessa execução: {len(pending)}")

    details = []
    processed = []
    for game in pending:
        try:
            detail = fetch_game_details(session, game["id"], api_key)
        except requests.RequestException as error:
            print(f"Falha nos detalhes do jogo {game['id']}: {type(error).__name__}")
            continue
        detail["_game_id"] = game ["id"]
        details.append(detail)
        processed.append(game)
        time.sleep(config["pause_seconds"])

    

    return {"games": len(games), "games_list_file": list_key}

if __name__ == "__main__":
    print(lambda_handler({}, None))