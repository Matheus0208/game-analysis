"""Coleta de jogos da API da RAWG para a camada bronze do S3."""

import json
import os
import time
from datetime import datetime, timezone
import logging
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
    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504, 520, 521, 522, 523, 524],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.mount("https://", adapter)

    class RawgError(RuntimeError):
        pass

    def rawg_get_json(session, path, params):
        try:
            response = session.get(f"{BASE_URL}{path}", params=params, timeout=30)
        except requests.RequestException as error:
            raise RawgError(
                f"Falha na requisição à RAWG ({type(error).__name__})"
            ) from None

        if not 200 <= response.status_code < 300:
            raise RawgError(f"RAWG respondeu com HTTP {response.status_code}")

        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            raise RawgError("RAWG retornou JSON inválido") from None
    return session

class RawgError(RuntimeError):
    pass

def rawg_get_json(session, path, params):
    try:
        response = session.get(f"{BASE_URL}{path}", params=params, timeout=30)
    except requests.RequestException as error:
        raise RawgError(
            f"Falha na requisição à RAWG ({type(error).__name__})"
        ) from None

    if not 200 <= response.status_code < 300:
        raise RawgError(f"RAWG respondeu com HTTP {response.status_code}")

    try:
        return response.json()
    except requests.exceptions.JSONDecodeError:
        raise RawgError("RAWG retornou JSON inválido") from None


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
        data = rawg_get_json(session, "/games", params)

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
    return rawg_get_json(session, f"/games/{game_id}", {"key": api_key})

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
        response = s3.get_object(Bucket=bucket, Key=STATE_KEY)
    except s3.exceptions.NoSuchKey:
        print("Controle não encontrado: primeira execução.")
        return {}

    return json.loads(response["Body"].read())


def save_state(s3, bucket, state):
    """Grava o dicionário de controle no S3, em STATE_KEY."""
    s3.put_object(
        Bucket=bucket,
        Key=STATE_KEY,
        Body=json.dumps(state).encode("utf-8"),
        ContentType="application/json",
    )


def select_games_for_details(games, state, limit):
    """Devolve os jogos que precisam de detalhes, até o limite."""
    pending = []
    for game in games:
        game_id = str(game["id"])
        if game_id not in state or state[game_id] != game.get("updated"):
            pending.append(game)

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
        except RawgError as error:
            print(f"Falha nos detalhes do jogo {game['id']}: {error}")
            continue
        detail["_game_id"] = game["id"]
        details.append(detail)
        processed.append(game)
        time.sleep(config["pause_seconds"])

    details_key = None
    if details:
        details_key = save_jsonl(s3, bucket, "bronze/rawg/details", details, run_ts)
        for game in processed:
            state[str(game["id"])] = game.get("updated")
        save_state(s3, bucket, state)

    return {
        "games": len(games),
        "details": len(details),
        "failed_details": len(pending) - len(details),
        "games_list_file": list_key,
        "game_details_file": details_key,
    }

if __name__ == "__main__":
    print(lambda_handler({}, None))