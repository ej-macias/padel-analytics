import os
import sys
import json
import time
import pandas as pd
import logging
from utils.padel_api_client import api_get
from utils.db_postgres import write_db


# Configure basic logging
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Required environment variables
REQUIRED_ENVS = [
    "PADEL_API_TOKEN",
    "POSTGRES_USER",
    "POSTGRES_PWD",
    "POSTGRES_HOST",
    "POSTGRES_DB",
]

missing = [v for v in REQUIRED_ENVS if not os.environ.get(v)]
if missing:
    logger.error("Missing required environment variables: %s", ", ".join(missing))
    sys.exit(2)  # non-zero so schedulers know it failed

# Optional configuration
INCREMENTAL_MATCHES = int(os.environ.get("INCREMENTAL_MATCHES", "0"))  # default to False (full refresh)

# Postgres configuration
username = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PWD"]
host = os.environ["POSTGRES_HOST"]
database = os.environ["POSTGRES_DB"]

# Function to extract the players names and sides from the nested structure
def extract_team_sides(players: dict) -> dict:
    out = {
        "team_1_backhand": None,
        "team_1_drive": None,
        "team_2_backhand": None,
        "team_2_drive": None,
    }

    if not isinstance(players, dict):
        return out

    for team_key in ("team_1", "team_2"):
        team_players = players.get(team_key, []) or []

        # First pass: use explicit side if present
        for player in team_players:
            side = (player.get("side") or "").lower()
            if side in ("backhand", "drive"):
                out[f"{team_key}_{side}"] = player.get("name")

        # Second pass (fallback): assign by order if still missing
        if out[f"{team_key}_backhand"] is None and len(team_players) >= 1:
            out[f"{team_key}_backhand"] = team_players[0].get("name")

        if out[f"{team_key}_drive"] is None and len(team_players) >= 2:
            out[f"{team_key}_drive"] = team_players[1].get("name")

    return out


def games_only(value: str) -> int:
    """
    Extract set games from values like:
      "6"      -> 6
      "6(5)"   -> 6
      "7"      -> 7
    """
    return int(value.split("(", 1)[0])

def match_score_from_sets(score):
    wins = [
        games_only(s["team_1"]) > games_only(s["team_2"])
        for s in score
    ]
    team_1_sets = sum(wins)
    team_2_sets = len(wins) - team_1_sets
    return f"{team_1_sets}-{team_2_sets}"


def parse_matches(df: pd.DataFrame) -> pd.DataFrame:
    # Flatten everything on first level (excluding players info)
    players_cols = df["players"].apply(extract_team_sides).apply(pd.Series)

    df_flat = pd.concat(
        [df.drop(columns=["players"]), players_cols],
        axis=1
    )

    # Get the match score in sets (e.g. "2-1") from the nested "score" structure
    df_flat["score"] = df_flat["score"].apply(match_score_from_sets)

    df_flat["created_at"] = pd.Timestamp("now")

    # Keep only relevant fields
    return df_flat[
        [
            "id",
            "played_at",
            "category",
            "round_name",
            "team_1_backhand",
            "team_1_drive",
            "team_2_backhand",
            "team_2_drive",
            "score",
            "winner",
            "duration",
            "created_at"
        ]
    ]


def fetch_score(match_id: int, dict_scores: dict) -> pd.DataFrame: # TO DO: remove 2nd param when using "live" endpoint !!
    # payload = api_get(endpoint=f"matches/{match_id}/live") # TO DO: uncomment when using "live" endpoint !!
    
    # TO DO: Remove when using "live" endpoint !!
    payload = dict_scores.get(match_id)

    if "sets" not in payload:
        logger.error("API response missing 'sets' key")
        raise RuntimeError("API response missing 'sets'")
    # else: logger.info("Fetched score data for match id: %d", match_id)

    df_score = pd.json_normalize(payload["sets"], max_level=0)
    return df_score


def parse_score(match_id, sets_scores: dict) -> pd.DataFrame:
    # Flatten the nested structure into a point-by-point DataFrame
    rows = []

    for _, s in sets_scores.iterrows():
        set_no = s["set_number"]
        for g in s["games"]:
            game_no = g["game_number"]
            game_score = g["game_score"]
            for point_idx, point in enumerate(g["points"], start=1):
                rows.append({
                    "match_id": match_id,
                    "set_number": set_no,
                    "game_number": game_no,
                    "game_score_start": game_score,
                    "point_number": point_idx,
                    "point_score_start": point,
                    "created_at": pd.Timestamp("now")
                })

    logger.info("Found %d point scores for match id: %d", len(rows), match_id)
    return pd.DataFrame(rows)


def get_match_data():
    # TO DO: Uncomment when using "live" endpoint !!
    # payload = api_get(endpoint="matches")
    with open("data/fake_matches_100.json") as f:
        payload = json.load(f)

        # TO DO: replace "matches" with "data" when using "live" endpoint !!
        if "matches" not in payload:
            logger.error("API response missing 'data' key")
            raise RuntimeError("API response missing 'data'")

        # TO DO: replace "matches" with "data" when using "live" endpoint !!
        df_raw_data = pd.json_normalize(payload["matches"], max_level=0)
        df_scores = pd.DataFrame()

        with open("data/fake_scores_100.json") as f:

            json_scores = json.load(f)
            dict_scores = {m["id"]: m for m in json_scores["matches"]}

            for match_id in df_raw_data['id']:
                df_raw_score = fetch_score(match_id, dict_scores) # TO DO: remove dict_scores when using "live" endpoint !!
                match_scores = parse_score(match_id, df_raw_score)
                df_scores = pd.concat([df_scores, match_scores], ignore_index=True)

    df_matches = pd.DataFrame()
    if len(df_raw_data) > 0:
        df_matches = parse_matches(df_raw_data)
        logger.info("Received %d matches and %d points from API", len(df_matches), len(df_scores))

    return df_matches, df_scores


def store_data(df, table_name, schema):
    if_exists = "append" if INCREMENTAL_MATCHES == 1 else "replace"
    write_db(df, table_name, schema, if_exists)


def main():
    start = time.time()
    try:
        df_matches, df_scores = get_match_data()

        if df_matches.empty:
            logger.warning("No new matches found")
        else:
            store_data(df_scores, table_name="fact_point", schema="bronze")
            store_data(df_matches, table_name="fact_match", schema="bronze")
    except Exception as e:
        logger.error("Get-matches script failed: %s", e)
        # non-zero exit so scheduler detects failure
        sys.exit(1)

    elapsed = time.time() - start
    logger.info("Get-matches script completed successfully in %d seconds", elapsed)
    sys.exit(0)

if __name__ == "__main__":
    main()