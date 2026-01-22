import os
import sys
import time
import pandas as pd
import logging
from utils.db_postgres import read_db_table, write_db, get_last_update_date
from utils.gsheet_client import open_sheet, export_df_to_sheet


# Configure basic logging
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Optional configuration
INCREMENTAL_MATCHES = int(os.environ.get("INCREMENTAL_MATCHES", "0"))  # default to False (full refresh)


def build_stats(df_matches, df_scores):

    # Total Points per Match
    df_total_points = df_scores.groupby("match_id").size().rename("total_points").reset_index()
    df_stats = df_total_points

    # Total Games per Match
    # count unique match_id, set_number, game_number
    df_total_games = df_scores.groupby(["match_id", "set_number", "game_number"]).size().reset_index()
    df_total_games = df_total_games.groupby("match_id").size().rename("total_games").reset_index()
    df_stats = df_stats.merge(df_total_games, on="match_id", how="left")

    # Total Deuce points
    df_deuce_points = (
        df_scores[df_scores["is_deuce"] == True]
        .groupby("match_id")
        .size()
        .rename("total_deuces")
        .reset_index()
    )
    df_stats = df_stats.merge(df_deuce_points, on="match_id", how="left")
    df_stats["total_deuces"] = df_stats["total_deuces"].fillna(0).astype(int)
    
    # Number of deuce points per game. If no deuce points, then 0
    df_deuce_count = (
        df_scores
        .groupby(["match_id", "set_number", "game_number"])["is_deuce"]
        .sum()
        .rename("deuce_count")
        .reset_index()
    )

    # Number of games with 1 deuce, 2 deuces, more than 2 deuces, per match
    s0 = df_deuce_count[df_deuce_count["deuce_count"] == 0].groupby("match_id").size().rename("games_0_deuce")
    s1 = df_deuce_count[df_deuce_count["deuce_count"] == 1].groupby("match_id").size().rename("games_1_deuce")
    s2 = df_deuce_count[df_deuce_count["deuce_count"] == 2].groupby("match_id").size().rename("games_2_deuces")
    s3 = df_deuce_count[df_deuce_count["deuce_count"] > 2].groupby("match_id").size().rename("games_3+_deuces")

    df_games_with_deuce = (
        pd.concat([s0, s1, s2, s3], axis=1)
        .fillna(0)
        .astype(int)
        .reset_index()
    )

    df_stats = df_stats.merge(df_games_with_deuce, on="match_id", how="left")
    
    # Sets with tie-breaks



    # Final join
    df_stats = df_matches.merge(df_stats, left_on="id", right_on="match_id", how="left").drop(columns=["id"])
    
    print("Match Stats Rows: " + str(len(df_stats)))
    print(df_stats.columns)
    print(df_stats.head())
    exit(0)
    
    return df_stats


def store_gold_data(df):
    df["created_at"] = pd.Timestamp.now()
    if_exists = "append" if INCREMENTAL_MATCHES == 1 else "replace"
    write_db(df, table_name="match_stats", schema="gold", if_exists=if_exists)

    df["created_at"] = df["created_at"].dt.strftime("%Y-%m-%d")
    export_df_to_sheet(worksheet_name="Match Stats", df=df, append_rows=(INCREMENTAL_MATCHES == 1))

def main():
    start = time.time()
    try:

        if INCREMENTAL_MATCHES == 1:
            last_update = get_last_update_date(table_name="match_stats", schema="gold")
            logger.info("Incremental mode: fetching rows from Silver layer created since %s", last_update.strftime("%Y-%m-%d %H:%M:%S"))
            df_matches = read_db_table(table_name="fact_match", schema="silver", from_timestamp=last_update)
            df_scores = read_db_table(table_name="fact_point", schema="silver", from_timestamp=last_update)
        else:
            df_matches = read_db_table(table_name="fact_match", schema="silver")
            df_scores = read_db_table(table_name="fact_point", schema="silver")

        if df_matches.empty or df_scores.empty:
            logger.warning("No rows found in Silver schema.")
        else:
            df_stats = build_stats(df_matches, df_scores)
            store_gold_data(df_stats)
            logger.info("Created stats from %d matches and %dK points", len(df_matches), round(len(df_scores)/1000,1))
            # logger.info("Created stats from %d matches and %dK points, %.1f%% deuce, %.1f%% tie-break, %.d%% game points, %d%% set points, %d%% match points", len(df_matches), round(len(df_scores)/1000,1), round(100 * df_trx_scores["is_deuce"].mean(),1), round(100 * df_trx_scores["is_tiebreak"].mean(),1), round(100 * df_trx_scores["is_game_point"].mean(),1), round(100 * df_trx_scores["is_set_point"].mean(),1), round(100 * df_trx_scores["is_match_point"].mean(),1))

    except Exception as e:
        logger.error("Match Stats script failed: %s", e)
        sys.exit(1)

    elapsed = time.time() - start
    logger.info("Match Stats script completed successfully in %d seconds", elapsed)
    sys.exit(0)


if __name__ == "__main__":
    main()