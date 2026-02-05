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


def get_simple_totals(df_matches, df_scores):
    # Total Points per Match
    df_total_points = df_scores.groupby("match_id").size().rename("total_points").reset_index()
    
    # Total Games per Match
    # count unique match_id, set_number, game_number
    df_total_games = df_scores.groupby(["match_id", "set_number", "game_number"]).size().reset_index()
    df_total_games = df_total_games.groupby("match_id").size().rename("total_games").reset_index()
    
    # Final join
    df_totals = df_matches.merge(df_total_points, left_on="id", right_on="match_id", how="left")
    return df_totals.merge(df_total_games, on="match_id", how="left")


def get_deuce_stats(df_scores):
    # Total Deuce points
    df_deuce_points = (
        df_scores #[df_scores["is_deuce"] == True]
        .groupby("match_id")["is_deuce"]
        .sum() #.size()
        .rename("total_deuces")
        .reset_index()
    )
    
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

    return df_deuce_points.merge(df_games_with_deuce, on="match_id", how="left")


def get_tiebreak_stats(df_matches, df_scores):
    # Sets with tie-breaks
    df_tiebreaks = (
        df_scores[df_scores["is_tiebreak"] == True]
        .groupby("match_id")["set_number"]
        .nunique()
        .rename("sets_with_tiebreak")
        .reset_index()
    )

    # Attach to full match list and fill missing with 0
    df_tiebreaks = (
        df_matches[["id"]].rename(columns={"id": "match_id"})
        .merge(df_tiebreaks, on="match_id", how="left")
    )

    df_tiebreaks["sets_with_tiebreak"] = (
        df_tiebreaks["sets_with_tiebreak"]
        .fillna(0)
        .astype(int)
    )

    # Tie-breaks won by each team, per match, 
    # 1) Keep only the last point of each tie-break game
    tb_last = (
        df_scores.loc[df_scores["is_tiebreak"] & df_scores["is_game_point"]]
        [["match_id", "set_number", "game_number", "point_score_team_1", "point_score_team_2"]]
        .drop_duplicates(subset=["match_id", "set_number", "game_number"])
        .copy()
    )

    # 2) Coerce pre-point tie-break scores to numeric; "A" -> NaN (won't crash)
    tb_last["p1"] = pd.to_numeric(tb_last["point_score_team_1"], errors="coerce")
    tb_last["p2"] = pd.to_numeric(tb_last["point_score_team_2"], errors="coerce")

    # 3) Winner logic using PRE-point scores
    team1_wins = (tb_last["p1"] + 1 >= 7) & ((tb_last["p1"] + 1 - tb_last["p2"]) >= 2)
    team2_wins = (tb_last["p2"] + 1 >= 7) & ((tb_last["p2"] + 1 - tb_last["p1"]) >= 2)

    tb_last["team_1_won_tb"] = team1_wins.fillna(False)
    tb_last["team_2_won_tb"] = team2_wins.fillna(False)

    # 4) Aggregate per match
    df_tb_wins_match = (
        tb_last
        .groupby("match_id", as_index=False)
        .agg(
            tie_breaks_won_team_1=("team_1_won_tb", "sum"),
            tie_breaks_won_team_2=("team_2_won_tb", "sum"),
        )
    )

    df_tb_wins_match[["tie_breaks_won_team_1", "tie_breaks_won_team_2"]] = (
        df_tb_wins_match[["tie_breaks_won_team_1", "tie_breaks_won_team_2"]].fillna(0).astype(int)
    )

    df_tiebreaks = df_tiebreaks.merge(df_tb_wins_match, on="match_id", how="left")
    df_tiebreaks[["tie_breaks_won_team_1", "tie_breaks_won_team_2"]] = (
        df_tiebreaks[["tie_breaks_won_team_1", "tie_breaks_won_team_2"]].fillna(0).astype(int)
    )

    return df_tiebreaks


def get_points_per_game_stats(df_scores):
    # Avg points per game for each match
    df_points_per_game = (
        df_scores
        .groupby(["match_id", "set_number", "game_number"])
        .size()
        .rename("points_per_game")
        .reset_index()
    )

    df_avg_points_per_game = (
        df_points_per_game
        .groupby("match_id")["points_per_game"]
        .mean().round(1)
        .rename("avg_points_per_game")
        .reset_index()
    )

    # Max points in a single game per match
    df_max_points_in_game = (
        df_points_per_game
        .groupby("match_id")["points_per_game"]
        .max()
        .rename("max_points_in_game")
        .reset_index()
    )
    
    return df_avg_points_per_game.merge(df_max_points_in_game, on="match_id", how="left")


def get_game_points_saved(df_scores):
    # Ensure points are in chronological order within each game
    dfp = df_scores.sort_values(["match_id", "set_number", "game_number", "point_number"]).copy()

    # Only regular games (tie-breaks have different logic)
    dfp = dfp[~dfp["is_tiebreak"]].copy()

    # Normalize to strings (in case you have ints mixed in)
    p1 = dfp["point_score_team_1"].astype(str)
    p2 = dfp["point_score_team_2"].astype(str)

    # Game point opportunities (pre-point)
    gp_t1 = (p1 == "A") | ((p1 == "40") & (p2.isin(["0", "15", "30"])))
    gp_t2 = (p2 == "A") | ((p2 == "40") & (p1.isin(["0", "15", "30"])))

    # Faced = opponent has a game point
    dfp["gp_faced_t1"] = gp_t2
    dfp["gp_faced_t2"] = gp_t1

    game_keys = ["match_id", "set_number", "game_number"]

    # Is there another point after this one in the same game?
    dfp["has_next_point_in_game"] = (
        dfp.groupby(game_keys)["point_number"].shift(-1).notna()
    )

    dfp["gp_saved_t1"] = dfp["gp_faced_t1"] & dfp["has_next_point_in_game"]
    dfp["gp_saved_t2"] = dfp["gp_faced_t2"] & dfp["has_next_point_in_game"]

    df_game_points_match = (
    dfp.groupby("match_id", as_index=False)
       .agg(
           game_points_faced_team_1=("gp_faced_t1", "sum"),
           game_points_saved_team_1=("gp_saved_t1", "sum"),
           game_points_faced_team_2=("gp_faced_t2", "sum"),
           game_points_saved_team_2=("gp_saved_t2", "sum"),
       )
    )

    # Convert booleans summed to ints cleanly
    cols = [
        "game_points_faced_team_1", "game_points_saved_team_1",
        "game_points_faced_team_2", "game_points_saved_team_2",
    ]
    df_game_points_match[cols] = df_game_points_match[cols].astype(int)
    
    return df_game_points_match


def add_set_point_flqgs(df_scores):
    dfp = df_scores.sort_values(
        ["match_id", "set_number", "game_number", "point_number"]
    ).copy()

    # --- reuse the same set-point logic (inline to keep this function standalone) ---
    g1 = pd.to_numeric(dfp["game_score_team_1"], errors="coerce")
    g2 = pd.to_numeric(dfp["game_score_team_2"], errors="coerce")

    dfp["sp_for_t1"] = False
    dfp["sp_for_t2"] = False

    reg = ~dfp["is_tiebreak"]
    p1 = dfp.loc[reg, "point_score_team_1"].astype(str)
    p2 = dfp.loc[reg, "point_score_team_2"].astype(str)

    gp_t1 = (p1 == "A") | ((p1 == "40") & (p2.isin(["0", "15", "30"])))
    gp_t2 = (p2 == "A") | ((p2 == "40") & (p1.isin(["0", "15", "30"])))

    set_can_end_t1 = ((g1 == 5) & (g2 <= 4)) | ((g1 == 6) & (g2 == 5))
    set_can_end_t2 = ((g2 == 5) & (g1 <= 4)) | ((g2 == 6) & (g1 == 5))

    dfp.loc[reg, "sp_for_t1"] = gp_t1.values & set_can_end_t1.loc[reg].fillna(False).values
    dfp.loc[reg, "sp_for_t2"] = gp_t2.values & set_can_end_t2.loc[reg].fillna(False).values

    tb = dfp["is_tiebreak"]
    tb_p1 = pd.to_numeric(dfp.loc[tb, "point_score_team_1"], errors="coerce")
    tb_p2 = pd.to_numeric(dfp.loc[tb, "point_score_team_2"], errors="coerce")

    dfp.loc[tb, "sp_for_t1"] = (((tb_p1 + 1) >= 7) & (((tb_p1 + 1) - tb_p2) >= 2)).fillna(False).values
    dfp.loc[tb, "sp_for_t2"] = (((tb_p2 + 1) >= 7) & (((tb_p2 + 1) - tb_p1) >= 2)).fillna(False).values

    return dfp


def get_set_points_saved(df_scores):
    dfp = add_set_point_flqgs(df_scores)

    # Faced = opponent has set point
    dfp["sp_faced_t1"] = dfp["sp_for_t2"]
    dfp["sp_faced_t2"] = dfp["sp_for_t1"]

    # Saved = faced AND set continues after this point
    dfp["has_next_point_in_set"] = (
        dfp.groupby(["match_id", "set_number"])["point_number"].shift(-1).notna()
    )

    dfp["sp_saved_t1"] = dfp["sp_faced_t1"] & dfp["has_next_point_in_set"]
    dfp["sp_saved_t2"] = dfp["sp_faced_t2"] & dfp["has_next_point_in_set"]

    out = (
        dfp.groupby("match_id", as_index=False)
           .agg(
               set_points_faced_team_1=("sp_faced_t1", "sum"),
               set_points_saved_team_1=("sp_saved_t1", "sum"),
               set_points_faced_team_2=("sp_faced_t2", "sum"),
               set_points_saved_team_2=("sp_saved_t2", "sum"),
           )
    )

    cols = [
        "set_points_faced_team_1", "set_points_saved_team_1",
        "set_points_faced_team_2", "set_points_saved_team_2",
    ]
    out[cols] = out[cols].astype(int)
    return out


def get_match_points_saved(df_scores, sets_to_win = 2):
    dfp = add_set_point_flqgs(df_scores)

    # --- match point = set point + already has (sets_to_win - 1) sets won ---
    s1 = pd.to_numeric(dfp["set_score_team_1"], errors="coerce").fillna(0).astype(int)
    s2 = pd.to_numeric(dfp["set_score_team_2"], errors="coerce").fillna(0).astype(int)

    dfp["mp_for_t1"] = dfp["sp_for_t1"] & (s1 >= (sets_to_win - 1))
    dfp["mp_for_t2"] = dfp["sp_for_t2"] & (s2 >= (sets_to_win - 1))

    # Faced = opponent has match point
    dfp["mp_faced_t1"] = dfp["mp_for_t2"]
    dfp["mp_faced_t2"] = dfp["mp_for_t1"]

    # Saved = faced AND match continues after this point
    dfp["has_next_point_in_match"] = (
        dfp.groupby(["match_id"])["point_number"].shift(-1).notna()
    )

    dfp["mp_saved_t1"] = dfp["mp_faced_t1"] & dfp["has_next_point_in_match"]
    dfp["mp_saved_t2"] = dfp["mp_faced_t2"] & dfp["has_next_point_in_match"]

    out = (
        dfp.groupby("match_id", as_index=False)
           .agg(
               match_points_faced_team_1=("mp_faced_t1", "sum"),
               match_points_saved_team_1=("mp_saved_t1", "sum"),
               match_points_faced_team_2=("mp_faced_t2", "sum"),
               match_points_saved_team_2=("mp_saved_t2", "sum"),
           )
    )

    cols = [
        "match_points_faced_team_1", "match_points_saved_team_1",
        "match_points_faced_team_2", "match_points_saved_team_2",
    ]
    out[cols] = out[cols].astype(int)
    return out


def build_stats(df_matches, df_scores):
    df_simple_totals = get_simple_totals(df_matches, df_scores)
    df_stats = df_simple_totals[["match_id", "total_points", "total_games"]].copy()

    df_deuce_stats = get_deuce_stats(df_scores)
    df_stats = df_stats.merge(df_deuce_stats, on="match_id", how="left")

    df_tiebreak_stats = get_tiebreak_stats(df_matches, df_scores)
    df_stats = df_stats.merge(df_tiebreak_stats, on="match_id", how="left")

    df_points_per_game_stats = get_points_per_game_stats(df_scores)
    df_stats = df_stats.merge(df_points_per_game_stats, on="match_id", how="left")

    df_game_points_saved = get_game_points_saved(df_scores)
    df_stats = df_stats.merge(df_game_points_saved, on="match_id", how="left")

    df_set_points_saved = get_set_points_saved(df_scores)
    df_stats = df_stats.merge(df_set_points_saved, on="match_id", how="left")

    df_match_points_saved = get_match_points_saved(df_scores)
    df_stats = df_stats.merge(df_match_points_saved, on="match_id", how="left")
    
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