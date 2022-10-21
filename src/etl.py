import logging
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import psycopg2
from tqdm.rich import tqdm

from sql_queries import *


def process_song_file(cur: Any, filepath: Path):
    """Open song file and insert relevant columns in database.

    Args:
        cur: database cursor.
        filepath: path to song file.
    """
    # 1. Load song metadata
    song_meta = pd.read_json(filepath, typ="series").rename(filepath.stem)

    # 2. Extract song records and insert
    song_columns = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = song_meta[song_columns]
    cur.execute(
        get_insert_query("songs", song_data.index, conflict_nothing_cols=("song_id",)),
        song_data.values,
    )

    # 3. Extract artist records and insert
    artist_columns = [
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude",
    ]
    artist_data = song_meta[artist_columns]
    cur.execute(
        get_insert_query(
            "artists",
            ("artist_id", "name", "location", "latitude", "longitude"),
            conflict_nothing_cols=("artist_id",),
        ),
        artist_data.values,
    )


def process_log_file(cur: Any, filepath: Path, load_copy: bool = False):
    """Open log file and insert relevant columns in database.

    Args:
        cur: database cursor.
        filepath: path to log file.
        load_copy: whether to use a COPY statement instead of INSERT.
    """
    # 1. Load log data
    log_data = pd.read_json(filepath, lines=True)

    # 2. Filter by NextSong action
    log_data = log_data[log_data["page"] == "NextSong"]

    # 3. Convert timestamp column to datetime
    log_data["ts"] = pd.to_datetime(log_data["ts"], unit="ms")

    # 4. Insert time data records
    column_labels = ("hour", "day", "week", "month", "year", "dayofweek")
    time_df = (
        pd.DataFrame(
            {
                ts: {col: getattr(ts, col) for col in column_labels}
                for ts in log_data["ts"]
            }
        )
        .transpose()
        .reset_index()
        .rename(columns={"index": "start_time", "dayofweek": "weekday"})
    )

    if load_copy:
        raise NotImplementedError
    else:
        for _, row in time_df.iterrows():
            cur.execute(
                get_insert_query(
                    "time", row.index, conflict_nothing_cols=("start_time",)
                ),
                row.values,
            )

    # 5. Load user table
    json_db_cols_map = {
        "userId": "user_id",
        "firstName": "first_name",
        "lastName": "last_name",
        "gender": "gender",
        "level": "level",
    }
    user_df = (
        log_data[json_db_cols_map.keys()]
        .rename(columns=json_db_cols_map)
        .drop_duplicates("user_id")
    )

    # 6. Insert user records
    if load_copy:
        raise NotImplementedError
    else:
        for _, row in user_df.iterrows():
            cur.execute(
                get_insert_query(
                    "users", row.index, conflict_nothing_cols=("user_id",)
                ),
                row.values,
            )

    # 7. Insert songplay records
    songplay_cols = (
        "songplay_id",
        "start_time",
        "user_id",
        "level",
        "song_id",
        "artist_id",
        "session_id",
        "location",
        "user_agent",
    )
    for index, row in log_data.iterrows():
        # 7.1 Get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # 7.2. Insert songplay record
        cur.execute(
            get_insert_query(
                "songplays", songplay_cols, conflict_nothing_cols=("songplay_id",)
            ),
            (
                index,
                row["ts"],
                row["userId"],
                row["level"],
                songid,
                artistid,
                row["sessionId"],
                row["location"],
                row["userAgent"],
            ),
        )


def process_data(
    cur: Any, conn: Any, filepath: Path, func: Callable, file_pattern: str = "**/*.json"
):
    # 1. Get all files matching extension from directory
    all_files = [f.resolve() for f in filepath.glob(file_pattern)]
    logging.info(f"{len(all_files)} files found in {filepath}")

    # 2. Iterate over files and process
    for datafile in (pbar := tqdm(all_files)):
        pbar.set_description(f"Loading {datafile.name}...")
        func(cur, datafile)
        conn.commit()


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(
        cur,
        conn,
        filepath=Path(__file__).resolve().parents[1].joinpath("data/song_data"),
        func=process_song_file,
    )
    process_data(
        cur,
        conn,
        filepath=Path(__file__).resolve().parents[1].joinpath("data/log_data"),
        func=process_log_file,
    )

    conn.close()


if __name__ == "__main__":
    main()
