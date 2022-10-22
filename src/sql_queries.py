from typing import Dict, Iterable

from pyparsing import Path

TABLES: Dict[str, Iterable[str]] = {
    "songplays": (
        "songplay_id int PRIMARY KEY",
        "start_time timestamp",
        "user_id int",
        "level text",
        "song_id text",
        "artist_id text",
        "session_id int",
        "location text",
        "user_agent text",
    ),
    "users": (
        "user_id int PRIMARY KEY",
        "first_name text",
        "last_name text",
        "gender text",
        "level text",
    ),
    "songs": (
        "song_id text PRIMARY KEY",
        "title text",
        "artist_id text",
        "year int",
        "duration numeric",
    ),
    "artists": (
        "artist_id text PRIMARY KEY",
        "name text",
        "location text",
        "latitude numeric",
        "longitude numeric",
    ),
    "time": (
        "start_time timestamp PRIMARY KEY",
        "hour int",
        "day int",
        "week int",
        "month int",
        "year int",
        "weekday int",
    ),
}


def get_drop_table_query(table_name: str) -> str:
    """Generate a DROP TABLE query given a table name.

    Args:
        table_name: table name.
    """
    return f"DROP TABLE IF EXISTS {table_name};"


def get_create_table_query(table_name: str, table_args: Iterable[str]) -> str:
    """Generate a CREATE TABLE query given a table name and a list of arguments.

    Args:
        table_name: table name.
        table_args: An iterable of strings including column names, data types and any
            other modfiers.
    """
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(table_args)});"


def get_insert_query(
    table_name: str,
    columns: Iterable[str],
    conflict_nothing_cols: Optional[Iterable[str]] = None,
) -> str:
    """Insert row into the given table.

    Args:
       table_name: table name.
       columns: Columns into which to insert row data.
       conflict_nothing_cols: if there is a conflict in these columns, do nothing.
    """
    return (
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
        f"({', '.join(['%s'] * len(columns))})"
        + (
            f" ON CONFLICT ({', '.join(conflict_nothing_cols)}) DO NOTHING"
            if conflict_nothing_cols
            else ""
        )
    )


def get_copy_json_query(table_name: str, columns: Iterable[str], ndjson_file: Path):
    return f"COPY {table_name} ({columns}) FROM {ndjson_file}"


def get_simple_select_query(
    columns: Iterable[str],
    table_name: str,
    where_columns: Optional[Dict[str, str]] = None,
    limit: Optional[int] = None,
) -> str:
    """Generate a simple select query.

    Args:
        columns: columns to select.
        table_name: table name to select columns from.
        where_columns: optionally, add a where clause. The keys and values in the
            dictionary will build an equality (i.e. WHERE key = value).
        limit: Optionally, add a query limit.

    """
    where_columns_str = (
        [f"{key} = {value}" for key, value in where_columns.items()]
        if where_columns
        else None
    )
    return (
        f"SELECT {', '.join(columns)} FROM {table_name} "
        + (f" WHERE {', '.join(where_columns_str)}" if where_columns_str else "")
        + (f" LIMIT {limit}" if limit else "")
    )


create_table_queries: Iterable[str] = [
    get_create_table_query(table_name, table_args)
    for table_name, table_args in TABLES.items()
]
drop_table_queries: Iterable[str] = [
    get_drop_table_query(table_name) for table_name in TABLES.keys()
]


# FIND SONGS
song_select = """
    SELECT songs.song_id, songs.artist_id
    FROM (songs JOIN artists ON songs.artist_id = artists.artist_id)
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s
"""
