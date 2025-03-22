import sqlalchemy
import modules.utils.lists as lists
import pickle
import pandas as pd
import os
from dotenv import load_dotenv
import time
import datetime
from typing import Union, List, Optional, Any
from sqlalchemy import (
    create_engine,
    text,
    MetaData,
    Engine,
    inspect,
    DateTime,
    Text,
    Integer,
    BLOB,
)
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import sessionmaker, mapped_column, declarative_base


# %% Database setup
def load_env_variables(path: str):
    """
    Load environment variables from the .env file
    at the given path.

    Parameters
    ----------
    path: str
        Path to the .env file
    """
    flag_env_vars = load_dotenv(dotenv_path=path)
    if not flag_env_vars:
        raise FileNotFoundError("Environment variables not found.")


def check_env_variables_loaded():
    """
    Check whether the environment variables are loaded or not.

    Returns
    -------
    bool:
        Are the environment variables loaded?
    """
    try:
        assert os.getenv("MARIADB_USER") is not None, "user env variable not found"
        return True
    except AssertionError:
        return False


def get_connection_string(database: str):
    """
    Get MySQL connection string to a given database

    Parameters
    ----------
    database: str
        Name of the database to get engine for

    Returns
    -------
    str:
        Connection string for the given database
    """
    assert isinstance(database, str), "database name must be a string"
    return (
        f'mysql+mysqlconnector://{os.getenv("MARIADB_USER")}:{os.getenv("MARIADB_PASSWORD")}'
        f'@127.0.0.1:{int(os.getenv("DB_PORT_HOST"))}/{database}?charset=utf8mb4&collation=utf8mb4_general_ci'
    )
    # return (
    #     f'mysql+mysqlconnector://{os.getenv("MARIADB_USER")}:{os.getenv("MARIADB_PASSWORD")}'
    #     f'@localhost:{int(os.getenv("DB_PORT_HOST"))}/{database}?charset=utf8mb4&collation=utf8mb4_general_ci'
    # )


def get_engine(database: str) -> Engine:
    """
    Get MySQL engine for a given database

    Parameters
    ----------
    database: str
        Name of the database to get engine for

    Returns
    -------
    Engine:
        SQL Engine for the given database
    """
    return create_engine(get_connection_string(database=database))


def get_default_dtypes_map():
    """
    The default data types mapping used for uploading pandas
    dataframes and dictionaries to SQL tables.
    """
    return {
        "int": "DOUBLE",
        "int64": "DOUBLE",
        "Int64": "DOUBLE",
        "float": "DOUBLE",
        "float64": "DOUBLE",
        "Float64": "DOUBLE",
        "NoneType": "TEXT",
        "object": "TEXT",
        "datetime": "DATETIME",
        "datetime64[ns]": "DATETIME",
        "bool": "BOOLEAN",
    }


def map_type(value):
    """
    Mapping of the input value type to the corresponding SQL type.

    Parameters
    ----------
    value: Any
        input value

    Returns
    -------
    SQL type from SQLAlchemy
    """
    if isinstance(value, int):
        return sqlalchemy.Integer
    elif isinstance(value, float):
        return sqlalchemy.Float
    elif isinstance(value, str):
        return sqlalchemy.Text
    elif isinstance(value, bool):
        return sqlalchemy.Boolean
    elif isinstance(value, datetime.datetime):
        return sqlalchemy.DateTime
    elif isinstance(value, bytes):
        return sqlalchemy.BLOB
    elif value is None:
        return sqlalchemy.Text
    else:
        raise TypeError(f"Unsupported data type for value: {value}")


def sql_column_strings(
    inpt: Union[pd.DataFrame, dict],
    exclude_cols: Union[None, list] = None,
    dtype_map: Union[None, dict] = None,
):
    """
    Create a list of strings of columns and data types to be used to
    create a table in MySQL database based on a pandas DataFrame
    or a dictionary.

    Parameters:
    -----------
    inpt: pd.DataFrame or dict
        The dataframe with columns to be processed or a dictionary
        with keys corresponding to column names.
    exclude_cols: list (optional)
        Columns to be excluded from the data type detection.
    dtype_map: dict (optional)
        Mapping of pandas data types to MySQL data types.
        If a mapping is not given, a reasonable mapping will be
        used, where in particular, integers are treated as doubles.

    Returns:
    --------
    list
        A list of strings in the format '`{column}` {dtype}'
    """
    # initialise
    cols_dtype = []
    # data types mapping from pandas to SQL
    if dtype_map is None:
        dtype_map = get_default_dtypes_map()
    else:
        assert isinstance(dtype_map, dict), "dtype_map must be a dict"
    # columns to exclude
    if exclude_cols is None:
        exclude_cols = []
    elif isinstance(exclude_cols, list):
        pass
    else:
        raise ValueError("exclude_cols must be a list or None")

    # process columns
    if isinstance(inpt, dict):
        # get the type of each value
        d = inpt.copy()
        for k, v in d.items():
            d[k] = type(v).__name__
        items = d.items()
    elif isinstance(inpt, pd.DataFrame):
        items = inpt.dtypes.items()
    else:
        raise ValueError("input must be a pandas DataFrame or a dict")
    # loop over items
    for col, dtype in items:
        if col in exclude_cols:
            continue
        else:
            mysql_dtype = dtype_map.get(
                str(dtype), "TEXT"
            )  # Default to TEXT if dtype not found in the mapping
            cols_dtype.append(f"`{col}` {mysql_dtype}")

    return cols_dtype


def check_database_available(engine: Engine, silent: bool = False):
    """
    Check whether the database specified in the engine
    is online and reachable.

    Parameters
    ----------
    engine: Engine
        SQLAlchemy engine
    silent: bool
        Print useful messages or not.

    Returns
    -------
    bool:
        Is the database available?
    """
    # 0: initialisation
    assert isinstance(engine, Engine), "engine must be an instance of Engine"

    # 1: try to connect
    try:
        # Try to connect and execute a simple query
        with engine.connect() as connection:
            connection.close()
            if not silent:
                print("Database is online.")
            return True
    except (OperationalError, ProgrammingError) as e:
        if not silent:
            print("Database is offline or unreachable.")
            print(f"Error: {e}")
        else:
            pass
        return False


# %% Predefined table classes for use with SQLAlchemy ORM
def create_model_class(
    table_name: str, column_definitions: dict, attributes: Union[None, dict] = None
):
    """
    Create an SQL table model object dynamically given a dictionary
    of column definitions, the table name and the attributes.

    Parameters
    ----------
    table_name: str
        Name of the table
    column_definitions: dict
        Dictionary of column definitions
    attributes: Union[None, dict]
        Optional dictionary of attributes

    Returns
    -------
    SQLAlchemy model class
    """
    # 0: set class attributes
    if attributes is None:
        attributes = {}
    else:
        attributes = {"__table_args__": attributes}
    attrs = {"__tablename__": table_name}
    attrs.update(attributes)

    # 1: Dynamically create columns
    for col_name, col_type in column_definitions.items():
        attrs[col_name] = col_type

    # 2: Create the SQL table model class and return
    return type("SQLTableModel", (declarative_base(),), attrs)


def pickle_single_sql_class(
    table_name: str, date_col: Union[None, str], symbol_col: str
):
    """
    SQLAlchemy ORM class for a single pickle.
    """
    # 0: assertions
    assert isinstance(table_name, str), "table_name must be a string"
    assert isinstance(symbol_col, str), "symbol_col must be a string"
    assert date_col is None or isinstance(
        date_col, str
    ), "date_col must be a string or None"

    # 1: prepare dictionary to create the table object from
    if date_col is None:
        d = {
            "id": mapped_column(Integer, primary_key=True),
            symbol_col: mapped_column(
                Text, primary_key=False, unique=True, nullable=False
            ),
            "data": mapped_column(BLOB, nullable=False),
        }
    else:
        d = {
            "id": mapped_column(Integer, primary_key=True),
            date_col: mapped_column(DateTime),
            symbol_col: mapped_column(
                Text, primary_key=False, unique=True, nullable=False
            ),
            "data": mapped_column(BLOB, nullable=False),
        }

    # 2: return SQLAlchemy ORM class
    return create_model_class(
        table_name=table_name,
        column_definitions=d,
        attributes={"extend_existing": True},
    )


# %% Interacting with the database
def check_tables_exist(
    engine: Engine,
    tables: Union[str, list],
) -> bool:
    """
    Check if given tables exist in the database.
    """
    # initialisation
    if isinstance(tables, str):
        tables = [tables]
    elif isinstance(tables, list):
        pass
    else:
        raise TypeError("invalid input for tables")
    # get tables in the database associated with the engine
    md = MetaData()
    md.reflect(bind=engine)
    db_tables = list(md.tables)
    # return bool (return False if empty)
    if len(db_tables) == 0:
        return False
    else:
        return lists.is_sublist(tables, db_tables)


def check_nonzero_rows(
    engine: Engine,
    mandatory_tables: list,
    filter_col: Union[None, str] = None,
    filter_val: Union[None, str] = None,
):
    """
    Check whether the filter value (e.g. symbol) exists in the database
    given a list of mandatory sheets, for which there must be a non-zero
    number of rows for.

    Parameters
    ----------
    engine: Engine
        SQLAlchemy engine
    mandatory_tables: list
        The list of mandatory tables to check for non-zero rows.
    filter_col: str or None
        Name of the filter column. If not given, all rows of the
        tables will be checked.
    filter_val: str or None
        Name of the value to filter. Only used if filter_col is given.

    Returns
    -------
    bool:
        result of the check for non-zero rows
    """
    # 0: initialisation
    # 0.1: assertions on inputs
    assert isinstance(engine, Engine), "engine must be an instance of Engine"
    assert isinstance(mandatory_tables, list), "mandatory_tables must be a list"
    assert filter_col is None or isinstance(
        filter_col, str
    ), "filter_col must be a string or None"
    assert filter_val is None or isinstance(
        filter_val, str
    ), "filter_val must be a string or None"
    # 0.2: assertions on the combination of input variables
    if filter_val is not None:
        assert (
            filter_col is not None
        ), "filter_col must be given when filter_val is given"
    # 0.3: initialise variables
    result_check = True

    # 1: check tables
    with engine.connect() as connection:
        for table in mandatory_tables:
            # parse query
            if filter_val is not None:
                # if filtering is given
                query_check = f"SELECT COUNT(*) FROM `{table}` WHERE `{filter_col}` = '{filter_val}';"
            else:
                # if filtering is not given
                query_check = f"SELECT COUNT(*) FROM `{table}`;"
            # execute the query; check whether the number of rows is larger than zero
            result_proxy_check = connection.execute(text(query_check))
            result_check *= result_proxy_check.fetchall()[0][0] > 0
    return bool(result_check)


def create_table(
    engine: Engine,
    table_name: str,
    categorical_cols: Union[None, list],
    numeric_cols: Union[None, list],
    date_col: Union[None, str],
    symbol_col: Union[None, str],
    set_index_date_col: bool = False,
    set_index_symbol_col: bool = False,
    df: Union[None, pd.DataFrame] = None,
    dtype_map: Union[None, dict] = None,
):
    """
    Create a table in the database with type "double" for all numeric columns, and
    otherwise text for all categorical columns.

    If a dataframe is given, then only the subset of categorical and numeric columns
    present will be identified accordingly, and the remaining not directly specified
    columns will be autodetect.

    Parameters:
    -----------
    engine: Engine
        SQLAlchemy engine
    table_name: str
        Name of the table to upload to. It will be created if it
        does not exist.
    categorical_cols: list or None
        The list of columns to be cast as categorical columns, i.e.
        as 'text' type in SQL. If None, there will only automatic
        detection of types according to the dtype_map.
    numeric_cols: list or None
        The list of columns to be cast as numerical columns, i.e.
        as 'text' type in SQL. If None, there will only automatic
        detection of types according to the dtype_map.
    date_col: str
        Name of date column, if it exists. If it does not exist,
        the input of update_latest will be ignored.
    symbol_col: str
        Name of the symbol column, if it exists. If it does not
        exist, symbol cannot be given.
    set_index_date_col: bool (optional)
        Add date_col to the list of index columns for the table.
    set_index_symbol_col: bool (optional)
        Add symbol_col to the list of index columns for the table.
    df: pd.DataFrame (optional)
        The dataframe with columns to be processed.
    dtype_map: dict (optional)
        Mapping of pandas data types to MySQL data types.
        If a mapping is not given, a reasonable mapping will be
        used, where in particular, integers are treated as doubles.
    """
    # 0: initialise
    # 0.1: assertion on inputs
    assert isinstance(engine, Engine), "engine must be an instance of Engine"
    assert (
        isinstance(table_name, str) and len(table_name) > 0
    ), "table_name must be a string of at least 1 character"
    assert categorical_cols is None or isinstance(
        categorical_cols, list
    ), "categorical_cols must be a list or None"
    assert numeric_cols is None or isinstance(
        numeric_cols, list
    ), "numeric_cols must be a list or None"
    assert date_col is None or isinstance(
        date_col, str
    ), "date_col must be a string or None"
    assert symbol_col is None or isinstance(
        symbol_col, str
    ), "symbol_col must be a string or None"
    assert isinstance(set_index_date_col, bool), "set_index_date_col must be a boolean"
    assert isinstance(
        set_index_symbol_col, bool
    ), "set_index_symbol_col must be a boolean"
    assert df is None or isinstance(df, pd.DataFrame), "df must be a data frame or None"
    assert dtype_map is None or isinstance(
        dtype_map, dict
    ), "dtype_map must be a dict or None"
    # 0.2: assertion on the combination of inputs
    if df is None:
        assert (
            categorical_cols is not None or numeric_cols is not None
        ), "If a dataframe is not given, numerical and/or categorical columns must be specified"
    # 0.3: initialise variables
    columns = []

    # 1: process columns explicitly given
    # 1.1: Add date_col and symbol_col
    cols_symbols_date = []
    if symbol_col is not None:
        columns.append(f"`{symbol_col}` TEXT")
        cols_symbols_date.append(symbol_col)
    if date_col is not None:
        columns.append(f"`{date_col}` DATETIME")
        cols_symbols_date.append(date_col)
    # 1.2: Add categorical columns
    if categorical_cols is not None:
        if df is None:
            pass
        if df is not None:
            categorical_cols = lists.intersection(df.columns, categorical_cols)
        for col in categorical_cols:
            columns.append(f"`{col}` TEXT")
    else:
        categorical_cols = []
    # 1.3: Add numerical columns
    if numeric_cols is not None:
        if df is None:
            pass
        if df is not None:
            numeric_cols = lists.intersection(df.columns, numeric_cols)
        for col in numeric_cols:
            columns.append(f"`{col}` DOUBLE")
    else:
        numeric_cols = []
    # 1.4: check that there is no overlap
    cols_check_intersect = lists.intersection(categorical_cols, numeric_cols)
    assert (
        len(cols_check_intersect) == 0
    ), f"There are common columns in categorical and numeric columns: {cols_check_intersect}"

    # 2: add auto-detected columns
    if df is not None:
        cols_auto = sql_column_strings(
            inpt=df,
            exclude_cols=cols_symbols_date + categorical_cols + numeric_cols,
            dtype_map=dtype_map,
        )
        columns = lists.union(columns, cols_auto)
    else:
        pass

    # 3: create table, if it does not already exist
    create_table_sql = (
        f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns)}"
    )
    # set indices, adding to query string:
    if set_index_date_col and date_col is not None:
        create_table_sql += f", INDEX idx_key1 (`{date_col}`)"
    if set_index_symbol_col and symbol_col is not None:
        create_table_sql += f", INDEX idx_key2 (`{symbol_col}`)"
    # final string addition to query
    create_table_sql += ");"
    # Execute the table creation SQL using SQLAlchemy engine
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))


def create_table_dict(
    d: dict, table_name: str, symbol_col: Union[None, str] = None, **kwargs
):
    """
    Dynamically creates a table with the given columns with
    a symbol column as the primary key from a dictionary
    used as template.

    There will be an auto-incremented id column added.

    Parameters
    ----------
    d: dict
        Dictionary to create table from, each key will correspond
        to a column.
    table_name : str
        Name of the table to create.
    symbol_col : str or None
        If given, this will be the symbol column name, which will
        be added to the columns of the table.
    **kwargs : dict
        Arguments to be passed to mapped_column.

    Returns
    -------
    A dynamic table class.
    """
    # 0: assertions
    assert isinstance(d, dict), "d must be a dictionary"
    assert isinstance(table_name, str), "table_name must be a string"
    assert symbol_col is None or isinstance(
        symbol_col, str
    ), "symbol_col must be a string or None"

    # 1: create the base object, map of data types in dictionary
    # 1.1: create the base object
    Base = declarative_base()
    # 1.2: map data types
    column_definitions = {}
    for key, val in d.items():
        column_definitions[key] = map_type(val)

    # 2: Define attributes to add to table class
    # 2.1: table name and symbol
    attr = {
        "__tablename__": table_name,
        "id": mapped_column(Integer, primary_key=True),
    }
    if symbol_col is not None:
        attr[symbol_col] = (
            mapped_column(Text, primary_key=False, unique=True, nullable=False),
        )
    # 2.2: add remaining columns
    attr.update(
        {
            name: mapped_column(col_type, **kwargs)
            for name, col_type in column_definitions.items()
        }
    )

    # 3: create a class that inherits from Base with the given table name and columns
    table_class = type(table_name, (Base,), attr)

    return table_class


def get_union_all_columns(
    engine: Engine,
    tables: Union[None, list] = None,
    exclude_tables: Union[None, list] = None,
    check_data_available: bool = False,
    filter_col: Union[None, str] = None,
    filter_val: Union[None, str] = None,
):
    """
    Get the union of all (unique) columns in the tables specified in the engine.
    If no list of tables is provided, all available tables are used.

    Additional check of the available data when filtering a given column by
    value
    """
    with engine.connect() as connection:
        # get tables
        if tables is None:
            md = MetaData()
            md.reflect(bind=engine)
            tables = list(md.tables)
        elif isinstance(tables, list):
            pass
        else:
            raise ValueError("tables must be a list or None")
        # remove tables to exclude
        if exclude_tables is None:
            pass
        elif isinstance(exclude_tables, list):
            tables = lists.difference(tables, exclude_tables)
        else:
            raise ValueError("tables must be a list or None")
        # check if the filtered table is empty, if not, get columns in each table
        columns = []
        for table in tables:
            if check_data_available:
                assert isinstance(filter_col, str), "filter_col must be a string"
                assert isinstance(filter_val, str), "filter_val must be a string"
                # parse optional query
                query_check = f"""SELECT COUNT(*) FROM `{table}`
                            WHERE {filter_col} = '{filter_val}'
                            """
                # execute the query; check whether the number of rows is larger than zero
                result_proxy_check = connection.execute(text(query_check))
                result_check = result_proxy_check.fetchall()
                result_check = result_check[0][0] > 0
            else:
                result_check = True

            if result_check:
                # parse query 0
                query = f"""SELECT COLUMN_NAME
                            FROM information_schema.columns
                            WHERE table_name = '{table}'
                            """
                # execute query
                result_proxy = connection.execute(text(query))
                # fetch the result
                result = result_proxy.fetchall()
                result = [x[0] for x in result]
                columns = lists.union(columns, result)
            else:
                continue

    return columns


# upload a dataframe for a symbol
def upload_df(
    engine: Engine,
    symbol: Union[str, None],
    df: Union[pd.DataFrame, None],
    table_name: str,
    categorical_cols: Union[None, list],
    numeric_cols: Union[None, list],
    date_col: Union[str, None],
    symbol_col: Union[str, None],
    drop_index_cols: bool = False,
    set_index_date_col: bool = False,
    set_index_symbol_col: bool = False,
    update_latest: bool = False,
    alter_table: bool = False,
    columns_to_drop: Union[list, None] = None,
    dtype_map: Union[None, dict] = None,
    keep_keys_nans: bool = True,
    raise_exception_keys_nans: bool = False,
    raise_exception_overwrite_symbol_col: bool = True,
    silent: bool = False,
):
    """
    This utility function creates a table in MySQL database based on
    a pandas DataFrame. There are several checks, and options for the
    upload, like ensuring that there are no NaNs in the designated
    index columns, and that symbol and date indices are treated as
    expected.

    For data uploaded to a new table, the table will be created with
    categorical columns (parsed as text) and numerical columns (parsed
    as doubles) as set in create_table().

    For updating existing data, the function ensures that only new dates
    are appended (and optionally update the newest row).

    Optionally, there are additional operations that can be carried out:
        - Set date and/or symbol columns as indices in the table.
        - Update the newest row by overwriting it with the data
        from the data frame if it already exists.
        - Alter the table by adding columns present in the
        dataframe that are not already in the table.
        - Drop columns from dataframe before uploading it to
        the database. Old index columns can also be dropped.
        - Set custom mapping of pandas datatypes to MySQL data types.
        - Raise exceptions for NaNs in the key columns (only if they
        are explicitly set), or if a symbol column exists and is
        overwritten with a new symbol value.

    Parameters:
    -----------
    engine: Engine
        SQLAlchemy engine
    symbol: Union[str, None]
        The symbol name to upload data for, if available. When given,
        the values in the symbol column are overwritten with the value
        "{symbol}".
    df: pd.DataFrame
        The dataframe with columns to be processed.
    table_name: str
        Name of the table to upload to. It will be created if it
        does not exist.
    categorical_cols: list or None
        The list of columns to be cast as categorical columns, i.e.
        as 'text' type in SQL. If None, there will only automatic
        detection of types according to the dtype_map.
    numeric_cols: list or None
        The list of columns to be cast as numerical columns, i.e.
        as 'text' type in SQL. If None, there will only automatic
        detection of types according to the dtype_map.
    date_col: str
        Name of date column, if it exists. If it does not exist,
        the input of update_latest will be ignored.
    symbol_col: str
        Name of the symbol column, if it exists. If it does not
        exist, symbol cannot be given.
    drop_index_cols: bool
        Drop the old index columns before writing the table to the
        database. If not, the index column(s) will be kept.
    set_index_date_col: bool (optional)
        Add date_col to the list of index columns for the table.
        If True and keep_keys_nans is False, then any nan values in
        the date column will be dropped.
    set_index_symbol_col: bool (optional)
        Add symbol_col to the list of index columns for the table.
        If True and keep_keys_nans is False, then any nan values in
        the symbol column will be dropped.
    update_latest: bool (optional)
        If a date column is given, setting this input to True
        will overwrite the newest row in the data with the
        corresponding row in the data frame.
    alter_table: bool (optional)
        If True, the table in the database will be altered by adding
        columns in the data frame that are not in the table.
    columns_to_drop: list (optional)
        Columns to be dropped before uploading to the database.
    dtype_map: dict (optional)
        Mapping of pandas data types to MySQL data types.
        If a mapping is not given, a reasonable mapping will be
        used, where in particular, integers are treated as doubles.
    keep_keys_nans: bool (optional)
        If True, and set_index_date_col or set_index_symbol_col are
        also True, and the columns are given, then potential nans
        in the rows will be kept.
        If False, the rows will be discarded before uploading.
    raise_exception_keys_nans: bool (optional)
        If True, and set_index_date_col or set_index_symbol_col are
        also True, and the columns are given, then the columns of the
        input df will be checked for missing values: If there are any,
        a ValueError will be raised. Otherwise, just a warning message
        will be printed.
    raise_exception_overwrite_symbol_col: bool (optional)
        If True, and set_index_date_col or set_index_symbol_col are
    silent: bool (optional)
        Silence the printing of informative messages.
    """
    # 0: initialise
    # 0.1: assertions
    assert isinstance(engine, Engine), "engine must be an SQL engine"
    assert symbol is None or isinstance(symbol, str), "symbol must be a string or None"
    if df is None:
        if not silent:
            print(f"{symbol}: No data to upload for {table_name}")
        return
    elif isinstance(df, pd.DataFrame):
        if df.empty:
            if not silent:
                print(f"{symbol}: No data to upload for {table_name}")
            return
        else:
            pass
    else:
        raise ValueError("df must be a pandas dataframe or None")
    assert isinstance(table_name, str), "table_name must be a string"
    assert date_col is None or isinstance(
        date_col, str
    ), "date_col must be a string or None"
    assert symbol_col is None or isinstance(
        symbol_col, str
    ), "symbol_col must be a string or None"
    assert isinstance(update_latest, bool), "update_latest must be a boolean"
    assert isinstance(alter_table, bool), "alter_table must be a boolean"
    assert columns_to_drop is None or isinstance(
        columns_to_drop, list
    ), "columns_to_drop must be a list or None"
    assert isinstance(silent, bool), "silent must be a boolean"
    # 0.2: optional drop of index columns
    df = df.reset_index(drop=drop_index_cols)
    # 0.3: checks on the combination of inputs
    if symbol is not None:
        assert isinstance(
            symbol_col, str
        ), "symbol_col must be given when a symbol is given"
    if date_col is not None:
        assert (
            date_col in df.columns
        ), f"date column not found in input data frame: {date_col}"
    # 0.4: initial messages
    if not silent:
        time_start = time.time()
        if symbol is not None:
            print(f"{symbol}: Uploading {table_name} to database... ", end="")
        else:
            print(f"Uploading {table_name} to database... ", end="")
    # 0.5: drop columns requested to be dropped (if they exist)
    if columns_to_drop is not None:
        df = df.drop(columns=columns_to_drop, errors="ignore")
    # 0.6: check if there are nans in the date columns (if given), filter nans out
    if date_col is not None and set_index_date_col:
        if df[date_col].isna().sum() > 0:
            # print warning or raise exception
            if raise_exception_keys_nans:
                raise ValueError("There are nans in date_col")
            elif not silent:
                print("Warning: There are nans in date_col")
            else:
                pass
            # optional discards rows
            if not keep_keys_nans:
                df = df.dropna(subset=[date_col])
    # 0.7: if the symbol column is given, check that if it already exists.
    # check if there are nans in the symbol columns (if given), filter nans out, create column is needed
    if symbol_col is not None:
        if symbol_col in df.columns:
            # check if symbol value is given
            if symbol is not None:
                # inform that the column is going to be overwritten or raise exception
                if not raise_exception_overwrite_symbol_col:
                    if not silent:
                        print(
                            f"Warning: symbol_col={symbol_col} already exists in the dataframe, overwriting..."
                        )
                    df[symbol_col] = symbol
                else:
                    raise ValueError(
                        f"symbol_col={symbol_col} already exists in the dataframe"
                    )
            # checks on the symbol column
            if set_index_symbol_col and df[symbol_col].isna().sum() > 0:
                # print warning or raise exception
                if raise_exception_keys_nans:
                    raise ValueError("There are nans in symbol_col")
                elif not silent:
                    print("Warning: There are nans in symbol_col")
                else:
                    pass
                # optional discards rows
                if not keep_keys_nans:
                    df = df.dropna(subset=[symbol_col])
        elif symbol is not None:
            df[symbol_col] = symbol
        else:
            pass

    # 1: checking whether the table already exists:
    md = MetaData()
    md.reflect(bind=engine)
    if table_name in md.tables:
        # 1A: table already exists, optional check whether additional columns needs to be added
        if alter_table:
            # get existing columns
            inspector = inspect(engine)
            existing_cols = [col["name"] for col in inspector.get_columns(table_name)]
            # get strings for dtypes for missing columns
            list_dtypes = sql_column_strings(
                inpt=df, exclude_cols=existing_cols, dtype_map=dtype_map
            )
            # check if there are columns that need to be added
            if len(list_dtypes) > 0:
                # alter statement
                alter_stmt = (
                    f"ALTER TABLE `{table_name}` ADD COLUMN ({', '.join(list_dtypes)});"
                )
                # execute the query
                with engine.connect() as conn:
                    conn.execute(text(alter_stmt))
                if not silent:
                    print(
                        f"{table_name}: added {len(list_dtypes)} columns successfully"
                    )
            else:
                pass
        else:
            pass
    else:
        # 1B: Create table, reindex and send data to server immediately
        # 1B.1: create table with appropriate datatypes
        create_table(
            engine=engine,
            table_name=table_name,
            categorical_cols=categorical_cols,
            numeric_cols=numeric_cols,
            date_col=date_col,
            symbol_col=symbol_col,
            set_index_date_col=set_index_date_col,
            set_index_symbol_col=set_index_symbol_col,
            df=df,
            dtype_map=dtype_map,
        )
        # 1B.2: manipulate dataframe and upload to server
        # 1B.2.1: get key columns; depending on what is set and symbol availability
        key_cols = []
        if date_col is not None and set_index_symbol_col:
            key_cols.append(date_col)
        if symbol_col is not None and set_index_symbol_col:
            key_cols.append(symbol_col)
        # set indices and flag whether to upload index column as well
        if len(key_cols) > 0:
            df = df.set_index(key_cols)
            flag_index = True
        else:
            flag_index = False
        # 1B.2.2: upload to server
        with engine.connect() as conn:
            df.to_sql(name=table_name, con=conn, if_exists="append", index=flag_index)
            conn.close()
        if not silent:
            time_end = time.time()
            print(f"Success (new table created)! (time: {time_end - time_start:.4g}s)")
        return

    # 2: If table exists: get the latest date that appears for the symbol (incl. duplicates)
    # 2.1: get the newest date in data
    with engine.connect() as conn:
        if date_col is not None and set_index_date_col:
            if symbol_col is not None and set_index_symbol_col and symbol is not None:
                result = conn.execute(
                    text(
                        f"SELECT `{date_col}` FROM `{table_name}` "
                        f"WHERE `{date_col}`=(SELECT MAX(`{date_col}`)"
                        f" FROM `{table_name}` "
                        f" WHERE `{symbol_col}` = '{symbol}') "
                        f"AND `{symbol_col}` = '{symbol}';"
                    )
                ).fetchall()
                # assertion: assumption is that there is one observation for each time-stamp
                assert (
                    len(result) <= 1
                ), f"There are duplicate dates; check table! (symbol: {symbol}, result: {result})"
            else:
                result = conn.execute(
                    text(
                        f"SELECT `{date_col}` FROM `{table_name}` "
                        f"WHERE `{date_col}`=(SELECT MAX(`{date_col}`)"
                        f" FROM `{table_name}`)"
                    )
                ).fetchall()
        else:
            result = []
    # 2.2: checks on the result from the server
    if len(result) == 0:
        db_latest_date = None
    else:
        assert isinstance(
            result[0][0], datetime.datetime
        ), "date column is not a datetime object; check table"
        db_latest_date = result[0][0]

    # 3: optional drop the latest row, so that it can be updated, if necessary
    if update_latest and not (db_latest_date is None):
        if date_col is not None and set_index_date_col:
            # first check that the data in df is contains db_latest_date; otherwise no update is to be done
            min_date_df = df[date_col].min()
            max_date_df = df[date_col].max()
            if min_date_df <= db_latest_date <= max_date_df:
                with engine.connect() as conn:
                    # delete latest observation if applicable, depending on whether
                    # the symbol column is also available and set as index
                    if (
                        symbol_col is not None
                        and set_index_symbol_col
                        and symbol is not None
                    ):
                        conn.execute(
                            text(
                                f"DELETE FROM `{table_name}` "
                                f"WHERE `{date_col}` = '{db_latest_date}' "
                                f"AND `{symbol_col}` = '{symbol}';"
                            )
                        )
                    else:
                        conn.execute(
                            text(
                                f"DELETE FROM `{table_name}` "
                                f"WHERE `{date_col}` = '{db_latest_date}' "
                            )
                        )
                    conn.commit()
            else:
                pass
    else:
        pass

    # 4: preprocessing of data to send to server
    # 4.1: filter dataframe
    # 4.1A: no date found or date not set as an index
    if db_latest_date is None:
        # optional discard of nans in key columns when there is no data found in the table
        if keep_keys_nans:
            pass
        else:
            if (date_col is not None and set_index_date_col) and (
                symbol_col is not None and set_index_symbol_col
            ):
                df = df[~(df[date_col].isna() | df[symbol_col].isna())]
            elif date_col is not None and set_index_date_col:
                df = df[~df[date_col].isna()]
            elif symbol_col is not None and set_index_symbol_col:
                df = df[~df[symbol_col].isna()]
            else:
                # date and symbol columns are not given and/or are not set as indices
                pass
            # check if data frame now is empty
            if df.empty:
                # Dataframe can still be empty, if the only remaining rows contained nans for
                # the designated key columns
                if not silent:
                    time_end = time.time()
                    print(
                        f"Success (no new data that is not nan in the key columns)! "
                        f"(time: {time_end - time_start:.4g}s)"
                    )
                return
    # 4.1B: data found and updating is needed
    elif update_latest:
        # optional discard of nans in key columns while keeping only newest data
        if keep_keys_nans:
            if (date_col is not None and set_index_date_col) and (
                symbol_col is not None and set_index_symbol_col
            ):
                df = df[
                    (df[date_col] >= db_latest_date)
                    | df[date_col].isna()
                    | df[symbol_col].isna()
                ]
            elif date_col is not None and set_index_date_col:
                df = df[(df[date_col] >= db_latest_date) | df[date_col].isna()]
            elif symbol_col is not None and set_index_symbol_col:
                df = df[(df[symbol_col] >= db_latest_date) | df[symbol_col].isna()]
            else:
                # date and symbol columns are not given and/or are not set as indices
                pass
        else:
            # nans will be discarded
            df = df[df[date_col] >= db_latest_date]
        # check if data frame now is empty
        if df.empty:
            # Dataframe can still be empty, if the newest data in df is from before db_latest_date
            if not silent:
                time_end = time.time()
                print(f"Success (no new data)! (time: {time_end - time_start:.4g}s)")
            return
    # 4.1B: data found and no updating is requested
    else:
        if keep_keys_nans:
            if (date_col is not None and set_index_date_col) and (
                symbol_col is not None and set_index_symbol_col
            ):
                df = df[
                    (df[date_col] > db_latest_date)
                    | df[date_col].isna()
                    | df[symbol_col].isna()
                ]
            elif date_col is not None and set_index_date_col:
                df = df[(df[date_col] > db_latest_date) | df[date_col].isna()]
            elif symbol_col is not None and set_index_symbol_col:
                df = df[(df[symbol_col] > db_latest_date) | df[symbol_col].isna()]
            else:
                # date and symbol columns are not given and/or are not set as indices
                pass
        else:
            # nans will be discarded
            df = df[df[date_col] > db_latest_date]
        if df.empty:
            if not silent:
                time_end = time.time()
                print(f"Success (no new data)! (time: {time_end - time_start:.4g}s)")
            return
    # 4.2: get key columns; depending on what is set and symbol availability
    key_cols = []
    if date_col is not None and set_index_symbol_col:
        key_cols.append(date_col)
    if symbol_col is not None and set_index_symbol_col:
        key_cols.append(symbol_col)
    # set indices and flag whether to upload index column as well
    if len(key_cols) > 0:
        df = df.set_index(key_cols)
        flag_index = True
    else:
        flag_index = False
    # 4.3: upload to server
    with engine.connect() as conn:
        df.to_sql(name=table_name, con=conn, if_exists="append", index=flag_index)
        conn.close()
    if not silent:
        time_end = time.time()
        print(f"Success (new rows: {len(df)})! (time: {time_end - time_start:.4g}s)")


def get_existing_rows(df, table_df):
    """
    Get the rows in the input df that already exist in the table_df.

    TODO:
        For the cases where there are nans to be kept we should check that there
            are not identical rows we are writing
        Can restrict ourselves to only nan rows (for relevant indices) to check
    """
    # Drop the index if it exists in both DataFrames (if any)
    table_df = table_df.reset_index(drop=True)
    df = df.reset_index(drop=True)

    # Step 4: Compare the two DataFrames to find identical rows
    # Remove any extra columns that shouldn't be considered in the comparison
    common_columns = table_df.columns.intersection(df.columns)
    table_df_filtered = table_df[common_columns]
    df_filtered = df[common_columns]

    # Check for identical rows
    matching_rows = df_filtered[
        df_filtered.apply(tuple, axis=1).isin(table_df_filtered.apply(tuple, axis=1))
    ]

    # Step 5: Output matching rows
    print(f"Number of identical rows found: {len(matching_rows)}")
    print(matching_rows)


def helper_upload_dict(
    engine: Engine,
    d: dict,
    table_name: str,
    symbol_col: Union[None, str] = None,
):
    """
    Helper to upload a dictionary to an SQL database using a given
    connection.
    """
    # 0: Create table object
    # 0.1: Create a base class for declarative class definitions
    DTable = create_table_dict(d=d, table_name=table_name, symbol_col=symbol_col)
    # 0.2: create table
    DTable.metadata.create_all(engine)

    # 1: create session and upload dict
    # 1.1: create session
    Session = sessionmaker(bind=engine)
    session = Session()
    # 1.2: add data and commit
    new_record = DTable(**d)
    session.add(new_record)
    session.commit()


# upload a dictionary
def upload_dict(
    engine: Engine,
    symbol: Union[str, None],
    d: Union[dict, None],
    table_name: str,
    symbol_col: Union[str, None],
    alter_table: bool = False,
    dtype_map: Union[None, dict] = None,
    silent: bool = False,
):
    """
    Upload a dictionary to the database, where the keys will be columns.
    Data will always be updated.

    Parameters:
    -----------
    engine: Engine
        SQLAlchemy engine
    symbol: Union[str, None]
        The symbol name to upload data for, if available. When given,
        the values in the symbol column are overwritten with the value
        "{symbol}".
    d: dict
        The dictionary to be processed.
    table_name: str
        Name of the table to upload to. It will be created if it
        does not exist.
    symbol_col: str
        Name of the symbol column, if it exists. If it does not
        exist, symbol cannot be given.
    alter_table: bool (optional)
        If True, the table in the database will be altered by adding
        columns in the data frame that are not in the table.
    dtype_map: dict (optional)
        Mapping of pandas data types to MySQL data types.
        If a mapping is not given, a reasonable mapping will be
        used, where in particular, integers are treated as doubles.
    silent: bool (optional)
        Silence the printing of informative messages.
    """
    # 0: initialisation
    # 0.1: assertions
    assert isinstance(d, dict) or d is None, "d must be a dictionary or None"
    assert isinstance(engine, Engine), "engine must be an SQL engine"
    assert symbol is None or isinstance(symbol, str), "symbol must be a string or None"
    if d is None:
        if not silent:
            print(f"{symbol}: No data to upload for {table_name}")
        return
    elif isinstance(d, dict):
        if len(d) == 0:
            if not silent:
                print(f"{symbol}: No data to upload for {table_name}")
            return
        else:
            pass
    else:
        raise ValueError("d must be a dict or None")
    assert isinstance(table_name, str), "table_name must be a string"
    assert symbol_col is None or isinstance(
        symbol_col, str
    ), "symbol_col must be a string or None"
    assert isinstance(alter_table, bool), "alter_table must be a boolean"
    assert isinstance(silent, bool), "silent must be a boolean"

    # 1: initial messages and set symbol key
    # decouple dictionary from input
    d = d.copy()
    time_start = time.time()
    if not symbol_col is None and not symbol is None:
        if not silent:
            print(f"{symbol}: Uploading {table_name} to database... ", end="")
        d[symbol_col] = symbol
    elif symbol_col is None and not symbol is None:
        raise ValueError(f"{symbol}: symbol given, but no column specified")
    elif not symbol_col is None:
        if symbol_col in d:
            if not silent:
                print(f"Uploading {table_name} to database... ", end="")
        else:
            raise ValueError(f"{symbol}: symbol given, but no column specified")
    else:
        if not silent:
            print(f"Uploading {table_name} to database... ", end="")

    # 2: checking whether the table already exists:
    md = MetaData()
    md.reflect(bind=engine)
    # 2A: table already exists
    if table_name in md.tables:
        # 2A.1: table already exists, optional check whether additional columns need to be added
        if alter_table:
            # get existing columns
            inspector = inspect(engine)
            existing_cols = [col["name"] for col in inspector.get_columns(table_name)]
            # get strings for dtypes for missing columns
            list_sql_col_strings = sql_column_strings(
                inpt=d, exclude_cols=existing_cols, dtype_map=dtype_map
            )
            # check if there are columns that need to be added
            if len(list_sql_col_strings) > 0:
                # alter statement
                alter_stmt = f"ALTER TABLE `{table_name}` ADD COLUMN ({', '.join(list_sql_col_strings)});"
                # execute the query
                with engine.connect() as conn:
                    conn.execute(text(alter_stmt))
                if not silent:
                    print(
                        f"{table_name}: added {len(list_sql_col_strings)} columns successfully ",
                        end="",
                    )
            else:
                pass
        else:
            pass
        # 2A.2: check if the row exists; delete it if it does
        with engine.connect() as conn:
            # delete symbol row, if it exists
            if symbol_col is None:
                pass
            else:
                conn.execute(
                    text(f"DELETE FROM `{table_name}` WHERE {symbol_col} = '{symbol}';")
                )
                conn.commit()
        # write new row
        helper_upload_dict(engine=engine, d=d, table_name=table_name)
        if not silent:
            time_end = time.time()
            print(f"Success! (time: {time_end - time_start:.4g}s)")
    # 2B: if table does not exist; reindex and send data to server immediately
    else:
        helper_upload_dict(engine=engine, d=d, table_name=table_name)
        if not silent:
            time_end = time.time()
            print(f"Success (new table created)! (time: {time_end - time_start:.4g}s)")
        return


def upload_pickle_helper(
    engine: Engine,
    d: dict,
    table_name: str,
    date: Union[None, datetime.datetime],
    symbol: str,
    date_col: Union[None, str],
    symbol_col: str,
):
    """
    Helper to upload a dictionary with a pickle (and more) to an
    SQL database using a given engine.
    """
    # 0: Create an SQL table object and in database
    # 0.1: Get object
    PickleSingle = pickle_single_sql_class(
        table_name=table_name, date_col=date_col, symbol_col=symbol_col
    )
    # 0.2: create table if it does not already exist
    PickleSingle.metadata.create_all(engine)
    # 0.3: add date and symbol to dict
    if date_col is not None:
        d[date_col] = date
    else:
        pass
    d[symbol_col] = symbol

    # 1: create session and upload dict
    # 1.1: create session
    Session = sessionmaker(bind=engine)
    session = Session()
    # 1.2: drop potential existing transformation
    transformation_exists = (
        session.query(PickleSingle).filter_by(**{symbol_col: symbol}).first()
    )
    if transformation_exists:
        session.delete(transformation_exists)
        session.commit()
    # 1.3: add data and commit
    transformation_new = PickleSingle(**d)
    session.add(transformation_new)
    session.commit()


def upload_object(
    engine: Engine,
    obj: Any,
    table_name: str,
    date: Union[datetime.datetime, pd.Timestamp],
    symbol: str,
    date_col: str,
    symbol_col: str,
):
    """
    Pickle an object and store it in the SQL database specified
    by the given SQL engine.
    """
    # 0: assertions
    assert isinstance(engine, Engine), "engine must be an SQL engine"
    assert obj is not None, "obj must be an object"
    assert isinstance(table_name, str), "symbol must be a string"
    assert isinstance(symbol, str), "symbol must be a string"
    assert date_col is None or isinstance(
        date_col, str
    ), "date_col must be a string or None"
    assert isinstance(symbol_col, str), "symbol_col must be a string"

    # 1: pickle and prepare
    # 1.1: pickle
    d = {"data": pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)}
    # 1.2: convert time-stamp if necessary
    if isinstance(date, pd.Timestamp):
        date = date.to_pydatetime()
    elif isinstance(date, datetime.datetime):
        pass
    else:
        raise ValueError("date must be a datetime or pd.Timestamp object")

    # 2: upload
    upload_pickle_helper(
        engine=engine,
        d=d,
        table_name=table_name,
        date=date,
        symbol=symbol,
        date_col=date_col,
        symbol_col=symbol_col,
    )


def get_object(
    engine: Engine,
    table_name: str,
    symbol: str,
    date_col: str,
    symbol_col: str,
):
    """
    Read an object pickle from the database, and unpickle it
    """
    # 1: initialise
    # 1.1: prepare SQL table model object
    PickleSingle = pickle_single_sql_class(
        table_name=table_name,
        date_col=date_col,
        symbol_col=symbol_col,
    )
    # 1.2: prepare session
    Session = sessionmaker(bind=engine)
    session = Session()

    # 2: read pickle from database and return unpickled object
    out = (
        session.query(PickleSingle)
        .filter(getattr(PickleSingle, symbol_col) == symbol)
        .first()
    )
    return pickle.loads(out.data)


def get_df_symbol_request(
    engine: Engine,
    request: str,
    symbol: str,
):
    """
    Send a request to the database and read it
    using pandas read_sql method.
    """
    # parse the query and send it
    query = f"{request} WHERE symbol = '{symbol}'"
    return pd.read_sql(query, engine)


def get_df_symbols_data(
    engine: Engine,
    symbols: list,
    table_name: str,
    symbol_col: str = "symbol",
):
    """
    Downloads data for a list of symbols for given table_name
    from the database.
    """
    # 0: initialisation
    assert isinstance(symbols, list), "symbols must be a list of strings"
    assert isinstance(table_name, str), "table_name must be a string"

    # parse the query and send it
    str_symbols = "', '".join(symbols)
    query = f"SELECT * FROM `{table_name}` WHERE {symbol_col} IN ('{str_symbols}')"
    if table_name == "Meta data":
        return pd.read_sql(query, engine).to_dict("records")[0]
    else:
        df = pd.read_sql(query, engine)
        return df.infer_objects()


def get_symbol_data(
    engine: Engine,
    symbol: str,
    table_name: str,
    date_col: str = "date",
    symbol_col: str = "date",
    drop_symbol_col: bool = False,
):
    """
    Downloads data for a given table_name from the database,
    filtering symbol column by the symbol
    """
    # parse the query and send it
    query = f"SELECT * FROM `{table_name}` WHERE symbol = '{symbol}'"
    if table_name == "Meta data":
        return pd.read_sql(query, engine).to_dict("records")[0]
    else:
        df = pd.read_sql(query, engine)
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df = df.sort_index(ascending=False)
        if drop_symbol_col:
            df = df.drop(columns=symbol_col)
        return df


def get_all_symbol_data(
    engine: Engine,
    symbol: str,
    date_col: str = "date",
    silent: bool = False,
):
    """
    Downloads all data from the database, for each
    of the available tables, filtering symbol column by
    the symbol
    """
    # 0: initialise
    d = {}
    # 1: iterate over tables that exist in the database
    md = MetaData()
    md.reflect(bind=engine)
    if not silent:
        print(f"{symbol}: Getting all data from database... ", end="")
    for table_name in md.tables:
        # Define the SQL query to select rows where the column "symbol" has the value "ASD"
        query = f"SELECT * FROM `{table_name}` WHERE symbol = '{symbol}'"
        # Execute the query and load the data into a DataFrame or dictionaryporpd
        if table_name == "Meta data":
            d[table_name] = pd.read_sql(query, engine).to_dict("records")[0]
        else:
            df = pd.read_sql(query, engine)
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.set_index(date_col)
            df = df.sort_index(ascending=False)
            d[table_name] = df
    if not silent:
        print("Success!")
    return d
