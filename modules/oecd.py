import os

import pandas as pd
from typing import Union
from urllib.error import URLError
from requests.exceptions import HTTPError
from http.client import RemoteDisconnected
import sdmx
from sdmx.message import DataMessage
import time
import datetime
from celery.result import AsyncResult
import modules.databases.db_connection as db
import modules.databases.celery_connection as cc
from rich.progress import Progress
from modules.utils import lists


class OECD:

    def __init__(
        self,
        date_start: Union[str, None] = None,
        time_wait_retry: int = 60,
        time_wait_query: float = 0.5,
        retries: int = 5,
        task_queuing: str = "legacy",
        tasks_module: str = "modules.databases.tasks",
        start_celery: bool = True,
        path_db_env: Union[str, None] = None,
        silent: bool = False,
    ):
        """
        API for OECD statistical data that implements some endpoints available
        using a legacy method or celery for task querying. Works with an optional
        SQL database backend, and uses redis for caching together with celery.

        Parameters
        ----------
        date_start: (str or None)
            The global start date as a lower cut-off for data
        silent: (bool)
            Should informative messages be printed?
        time_wait_retry: (int)
            Time to wait before a retry (in seconds) in legacy mode
        time_wait_query: (float)
            Time to wait before sending a query (in seconds) in legacy mode
        retries: (int)
            Number of times to try downloading again after a short break in legacy mode
        task_queuing: (str)
            Legacy (waiting and retrying),
            celery_wait (use celery, but submit one job at a time and wait for the result),
            celery_submit (distributed and asynchronous task queuing)
        tasks_module: (str)
            Task module to be imported and used
        start_celery: (bool)
            If task_queuing is celery, should the workers be started?
        path_db_env: (str) (optional)
            Location of .env file used to create the Docker container for
            redis with. Container name and ports will be loaded from here.
            If None, the default location is the current working directory.
        """
        # assertions
        assert task_queuing in [
            "legacy",
            "celery_wait",
            "celery_submit",
        ], "task_queuing must be either 'legacy', 'celery_wait' or 'celery_submit'"
        assert isinstance(retries, int), "Invalid type for retries, must be an integer"
        assert isinstance(
            time_wait_retry, int
        ), "Invalid type for time_wait_retry, must be an integer"

        # initialise attributes
        if date_start is None:
            self.date_start = "2000-01-01"
        elif isinstance(date_start, str):
            assert len(date_start) == 10, "Date must be in the format YYYY-MM-DD"
            self.date_start = date_start
        else:
            raise ValueError("date_start must be either a string or None")
        self.silent = silent
        self.time_wait_retry = time_wait_retry
        self.time_wait_query = time_wait_query
        self.retries = retries
        self.task_queuing = task_queuing
        self.date_col = "date"
        self.current_year = datetime.date.today().year
        self.current_quarter = (datetime.date.today().month - 1) // 3
        self.tasks_module = tasks_module
        self.start_celery = start_celery
        if path_db_env is None:
            self.path_db_env = os.getcwd()
            self.path_db_env = os.path.join(self.path_db_env, ".env")
            assert os.path.isfile(self.path_db_env), ".env file is not found at the default location"
        elif isinstance(path_db_env, str):
            assert os.path.isdir(path_db_env), "Invalid path for environment variables, must be a directory."
            self.path_db_env = path_db_env
            assert os.path.isfile(self.path_db_env), ".env file is not found at the given location"
        else:
            raise ValueError("Invalid type for path_db_env, must be a string or None.")
        self.worker_processes = None
        self.tasks = None

        if (
            self.task_queuing == "celery_wait" or self.task_queuing == "celery_submit"
        ) and self.start_celery:
            self.celery_app_initialise()
            self.celery_workers_start()

    # ----------------------------------------------------------------------------- #
    # Helper methods
    # ----------------------------------------------------------------------------- #
    def celery_app_initialise(self, tasks=None):
        """
        Initialise redis backend, import tasks, and initialise Celery app.
        """
        if tasks is None:
            self.tasks = cc.celery_app_initialise(
                tasks_module=self.tasks_module, path_db_env=self.path_db_env
            )
        else:
            self.tasks = tasks

    def celery_workers_start(
        self,
        pool: Union[str, None] = None,
        concurrency: int = 100,
        worker_processes=None,
    ):
        """
        Start Celery workers; pooling and concurrency settings can be controlled.
        """
        if worker_processes is None:
            self.worker_processes = cc.celery_workers_start(
                tasks=self.tasks,
                task_queuing=self.task_queuing,
                pool=pool,
                concurrency=concurrency,
            )
        else:
            self.worker_processes = worker_processes

    def celery_workers_stop(self, worker_processes=None, task_queuing=None):
        """
        Stop all Celery workers.
        """
        if worker_processes is None:
            cc.celery_workers_stop(
                worker_processes=self.worker_processes, task_queuing=self.task_queuing
            )
        else:
            cc.celery_workers_stop(
                worker_processes=worker_processes, task_queuing=task_queuing
            )

    def celery_workers_running(self, worker_processes=None):
        """
        Check if the Celery workers are running
        """
        if worker_processes is None:
            return cc.celery_workers_running(worker_processes=self.worker_processes)
        else:
            return cc.celery_workers_running(worker_processes=worker_processes)

    def celery_submit_several_jobs_all_data(
        self,
    ) -> dict:
        """
        Get all OECD data for the default parameters.
        """
        # 0: initialisation
        d = dict()
        print(" *** Submitting tasks for all macroeconomic data *** ")

        # 1: submit tasks and create a dictionary for all OECD data implemented and return
        d[self.key_economic_indicators(return_key=True)] = (
            self.key_economic_indicators()
        )
        d[self.business_tendency_surveys(return_key=True)] = (
            self.business_tendency_surveys()
        )
        d[self.composite_leading_indicators(return_key=True)] = (
            self.composite_leading_indicators()
        )
        d[self.financial_indicators(return_key=True)] = self.financial_indicators()
        d[self.production_and_sales(return_key=True)] = self.production_and_sales()
        d[self.consumer_price_indices(return_key=True)] = self.consumer_price_indices()
        print("Completed!")
        return d

    def process_celery_results(self, d: dict, combine_level1: bool = True) -> dict:
        """
        Process the results stored in the redis backend to retrieve
        the downloaded data as dataframes and can combine the data.

        Symbols where data from the mandatory sheets are missing are
        automatically discarded (and the removed symbols recorded).

        Parameters
        ----------
        d: dict
            contains the AsyncResult objects in nested dictionaries of
            the form {level0: {level1: AsyncResult, ...}, ...}
        combine_level1: bool
            If combine_level1, the values of the dictionary at level 1
            are combined into a single dataframe as
                {level0: {"combined": pd.DataFrame}, ...}
            Otherwise, they will be kept separate as
                {level0: {level1: pd.DataFrame, ...}, ...}

        Returns
        -------
        dict
            Contains the dataframes.
        """
        # 0: initialise
        print(" *** Processing downloaded data *** ")

        # 1: process each sheet
        dp = {}
        time_start = time.time()
        for sheet in d:
            dp[sheet] = {}
            if combine_level1:
                df_tmp = pd.DataFrame()
                # join all dataframes
                for symbol in d[sheet]:
                    result = d[sheet][symbol].result
                    if result is None:
                        continue
                    df_tmp = df_tmp.join(
                        self.helper_process_sdmx(
                            data=result,
                            columns_prefix=sheet,
                            column_name_from_dict_key=False,
                        ),
                        on=None,
                        how="outer",
                    )
                dp[sheet] = {"combined": df_tmp.copy()}
            else:
                for symbol in d[sheet]:
                    result = d[sheet][symbol].result
                    dp[sheet][symbol] = self.helper_process_sdmx(
                        data=result,
                        columns_prefix=sheet,
                        column_name_from_dict_key=False,
                    )
        time_end = time.time()
        print(f"Completed! (in {round((time_end - time_start) / 60, 2)}m)")

        return dp

    # %% Batch download
    def download_all_data(
        self,
    ) -> dict:
        """
        Download all implemented OECD data.
        """
        # 0: initialisation
        print(" +++ Download all available data +++ ")
        # 0.1: check queuing method and other assertions
        assert (
            self.task_queuing == "celery_submit"
        ), "this method is only implemented for celery_submit task queuing"

        # 1: submit jobs and download
        # 1.1: submit jobs for all data
        d = self.celery_submit_several_jobs_all_data()
        # 1.2: monitor progress until all have been downloaded
        cc.celery_download_status(d=d)

        # 2: Processing results after downloads have been completed
        d = self.process_celery_results(d=d)
        return d

    def update_db(self, d: dict, database: str, parallel: bool = False):
        """
        Update a database with the newest data
        """
        # 0: initialise
        # 0.1: get env variables
        print(" *** Updating database *** ")
        db.load_env_variables(path=self.path_db_env)
        # 0.2: create the SQL engine
        engine = db.get_engine(database=database)

        # 1: upload
        d_progress = {}
        time_start = time.time()
        with Progress() as progress:
            # add progress bars
            for sheet in d:
                d_progress[sheet] = progress.add_task(
                    f"[red]{sheet}", total=len(d[sheet])
                )
            if parallel:
                raise NotImplementedError("Parallel mode is not yet implemented")
            else:
                for sheet in d:
                    n = 0
                    for symbol in d[sheet]:
                        db.upload_df(
                            engine=engine,
                            symbol=None,
                            df=d[sheet][symbol].reset_index(),
                            table_name=sheet,
                            categorical_cols=None,
                            numeric_cols=None,
                            date_col=self.date_col,
                            symbol_col=None,
                            drop_index_cols=True,
                            set_index_date_col=True,
                            set_index_symbol_col=False,
                            update_latest=True,
                            columns_to_drop=None,
                            dtype_map=None,
                            keep_keys_nans=False,
                            raise_exception_keys_nans=True,
                            raise_exception_overwrite_symbol_col=False,
                            silent=True,
                        )
                        n += 1
                        progress.update(d_progress[sheet], completed=n)
                        engine.dispose(close=True)
        time_end = time.time()
        print(f"Completed! (in {round((time_end - time_start) / 60,2)}m)")

    # %% Queries
    def get_sdmx_parsed_data(self, url):
        """
        Receive the content of url, parse it as SDMX data and return the object.
        Checks if the returned data is empty or if an error occurred.

        Parameters
        ----------
        url: (str)
            url of the API

        Returns
        -------
        AsyncResult
            parsed content returned from the API.
        """
        # get a response from the API or submit a task
        if self.task_queuing == "legacy":
            # 2.1: initialise
            n = 0
            response = None
            # 2.2: try to download
            while n < self.retries:
                try:
                    time.sleep(self.time_wait_query)
                    response = sdmx.read_url(url)
                    break  # if succeeded, break the loop
                except (URLError, pd.errors.ParserError, RemoteDisconnected, HTTPError):
                    if not self.silent:
                        print(f"OECD: Trying downloading again (trial {n})... ")
                    n += 1
                    time.sleep(self.time_wait_retry)
            # 2.3: assertions
            if n >= self.retries:  # raise exception if no data was found
                raise LookupError(
                    f"Could not establish connection to the server or end-point not found (URL: {url})"
                )
            if response.response.status_code in [403, 404]:
                raise LookupError("Data table not found with the API.")
            elif response.response.status_code != 200:
                raise ConnectionError(
                    "Could not establish connection to the server or end-point not found"
                )
            # parse sdmx message to json format
            return response
        elif self.task_queuing == "celery_wait":
            res = self.tasks.sdmx_request_celery.delay(url)
            out = res.wait(propagate=True)
            res.forget()
            return out
        elif self.task_queuing == "celery_submit":
            return self.tasks.sdmx_request_celery.delay(url)
        else:
            raise NotImplementedError("Invalid task queuing method")

    def oecd_query(
        self,
        dataflow_identifier: str,
        filter_expression: str,
        optional_parameters: Union[None, dict] = None,
        dataflow_version: Union[None, str] = None,
        sdmx_version: int = 2,
        agency_identifier: str = "OECD.SDD.STES",
    ) -> Union[pd.DataFrame, None]:
        """
        Fetches the newest data available for a given statistical series
        and returns a data frame, wrapping around the OECD SDMX API to
        provide some extra functionality and automations.
        The row index is per default the time axis.

        The requests are structured following the SDMX standard, see the
        official OECD Data API for more details:
        https://gitlab.algobank.oecd.org/public-documentation/dotstat-migration/-/raw/main/OECD_Data_API_documentation.pdf

        Parameters
        ---------
        dataflow_identifier: str
            Identifier for the dataflow (table) query.
        filter_expression: str
            Expression for filtering in SDMX v1 formatting (e.g. US.M.EM.....).
            This expression must be adapted to the particular table queried.
        optional_parameters: Union[None, dict]
            Optional parameters to use: startPeriod (v1), endPeriod (v1),
            c[TIME_PERIOD] (v2), lastNObservations, dimensionAtObservation,
            detail (v1), attributes (v2), measures (v2), updatedAfter (v2).
        dataflow_version: str or list
            Dataflow structure version, per default the newest is chosen.
        sdmx_version: int,
            SDMX version: Queries will be adapted to the version syntax.
        agency_identifier: str
            Data owner identifier for the dataflow (table) query. The default
            string 'OECD.SDD.STES' is the standard OECD identifier.

        Returns
        -------
        Union
            pd.DataFrame:
                DataFrame with date column and value of the series.
            None:
                Is returned if there is no data available.
        """
        # 0: initialisation
        # 0.1: print
        if not self.silent:
            print(
                f"Preparing the query {dataflow_identifier}/{filter_expression} for OECD... ",
                end="",
            )
        # 0.2: assertions
        assert isinstance(
            dataflow_identifier, str
        ), "Invalid type for dataflow_identifier, must be a string"
        assert isinstance(
            filter_expression, str
        ), "Invalid type for dataflow_identifier, must be a string"
        assert dataflow_version is None or isinstance(
            dataflow_version, str
        ), "Invalid type for dataflow_version, must be a string or None"
        assert (
            isinstance(optional_parameters, dict) or optional_parameters is None
        ), "Invalid type for optional_parameters, must be a dict or None."
        assert sdmx_version == 1 or sdmx_version == 2, "sdmx_version must be 1 or 2."
        assert isinstance(
            agency_identifier, str
        ), "Invalid type for agency_identifier, must be a string"

        # 1: parse request
        # 1.1: format request URL depending on SDMX version
        base_url = ""
        str_principal = ""
        str_filter = ""
        if sdmx_version == 1:
            # <agency identifier>,<dataflow identifier>,<dataflow version>/<filter expression>[?<optional parameters>]
            if dataflow_version is None:
                dataflow_version = ""
            base_url = "https://sdmx.oecd.org/public/rest/data"
            # principal part
            str_principal = (
                f"{agency_identifier},{dataflow_identifier},{dataflow_version}"
            )
            # filter
            str_filter = filter_expression
        elif sdmx_version == 2:
            # https://github.com/sdmx-twg/sdmx-rest/blob/v2.0.0/doc/data.md
            # <agency identifier>/<dataflow identifier>/<dataflow version>/<filter expression>[?<optional parameters>]
            if dataflow_version is None:
                dataflow_version = "+"
            base_url = "https://sdmx.oecd.org/public/rest/v2/data/dataflow"
            # principal part
            str_principal = (
                f"{agency_identifier}/{dataflow_identifier}/{dataflow_version}"
            )
            # filter
            str_filter = filter_expression.replace("..", ".*.")
            str_filter = str_filter.replace("*.", "*.*")
        # 1.2: add optional parameters
        if optional_parameters is None or (
            isinstance(optional_parameters, dict) and len(optional_parameters) == 0
        ):
            str_optional_parameters = ""
        elif isinstance(optional_parameters, dict):
            str_optional_parameters = ""
            for k, v in optional_parameters.items():
                if len(str_optional_parameters) == 0:
                    str_optional_parameters = f"?{k}={v}"
                else:
                    str_optional_parameters += f"&{k}={v}"
        else:
            raise ValueError(
                "Invalid type for optional_parameters, must be a dict or None."
            )
        # 1.3: final url
        url = f"{base_url}/{str_principal}/{str_filter}{str_optional_parameters}"
        if not self.silent:
            print("Success!")

        # 2: send request and handle exceptions if no data is available
        try:
            data = self.get_sdmx_parsed_data(url)
        except (LookupError, HTTPError):
            data = None
        return data

    def helper_process_sdmx(
        self, data, columns_prefix: Union[str, None], column_name_from_dict_key: bool
    ):
        """
        Process the SDMX data returned from OECD to a dataframe,
        parse date, raise exception if the parsing cannot be done.

        If data is None (i.e. no data was downloaded), None is returned
        for consistency with methods in other objects.

        Parameters
        ----------
        data: DataMessage or dict
            Data as an SDMX DataMessage object or a dict of DataMessage objects.
        columns_prefix: str or None
            Prefix to add to all the column names
        column_name_from_dict_key: bool
            If False, the column names in the SDMX data will be kept.
            If True (only works if there is a single column), the column names
            will be overwritten by the corresponding key at level 0.

        Returns
        -------
        pd.DataFrame:
            DataFrame with date column as index.
        """
        # 0: initialisation
        # 0.1: assertions
        assert (
            isinstance(data, DataMessage) or isinstance(data, dict) or data is None
        ), f"data input is not of the expected format: {data}"
        assert columns_prefix is None or isinstance(
            columns_prefix, str
        ), "Invalid type for columns_prefix."
        assert isinstance(
            column_name_from_dict_key, bool
        ), "Invalid type for column_name_from_dict_key."

        # 1: process sdmx data
        # 1A: return None if the data is empty
        if data is None:
            return
        # 1B: process data
        else:
            # 1B.1: try-except for expected format
            try:
                # 1BA.2: if the input is a dict, the data will be combined
                if isinstance(data, dict):
                    df = pd.DataFrame()
                    for k, v in data.items():
                        # 1BA.3: column name can be asked to be overwritten
                        if not column_name_from_dict_key:
                            k = None
                        # 1BA.4a: if the data is empty, continue with the next item in the dictionary
                        if v is None:
                            continue
                        else:
                            # 1BA.4b: otherwise convert to dataframe and join on index column
                            df = df.join(
                                self.helper_covert_df(
                                    data=v, columns_prefix=columns_prefix, column_name=k
                                ),
                                how="outer",
                            )
                    if df.empty:
                        df = None
                # 1BB: if a single item in the data, convert directly
                else:
                    df = self.helper_covert_df(
                        data=data, columns_prefix=columns_prefix, column_name=None
                    )
            except ValueError:
                raise LookupError("Data table not consistent or of expected format")
            # return
            return df

    def helper_covert_df(
        self,
        data: DataMessage,
        columns_prefix: Union[str, None],
        column_name: Union[str, None] = None,
    ):
        # convert to dataframe
        df = sdmx.to_pandas(data, datetime="TIME_PERIOD")
        # treat index
        df = df[~df.index.isna()]
        df = df.sort_index(ascending=False)
        # flatten columns and rename index
        df.columns = ["_".join(col).strip() for col in list(df.columns)]
        df = df.rename_axis(self.date_col)
        # overwrite column name if there is only 1 column
        if column_name is not None:
            assert (
                len(df.columns) == 1
            ), "There are more than one column; cannot set column name"
            df.columns = [column_name]
        # add optional column prefix
        if columns_prefix is not None:
            df = df.add_prefix(f"{columns_prefix}_")
        return df

    def helper_return(self, data, columns_prefix: str):
        # A: convert to dataframe, parse date, raise exception if the parsing cannot be done
        if self.task_queuing == "legacy" or self.task_queuing == "celery_wait":
            return self.helper_process_sdmx(
                data=data,
                columns_prefix=columns_prefix,
                column_name_from_dict_key=False,
            )
        # B: with celery backend, return the async object directly
        else:
            return data

    # %% Predefined data-series
    # ---------------------------------------------------------------- #
    # Key economic indicators (KEI series)
    # ---------------------------------------------------------------- #
    def key_economic_indicators(
        self,
        countries: Union[None, list, str] = None,
        frequency: str = "M",
        return_key: bool = False,
        return_df_expected_columns_if_empty: bool = True,
    ) -> Union[pd.DataFrame, AsyncResult, str]:
        """
        The Key Economic Indicators (KEI) database contains monthly and quarterly
        statistics (and associated statistical methodological information) for all
        OECD member countries and for a selection of non-member countries on a
        wide variety of economic indicators, namely: quarterly national accounts,
        industrial production, composite leading indicators, business tendency
        and consumer opinion surveys, retail trade, consumer and producer prices,
        hourly earnings, employment/unemployment, interest rates, monetary
        aggregates, exchange rates, international trade and balance of payments.

        Parameters
        ----------
        countries: None, list or string (optional)
            Countries to download KEIs for.
            If None, the default list is used
        frequency: str (optional)
            Frequency of the data to download; the highest frequency is the
            default value
        return_key: bool (optional)
            If True, only the designated key of the data series will be returned.
        return_df_expected_columns_if_empty: bool (optional)
            If True, if there is no data, an empty dataframe with the expected
            columns will be returned with a NaT in the index.
            Otherwise, the processed output will be returned directly.

        Returns
        -------
        Union
            str
                Designated key of the data series is returned if return_key
            AsyncResult
                Otherwise, if not return_key and the task queuing is celery_submit,
                an AsyncResult object is returned
            pd.DataFrame
                Otherwise if the task queuing is not celery_submit, the output
                data parsed as a pd.DataFrame object
        """
        # 0: initialisation
        # 0.1: define key of the data series
        key = "OECD-KEI"
        if return_key:
            return key
        # 0.2: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print("Fetching OECD key_economic_indicators")
        # 0.3: define default countries, if none are given
        if countries is None:
            countries = ["USA", "EA20", "EU27_2020", "OECD"]
        # 0.4: parsing of countries
        if isinstance(countries, list):
            countries = "+".join(countries)
        elif isinstance(countries, str):
            pass
        else:
            raise ValueError("countries must be a list or a string.")

        # 1: get data
        data = dict()
        data[countries] = self.oecd_query(
            dataflow_identifier="DSD_KEI@DF_KEI",
            dataflow_version="4.0",
            filter_expression=f"{countries}.{frequency}......",
            optional_parameters=None,
            sdmx_version=1,
            agency_identifier="OECD.SDD.STES",
        )

        # 2: prepare to return or create an empty dataframe with the expected columns if there is no data
        data = self.helper_return(data=data, columns_prefix=key)
        if data is None and return_df_expected_columns_if_empty:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(
                    "No data could be downloaded, filling expected columns with no data"
                )
            cols_expected = [
                "OECD-KEI_EU27_2020_M_TOCAPA_GR_G45_Y_G1",
                "OECD-KEI_USA_M_PRVM_IX_C_Y__Z",
                "OECD-KEI_USA_M_IM_USD__T_Y__Z",
                "OECD-KEI_EA20_M_NODW_GR_F41_Y_GY",
                "OECD-KEI_EU27_2020_M_IM_USD__T_Y__Z",
                "OECD-KEI_EU27_2020_M_BCICP_PB_C_Y__Z",
                "OECD-KEI_EU27_2020_M_EX_GR__T_Y_GY",
                "OECD-KEI_OECD_M_H_EARN_IX_C_Y__Z",
                "OECD-KEI_USA_M_LI_IX__T_AA__Z",
                "OECD-KEI_EA20_M_TOCAPA_GR_G45_Y_GY",
                "OECD-KEI_USA_M_SHARE_GR__Z__Z_GY",
                "OECD-KEI_OECD_M_TOCAPA_GR_G45_Y_G1",
                "OECD-KEI_USA_M_IRLT_PA__Z__Z__Z",
                "OECD-KEI_EU27_2020_M_TOCAPA_IX_G45_Y__Z",
                "OECD-KEI_USA_M_UNEMP_PT_LF__T_Y__Z",
                "OECD-KEI_USA_M_IM_GR__T_Y_G1",
                "OECD-KEI_USA_M_PRVM_GR_BTE_Y_GY",
                "OECD-KEI_EU27_2020_M_PRVM_IX_C_Y__Z",
                "OECD-KEI_USA_M_RS_IX__T_RT__Z",
                "OECD-KEI_USA_M_IM_GR__T_Y_GY",
                "OECD-KEI_EU27_2020_M_PRVM_GR_F_Y_GY",
                "OECD-KEI_USA_M_PP_IX_C__Z__Z",
                "OECD-KEI_EU27_2020_M_PRVM_GR_C_Y_GY",
                "OECD-KEI_EA20_M_PRVM_GR_C_Y_G1",
                "OECD-KEI_OECD_M_MANM_GR__Z_Y_G1",
                "OECD-KEI_OECD_M_PRVM_IX_F_Y__Z",
                "OECD-KEI_EU27_2020_M_CCICP_PB__Z_Y__Z",
                "OECD-KEI_USA_M_TOVM_GR_G47_Y_G1",
                "OECD-KEI_EA20_M_IM_USD__T_Y__Z",
                "OECD-KEI_EU27_2020_M_TOCAPA_GR_G45_Y_GY",
                "OECD-KEI_OECD_M_PRVM_GR_F_Y_G1",
                "OECD-KEI_OECD_M_CP_GR__Z__Z_GY",
                "OECD-KEI_USA_M_H_EARN_GR_C_Y_GY",
                "OECD-KEI_USA_M_EX_USD__T_Y__Z",
                "OECD-KEI_USA_M_CCICP_PB__Z_Y__Z",
                "OECD-KEI_OECD_M_MANM_IX__Z_Y__Z",
                "OECD-KEI_OECD_M_IM_USD__T_Y__Z",
                "OECD-KEI_OECD_M_MABM_GR__Z_Y_GY",
                "OECD-KEI_USA_M_CP_IX__Z__Z__Z",
                "OECD-KEI_OECD_M_PRVM_GR_C_Y_G1",
                "OECD-KEI_USA_M_PRVM_GR_C_Y_GY",
                "OECD-KEI_EA20_M_UNEMP_PT_LF__T_Y__Z",
                "OECD-KEI_EU27_2020_M_TOVM_IX_G47_Y__Z",
                "OECD-KEI_EU27_2020_M_EX_GR__T_Y_G1",
                "OECD-KEI_EA20_M_EX_GR__T_Y_G1",
                "OECD-KEI_USA_M_IR3TIB_PA__Z__Z__Z",
                "OECD-KEI_EU27_2020_M_PP_IX_C__Z__Z",
                "OECD-KEI_OECD_M_PRVM_IX_BTE_Y__Z",
                "OECD-KEI_OECD_M_H_EARN_GR_C_Y_G1",
                "OECD-KEI_USA_M_TOCAPA_GR_G45_Y_GY",
                "OECD-KEI_EA20_M_CP_GR__Z__Z_GY",
                "OECD-KEI_EU27_2020_M_CP_GR__Z__Z_G1",
                "OECD-KEI_EU27_2020_M_CP_GR__Z__Z_GY",
                "OECD-KEI_OECD_M_PRVM_GR_C_Y_GY",
                "OECD-KEI_EU27_2020_M_TOVM_GR_G47_Y_GY",
                "OECD-KEI_USA_M_EX_GR__T_Y_GY",
                "OECD-KEI_OECD_M_MABM_GR__Z_Y_G1",
                "OECD-KEI_OECD_M_IM_GR__T_Y_GY",
                "OECD-KEI_OECD_M_CP_GR__Z__Z_G1",
                "OECD-KEI_OECD_M_EX_USD__T_Y__Z",
                "OECD-KEI_USA_M_EX_GR__T_Y_G1",
                "OECD-KEI_OECD_M_EX_GR__T_Y_GY",
                "OECD-KEI_EU27_2020_M_PRVM_GR_BTE_Y_GY",
                "OECD-KEI_USA_M_EMP_PS__T_Y__Z",
                "OECD-KEI_USA_M_PRVM_IX_BTE_Y__Z",
                "OECD-KEI_USA_M_PP_GR_C__Z_G1",
                "OECD-KEI_USA_M_TOCAPA_IX_G45_Y__Z",
                "OECD-KEI_EA20_M_PRVM_IX_BTE_Y__Z",
                "OECD-KEI_USA_M_SHARE_GR__Z__Z_G1",
                "OECD-KEI_USA_M_MABM_GR__Z_Y_G1",
                "OECD-KEI_EA20_M_PRVM_GR_BTE_Y_GY",
                "OECD-KEI_OECD_M_TOVM_IX_G47_Y__Z",
                "OECD-KEI_EA20_M_PRVM_IX_C_Y__Z",
                "OECD-KEI_EA20_M_CP_GR__Z__Z_G1",
                "OECD-KEI_USA_M_H_EARN_IX_C_Y__Z",
                "OECD-KEI_EA20_M_PRVM_IX_F_Y__Z",
                "OECD-KEI_EA20_M_CP_IX__Z__Z__Z",
                "OECD-KEI_USA_M_MANM_IX__Z_Y__Z",
                "OECD-KEI_EA20_M_NODW_GR_F41_Y_G1",
                "OECD-KEI_EA20_M_EX_USD__T_Y__Z",
                "OECD-KEI_OECD_M_H_EARN_GR_C_Y_GY",
                "OECD-KEI_USA_M_CP_GR__Z__Z_GY",
                "OECD-KEI_EA20_M_TOVM_IX_G47_Y__Z",
                "OECD-KEI_OECD_M_TOVM_GR_G47_Y_G1",
                "OECD-KEI_EU27_2020_M_PRVM_IX_F_Y__Z",
                "OECD-KEI_EU27_2020_M_PP_GR_C__Z_G1",
                "OECD-KEI_EU27_2020_M_IM_GR__T_Y_GY",
                "OECD-KEI_EU27_2020_M_PRVM_IX_BTE_Y__Z",
                "OECD-KEI_OECD_M_TOVM_GR_G47_Y_GY",
                "OECD-KEI_OECD_M_TOCAPA_IX_G45_Y__Z",
                "OECD-KEI_EA20_M_PRVM_GR_F_Y_GY",
                "OECD-KEI_OECD_M_PRVM_GR_BTE_Y_G1",
                "OECD-KEI_EA20_M_EX_GR__T_Y_GY",
                "OECD-KEI_EU27_2020_M_TOVM_GR_G47_Y_G1",
                "OECD-KEI_OECD_M_PRVM_GR_BTE_Y_GY",
                "OECD-KEI_USA_M_MABM_IX__Z_Y__Z",
                "OECD-KEI_OECD_M_PRVM_GR_F_Y_GY",
                "OECD-KEI_EA20_M_TOVM_GR_G47_Y_G1",
                "OECD-KEI_EU27_2020_M_CP_IX__Z__Z__Z",
                "OECD-KEI_OECD_M_UNEMP_PT_LF__T_Y__Z",
                "OECD-KEI_USA_M_TOCAPA_GR_G45_Y_G1",
                "OECD-KEI_USA_M_TOVM_GR_G47_Y_GY",
                "OECD-KEI_USA_M_PRVM_IX_F_Y__Z",
                "OECD-KEI_EA20_M_IM_GR__T_Y_G1",
                "OECD-KEI_EU27_2020_M_EX_USD__T_Y__Z",
                "OECD-KEI_EU27_2020_M_PRVM_GR_C_Y_G1",
                "OECD-KEI_EA20_M_PRVM_GR_C_Y_GY",
                "OECD-KEI_EA20_M_PRVM_GR_BTE_Y_G1",
                "OECD-KEI_OECD_M_EX_GR__T_Y_G1",
                "OECD-KEI_USA_M_CP_GR__Z__Z_G1",
                "OECD-KEI_USA_M_MANM_GR__Z_Y_G1",
                "OECD-KEI_OECD_M_IM_GR__T_Y_G1",
                "OECD-KEI_EU27_2020_M_PRVM_GR_F_Y_G1",
                "OECD-KEI_USA_M_MABM_GR__Z_Y_GY",
                "OECD-KEI_OECD_M_MABM_IX__Z_Y__Z",
                "OECD-KEI_USA_M_PP_GR_C__Z_GY",
                "OECD-KEI_EA20_M_PRVM_GR_F_Y_G1",
                "OECD-KEI_USA_M_BCICP_PB_C_Y__Z",
                "OECD-KEI_EA20_M_TOCAPA_GR_G45_Y_G1",
                "OECD-KEI_EA20_M_TOVM_GR_G47_Y_GY",
                "OECD-KEI_EU27_2020_M_PRVM_GR_BTE_Y_G1",
                "OECD-KEI_USA_M_PRVM_GR_F_Y_GY",
                "OECD-KEI_EU27_2020_M_UNEMP_PT_LF__T_Y__Z",
                "OECD-KEI_EU27_2020_M_PP_GR_C__Z_GY",
                "OECD-KEI_USA_M_IRSTCI_PA__Z__Z__Z",
                "OECD-KEI_USA_M_PRVM_GR_BTE_Y_G1",
                "OECD-KEI_USA_M_PRVM_GR_F_Y_G1",
                "OECD-KEI_OECD_M_CP_IX__Z__Z__Z",
                "OECD-KEI_OECD_M_PRVM_IX_C_Y__Z",
                "OECD-KEI_OECD_M_TOCAPA_GR_G45_Y_GY",
                "OECD-KEI_USA_M_TOVM_IX_G47_Y__Z",
                "OECD-KEI_USA_M_H_EARN_GR_C_Y_G1",
                "OECD-KEI_USA_M_PRVM_GR_C_Y_G1",
                "OECD-KEI_EU27_2020_M_IM_GR__T_Y_G1",
                "OECD-KEI_EA20_M_IM_GR__T_Y_GY",
                "OECD-KEI_EA20_M_NODW_IX_F41_Y__Z",
                "OECD-KEI_EA20_M_TOCAPA_IX_G45_Y__Z",
            ]
            data = pd.DataFrame(
                columns=cols_expected,
                index=pd.DatetimeIndex([], name=self.date_col),
            )
        return data

    # ---------------------------------------------------------------- #
    # Main Economic Indicators (MEI series)
    # ---------------------------------------------------------------- #
    # Business Tendency Surveys (BTS)
    def business_tendency_surveys(
        self,
        countries: Union[None, list, str] = None,
        frequency: str = "M",
        return_key: bool = False,
        return_df_expected_columns_if_empty: bool = True,
    ) -> Union[pd.DataFrame, AsyncResult, str]:
        """
        Business Tendency Surveys (BTS) – also called business opinion or business
        climate surveys – are economic surveys used to monitor and forecast
        business cycles. Covering four different economic sectors (manufacturing,
        construction, retail trade and services), they are designed to collect
        qualitative information useful in monitoring the current business situation
        and forecasting short-term developments by directly asking company managers
        about the pulse of their businesses. They are well known for providing
        warning of turning points in aggregate economic activity as
        measured by GDP or industrial production.

        Parameters
        ----------
        countries: None, list or string (optional)
            Countries to download BTSs for.
            If None, the default list is used
        frequency: str (optional)
            Frequency of the data to download; the highest frequency is the
            default value
        return_key: bool (optional)
            If True, only the designated key of the data series will be returned.
        return_df_expected_columns_if_empty: bool (optional)
            If True, if there is no data, an empty dataframe with the expected
            columns will be returned with a NaT in the index.
            Otherwise, the processed output will be returned directly.

        Returns
        -------
        Union
            str
                Designated key of the data series is returned if return_key
            AsyncResult
                Otherwise, if not return_key and the task queuing is celery_submit,
                an AsyncResult object is returned
            pd.DataFrame
                Otherwise if the task queuing is not celery_submit, the output
                data parsed as a pd.DataFrame object
        """
        # 0: initialisation
        # 0.1: define key of the data series
        key = "OECD-BTS"
        if return_key:
            return key
        # 0.2: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print("Fetching OECD business_tendency_surveys")
        # 0.3: define default countries, if none are given
        if countries is None:
            countries = ["USA", "EA20", "EA19", "OECD"]
        # 0.4: parsing of countries
        if isinstance(countries, list):
            countries = "+".join(countries)
        elif isinstance(countries, str):
            pass
        else:
            raise ValueError("countries must be a list or a string.")

        # 1: get data
        data = dict()
        data[countries] = self.oecd_query(
            dataflow_identifier="DSD_STES@DF_BTS",
            dataflow_version=None,
            filter_expression=f"{countries}.{frequency}.......",
            optional_parameters=None,
            sdmx_version=1,
            agency_identifier="OECD.SDD.STES",
        )

        # 2: prepare to return or create an empty dataframe with the expected columns if there is no data
        data = self.helper_return(data=data, columns_prefix=key)
        if data is None and return_df_expected_columns_if_empty:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(
                    "No data could be downloaded, filling expected columns with no data"
                )
            cols_expected = [
                "OECD-BTS_EA19_M_XR_PB_C_Y__Z_C_N",
                "OECD-BTS_USA_M_XR_PB_C_Y__Z_C_N",
                "OECD-BTS_EA19_M_EM_PB_GTU_Y__Z_T_N",
                "OECD-BTS_USA_M_BU_PB_A_F_HTU_Y__Z_C_N",
                "OECD-BTS_EA19_M_OD_PB_G47_Y__Z_FT_N",
                "OECD-BTS_EA19_M_SP_PB_F_Y__Z_FT_N",
                "OECD-BTS_EA19_M_BCICP_PB_G47_Y__Z__Z_N",
                "OECD-BTS_EA19_M_DE_PB_GTU_Y__Z_T_N",
                "OECD-BTS_EA19_M_EM_PB_C_Y__Z_FT_N",
                "OECD-BTS_USA_M_CURT_PB_C_Y__Z_C_N",
                "OECD-BTS_EA19_M_EM_PB_F_Y__Z_FT_N",
                "OECD-BTS_EA19_M_DE_PB_GTU_Y__Z_FT_N",
                "OECD-BTS_EA19_M_EM_PB_G47_Y__Z_FT_N",
                "OECD-BTS_EA19_M_VS_PB_G47_Y__Z_C_N",
                "OECD-BTS_EA19_M_BU_PB_G47_Y__Z_T_N",
                "OECD-BTS_USA_M_OD_PB_A_F_HTU_Y__Z_T_N",
                "OECD-BTS_USA_M_EM_PB_A_F_HTU_Y__Z_T_N",
                "OECD-BTS_EA19_M_BCICP_PB_F_Y__Z__Z_N",
                "OECD-BTS_EA19_M_EM_PB_GTU_Y__Z_FT_N",
                "OECD-BTS_EA19_M_BCICP_PB_GTU_Y__Z__Z_N",
                "OECD-BTS_EA19_M_BU_PB_F_Y__Z_T_N",
                "OECD-BTS_USA_M_EM_PB_C_Y__Z_FT_N",
                "OECD-BTS_EA19_M_BU_PB_GTU_Y__Z_T_N",
                "OECD-BTS_EA19_M_BCICP_PB_C_Y__Z__Z_N",
                "OECD-BTS_USA_M_OI_PB_C_Y__Z_T_N",
                "OECD-BTS_USA_M_OB_PB_C_Y__Z_C_N",
                "OECD-BTS_EA19_M_OB_PB_C_Y__Z_C_N",
                "OECD-BTS_EA19_M_OB_PB_F_Y__Z_C_N",
                "OECD-BTS_EA19_M_FG_PB_C_Y__Z_C_N",
                "OECD-BTS_EA19_M_PR_PB_C_Y__Z_T_N",
                "OECD-BTS_EA19_M_PR_PB_C_Y__Z_FT_N",
                "OECD-BTS_USA_M_PR_PB_C_Y__Z_T_N",
                "OECD-BTS_EA19_M_SP_PB_C_Y__Z_FT_N",
                "OECD-BTS_USA_M_RM_PB_A_F_HTU_Y__Z_T_N",
                "OECD-BTS_USA_M_BCICP_PB_C_Y__Z__Z_N",
                "OECD-BTS_EA19_M_BU_PB_G47_Y__Z_FT_N",
            ]
            data = pd.DataFrame(
                columns=cols_expected,
                index=pd.DatetimeIndex([], name=self.date_col),
            )
        return data

    # Composite leading indicators (CLI)
    def composite_leading_indicators(
        self,
        countries: Union[None, list, str] = None,
        frequency: str = "M",
        return_key: bool = False,
        return_df_expected_columns_if_empty: bool = True,
    ) -> Union[pd.DataFrame, AsyncResult, str]:
        """
        The composite leading indicator is a times series, formed by aggregating a variety
        of component indicators which show a reasonably consistent relationship with a
        reference series (e.g. industrial production IIP up to March 2012 and since then
        the reference series is GDP) at turning points. The OECD CLI is designed to provide
        qualitative information on short-term economic movements, especially at the turning
        points, rather than quantitative measures. Therefore, the main message of CLI
        movements over time is the increase or decrease, rather than the amplitude of the
        changes.

        Data preprocessing:
            Adjustments implement
            OECD harmonisation

        Parameters
        ----------
        countries: None, list or string (optional)
            Countries to download CLIs for.
            If None, the default list is used
        frequency: str (optional)
            Frequency of the data to download; the highest frequency is the
            default value
        return_key: bool (optional)
            If True, only the designated key of the data series will be returned.
        return_df_expected_columns_if_empty: bool (optional)
            If True, if there is no data, an empty dataframe with the expected
            columns will be returned with a NaT in the index.
            Otherwise, the processed output will be returned directly.

        Returns
        -------
        Union
            str
                Designated key of the data series is returned if return_key
            AsyncResult
                Otherwise, if not return_key and the task queuing is celery_submit,
                an AsyncResult object is returned
            pd.DataFrame
                Otherwise if the task queuing is not celery_submit, the output
                data parsed as a pd.DataFrame object
        """
        # 0: initialisation
        # 0.1: define key of the data series
        key = "OECD-CLI"
        if return_key:
            return key
        # 0.2: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print("Fetching OECD composite_leading_indicators")
        # 0.3: define default countries, if none are given
        if countries is None:
            countries = ["USA", "EA20", "EA19", "OECD"]
        # 0.4: parsing of countries
        if isinstance(countries, list):
            countries = "+".join(countries)
        elif isinstance(countries, str):
            pass
        else:
            raise ValueError("countries must be a list or a string.")

        # 1: get data
        data = dict()
        data[countries] = self.oecd_query(
            dataflow_identifier="DSD_STES@DF_CLI",
            dataflow_version=None,
            filter_expression=f"{countries}.{frequency}....AA...H",
            optional_parameters=None,
            sdmx_version=1,
            agency_identifier="OECD.SDD.STES",
        )

        # 2: prepare to return or create an empty dataframe with the expected columns if there is no data
        data = self.helper_return(data=data, columns_prefix=key)
        if data is None and return_df_expected_columns_if_empty:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(
                    "No data could be downloaded, filling expected columns with no data"
                )
            cols_expected = [
                "OECD-CLI_USA_M_LI_IX__Z_AA_IX__Z_H",
                "OECD-CLI_USA_M_CCICP_IX__Z_AA_IX__Z_H",
                "OECD-CLI_OECD_M_BCICP_IX__Z_AA_IX__Z_H",
                "OECD-CLI_EA19_M_BCICP_IX__Z_AA_IX__Z_H",
                "OECD-CLI_EA19_M_CCICP_IX__Z_AA_IX__Z_H",
                "OECD-CLI_OECD_M_CCICP_IX__Z_AA_IX__Z_H",
                "OECD-CLI_USA_M_BCICP_IX__Z_AA_IX__Z_H",
            ]
            data = pd.DataFrame(
                columns=cols_expected,
                index=pd.DatetimeIndex([], name=self.date_col),
            )
        return data

    # Financial indicators (FI)
    def financial_indicators(
        self,
        countries: Union[None, list, str] = None,
        frequency: str = "M",
        return_key: bool = False,
        return_df_expected_columns_if_empty: bool = True,
    ) -> Union[pd.DataFrame, AsyncResult, str]:
        """
        Financial Indicators aim to capture in quantitative terms an important but
        heterogeneous and fast evolving area. Key factors driving this change are:
        globalisation of the financial markets; maturing of national financial
        markets and therefore the structure of these markets required to service
        their needs; increased sophistication of the actors in these markets;
        rapid technological change; and evolving regulatory frameworks. Financial
        institutions react and adapt to these conditions by changing their
        strategies; by specialising, by diversifying or concentrating their
        activities, and by extending through mergers and acquisitions. As a
        consequence, there is almost constant evolution in the institutional
        structures in which financial markets operate.

        Parameters
        ----------
        countries: None, list or string (optional)
            Countries to download FIs for.
            If None, the default list is used
        frequency: str (optional)
            Frequency of the data to download; the highest frequency is the
            default value
        return_key: bool (optional)
            If True, only the designated key of the data series will be returned.
        return_df_expected_columns_if_empty: bool (optional)
            If True, if there is no data, an empty dataframe with the expected
            columns will be returned with a NaT in the index.
            Otherwise, the processed output will be returned directly.

        Returns
        -------
        Union
            str
                Designated key of the data series is returned if return_key
            AsyncResult
                Otherwise, if not return_key and the task queuing is celery_submit,
                an AsyncResult object is returned
            pd.DataFrame
                Otherwise if the task queuing is not celery_submit, the output
                data parsed as a pd.DataFrame object
        """
        # 0: initialisation
        # 0.1: define key of the data series
        key = "OECD-FI"
        if return_key:
            return key
        # 0.2: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print("Fetching OECD financial_indicators")
        # 0.3: define default countries, if none are given
        if countries is None:
            countries = ["USA", "EA20", "EA19", "OECD"]
        # 0.4: parsing of countries
        if isinstance(countries, list):
            countries = "+".join(countries)
        elif isinstance(countries, str):
            pass
        else:
            raise ValueError("countries must be a list or a string.")

        # 1: get data
        data = dict()
        data[countries] = self.oecd_query(
            dataflow_identifier="DSD_STES@DF_FINMARK",
            dataflow_version=None,
            filter_expression=f"{countries}.{frequency}.......",
            optional_parameters=None,
            sdmx_version=1,
            agency_identifier="OECD.SDD.STES",
        )

        # 2: prepare to return or create an empty dataframe with the expected columns if there is no data
        data = self.helper_return(data=data, columns_prefix=key)
        if data is None and return_df_expected_columns_if_empty:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(
                    "No data could be downloaded, filling expected columns with no data"
                )
            cols_expected = [
                "OECD-FI_USA_M_IR3TIB_PA__Z__Z__Z__Z_N",
                "OECD-FI_USA_M_IRSTCI_PA__Z__Z__Z__Z_N",
                "OECD-FI_EA19_M_IR3TIB_PA__Z__Z__Z__Z_N",
                "OECD-FI_EA19_M_CC_XDC_USD__Z__Z__Z__Z_N",
                "OECD-FI_EA19_M_IRSTCI_PA__Z__Z__Z__Z_N",
                "OECD-FI_USA_M_IRLT_PA__Z__Z__Z__Z_N",
                "OECD-FI_EA19_M_SHARE_IX__Z__Z__Z__Z_N",
                "OECD-FI_USA_M_SHARE_IX__Z__Z__Z__Z_N",
                "OECD-FI_USA_M_CCRE_IX__Z__Z__Z__Z_N",
                "OECD-FI_EA19_M_IRLT_PA__Z__Z__Z__Z_N",
                "OECD-FI_EA19_M_CCRE_IX__Z__Z__Z__Z_N",
            ]
            data = pd.DataFrame(
                columns=cols_expected,
                index=pd.DatetimeIndex([], name=self.date_col),
            )
        return data

    # Production, sales, work started and orders (PS)
    def production_and_sales(
        self,
        countries: Union[None, list, str] = None,
        frequency: str = "M",
        return_key: bool = False,
        return_df_expected_columns_if_empty: bool = True,
    ) -> Union[pd.DataFrame, AsyncResult, str]:
        """
        The Production and Sales dataset contains industrial statistics on four
        separate subjects: Production; Sales; Orders; and Work started. The data
        series presented within these subjects have been chosen as the most
        relevant industrial statistics for which comparable data across
        countries is available. For Production, data comprise Indices of
        industrial production (IIP) for total industry, manufacturing, energy and
        crude petroleum; and further disaggregation of manufacturing production
        for intermediate goods and for investment goods and crude steel. For
        others, they comprise retail trade and registration of passenger cars;
        and permits issued and work started for dwellings.

        Parameters
        ----------
        countries: None, list or string (optional)
            Countries to download PSs for.
            If None, the default list is used
        frequency: str (optional)
            Frequency of the data to download; the highest frequency is the
            default value
        return_key: bool (optional)
            If True, only the designated key of the data series will be returned.
        return_df_expected_columns_if_empty: bool (optional)
            If True, if there is no data, an empty dataframe with the expected
            columns will be returned with a NaT in the index.
            Otherwise, the processed output will be returned directly.

        Returns
        -------
        Union
            str
                Designated key of the data series is returned if return_key
            AsyncResult
                Otherwise, if not return_key and the task queuing is celery_submit,
                an AsyncResult object is returned
            pd.DataFrame
                Otherwise if the task queuing is not celery_submit, the output
                data parsed as a pd.DataFrame object
        """
        # 0: initialisation
        # 0.1: define key of the data series
        key = "OECD-PS"
        if return_key:
            return key
        # 0.2: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print("Fetching OECD production_and_sales")
        # 0.3: define default countries, if none are given
        if countries is None:
            countries = ["USA", "EA20", "EA19", "OECD"]
        # 0.4: parsing of countries
        if isinstance(countries, list):
            countries = "+".join(countries)
        elif isinstance(countries, str):
            pass
        else:
            raise ValueError("countries must be a list or a string.")

        # 1: get data
        data = dict()
        data[countries] = self.oecd_query(
            dataflow_identifier="DSD_STES@DF_INDSERV",
            dataflow_version=None,
            filter_expression=f"{countries}.{frequency}.......",
            optional_parameters=None,
            sdmx_version=1,
            agency_identifier="OECD.SDD.STES",
        )

        # 2: prepare to return or create an empty dataframe with the expected columns if there is no data
        data = self.helper_return(data=data, columns_prefix=key)
        if data is None and return_df_expected_columns_if_empty:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(
                    "No data could be downloaded, filling expected columns with no data"
                )
            cols_expected = [
                "OECD-PS_EA20_M_PRVM_IX_F_Y__Z__Z_N",
                "OECD-PS_EA20_M_TOVM_IX_G47_Y__Z__Z_N",
                "OECD-PS_USA_M_PRVM_IX_F_Y__Z__Z_N",
                "OECD-PS_USA_M_WSDW_IX_F41_Y__Z__Z_N",
                "OECD-PS_EA20_M_PRVM_IX_BTE_N__Z__Z_N",
                "OECD-PS_USA_M_TOCAPA_IX_G45_Y__Z__Z_N",
                "OECD-PS_USA_M_PRVM_IX_F_N__Z__Z_N",
                "OECD-PS_EA20_M_TOCAPA_IX_G45_Y__Z__Z_N",
                "OECD-PS_OECD_M_TOCAPA_IX_G45_Y__Z__Z_N",
                "OECD-PS_USA_M_TOCAPA_IX_G45_N__Z__Z_N",
                "OECD-PS_EA20_M_NODW_IX_F41_Y__Z__Z_N",
                "OECD-PS_USA_M_TOVM_IX_G47_Y__Z__Z_N",
                "OECD-PS_EA20_M_TOVM_IX_G47_N__Z__Z_N",
                "OECD-PS_EA20_M_TOCAPA_IX_G45_N__Z__Z_N",
                "OECD-PS_OECD_M_PRVM_IX_BTE_Y__Z__Z_N",
                "OECD-PS_USA_M_PRVM_IX_BTE_Y__Z__Z_N",
                "OECD-PS_USA_M_PRVM_IX_C_N__Z__Z_N",
                "OECD-PS_EA20_M_PRVM_IX_C_N__Z__Z_N",
                "OECD-PS_OECD_M_TOVM_IX_G47_Y__Z__Z_N",
                "OECD-PS_USA_M_PRVM_IX_C_Y__Z__Z_N",
                "OECD-PS_EA20_M_PRVM_IX_BTE_Y__Z__Z_N",
                "OECD-PS_USA_M_PRVM_IX_BTE_N__Z__Z_N",
                "OECD-PS_EA20_M_PRVM_IX_C_Y__Z__Z_N",
                "OECD-PS_EA20_M_PRVM_IX_F_N__Z__Z_N",
            ]
            data = pd.DataFrame(
                columns=cols_expected,
                index=pd.DatetimeIndex([], name=self.date_col),
            )
        return data

    # ---------------------------------------------------------------- #
    # Consumer price indices (CPI series)
    # ---------------------------------------------------------------- #
    # Consumer price indices
    def consumer_price_indices(
        self,
        countries: Union[None, list, str] = None,
        frequency: str = "M",
        methodology: str = "HICP",
        return_key: bool = False,
        return_df_expected_columns_if_empty: bool = True,
    ) -> Union[pd.DataFrame, AsyncResult, str]:
        """
        This dataset contains statistics on Consumer Price Indices by
        COICOP 1999 divisions, including national CPIs, Harmonised
        Indices of Consumer Prices (HICPs) and their associated weights
        and contributions to national year-on-year inflation.

        CPIs:
            - 01-12: All items
            - 01: Food and non-Alcoholic beverages
            - 02: Alcoholic beverages, tobacco and narcotics
            - 03: Clothing and footwear
            - 04: Housing, water, electricity, gas and other fuels
                - 04.1: CPI Actual rentals for housing
                - 04.2: CPI Imputed rentals for housing
                - 04.3: CPI Maintenance & repairs of the dwellings
                - 04.4: CPI Water supply and miscellaneous services relating to the dwelling
                - 04.5: CPI Electricity, gas and other fuels
            - 05: Furnishings, household equipment and routine household maintenance
            - 06: Health
            - 07: Transport
                - 07.2.2: CPI Fuels and lubricants for personal transport equipment
            - 08: Communication
            - 09: Recreation and culture
            - 10: Education
            - 11: Restaurants and hotels
            - 12: Miscellaneous goods and services
            - Contribution to annual inflation: residual

        Parameters
        ----------
        countries: None, list or string (optional)
            Countries to download CPIs for.
            If None, the default list is used
        frequency: str (optional)
            Frequency of the data to download; the highest frequency is the
            default value
        methodology: str (optional): str
            Data preprocessing methodology can be Eurostat harmonised index
            (HICP) or National (N)
        return_key: bool (optional)
            If True, only the designated key of the data series will be returned.
        return_df_expected_columns_if_empty: bool (optional)
            If True, if there is no data, an empty dataframe with the expected
            columns will be returned with a NaT in the index.
            Otherwise, the processed output will be returned directly.

        Returns
        -------
        Union
            str
                Designated key of the data series is returned if return_key
            AsyncResult
                Otherwise, if not return_key and the task queuing is celery_submit,
                an AsyncResult object is returned
            pd.DataFrame
                Otherwise if the task queuing is not celery_submit, the output
                data parsed as a pd.DataFrame object
        """
        # 0: initialisation
        # 0.1: define key of the data series
        key = "OECD-CPI"
        if return_key:
            return key
        # 0.2: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print("Fetching OECD consumer price indices")
        # 0.3: define default countries, if none are given
        if countries is None:
            countries = ["USA", "EA20"]
        # 0.4: parsing of countries
        if isinstance(countries, list):
            countries = "+".join(countries)
        elif isinstance(countries, str):
            pass
        else:
            raise ValueError("countries must be a list or a string.")

        # 1: get data
        data = dict()
        data[countries] = self.oecd_query(
            dataflow_identifier="DSD_PRICES@DF_PRICES_ALL",
            dataflow_version="1.0",
            filter_expression=f"{countries}.{frequency}.{methodology}.....",
            optional_parameters=None,
            sdmx_version=1,
            agency_identifier="OECD.SDD.TPS",
        )

        # 2: prepare to return or create an empty dataframe with the expected columns if there is no data
        data = self.helper_return(data=data, columns_prefix=key)
        if data is None and return_df_expected_columns_if_empty:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(
                    "No data could be downloaded, filling expected columns with no data"
                )
            cols_expected_usa = [
                "OECD-CPI_USA_M_HICP_CPI_IX_CP09_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP08_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP02_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP05_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP01_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP01_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP09_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP08_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP10_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP07_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP04_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_PC__T_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP05_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP01_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP07_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP11_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP11_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_IX__T_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP07_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP06_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP12_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP10_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP02_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP04_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP06_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP03_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PA__T_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP08_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP03_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP04_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_PA_CP12_N_GY",
                "OECD-CPI_USA_M_HICP_CPI_PC_CP09_N_G1",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP05_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP11_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP12_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP03_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP02_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP06_N__Z",
                "OECD-CPI_USA_M_HICP_CPI_IX_CP10_N__Z",
            ]
            cols_expected_ea20 = [
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP045_0722_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP05_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP06_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP043_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP045_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP08_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP05_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP11_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PA_SERV_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP02_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP04_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP0722_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP01_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP01_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP07_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP0722_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP02_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP04_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP02_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP01_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP12_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP07_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP09_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP03_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP043_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP043_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PC__T_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP11_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP03_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP06_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_IX_SERV_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP10_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP05_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP10_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PC__TXNRG_01_02_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP044_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP041_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP09_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_IX_GD_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP045_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP04_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP09_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PA__TXNRG_01_02_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PC_SERV_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PA__T_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP11_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP045_0722_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP08_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_IX__TXNRG_01_02_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP12_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP041_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP10_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP045_0722_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP041_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_IX_CP044_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP0722_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP03_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP08_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP06_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PC_CP07_N_G1",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP12_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_IX__T_N__Z",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP044_N_GY",
                "OECD-CPI_EA20_M_HICP_CPI_PA_CP045_N_GY",
            ]
            cols_expected = []
            if countries.find("USA") > -1:
                cols_expected = lists.union(cols_expected, cols_expected_usa)
            if countries.find("EA20") > -1:
                cols_expected = lists.union(cols_expected, cols_expected_ea20)
            data = pd.DataFrame(
                columns=cols_expected,
                index=pd.DatetimeIndex([], name=self.date_col),
            )
        return data
