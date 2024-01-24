import pandas as pd
from typing import Union
from itertools import compress
import time
from urllib.error import URLError
from http.client import RemoteDisconnected
import urllib3
from urllib3.util.ssl_ import create_urllib3_context
from io import BytesIO


def OECD_query(
    series: str,
    subject: Union[str, list],
    country: Union[str, list],
    measure: Union[str, list, None],
    frequency: str = "M",
    extra_args: Union[str, list, None] = None,
    potential_time_cols: Union[list, None] = None,
    potential_data_cols: Union[list, None] = None,
    columns_prefix: Union[str, None] = None,
    retries: int = 3,
):
    """
    Fetches the newest data available for a given statistical series (one data
    column only) and returns a data frame.

    The rows are per default the time axis (i.e. TIME, YEAR etc.).

    The request is submitted in the form:
        series/subject/country(.measure)(.frequency)
    followed by the optional 'extra_args'.

    Find new data series here: https://stats.oecd.org/#

    Parameters
    ---------
    series: str
        Name of the data table.
    subject: str or list
        Subject.
    country: str or list
        Three letter country code. Combination of counties allowed.
    measure: str, list or None
        measure argument (optional).
    frequency: str or list
        Can be M, Q, Y, or combinations of it
    extra_args: str, list or None
        potential extra arguments (optional).
    potential_time_cols: list or None
        Potential time columns to use. Defaults to ["TIME", "YEAR"] (hierarchy
        to choose: left first)
    potential_data_cols: list or None
        Potential data columns to use. Defaults to ["SUBJECT", "MEASURE",
        "INDICATOR"]
    columns_prefix: str or None
        Prefix to add to all columns
    retries: int
        Number of times the data should be tried to be downloaded again
        after waiting a little bit.

    Returns
    -------
    pd.DataFrame:
        DataFrame with date column and value of the series.

    """
    # step 0: initialisation
    # set some internal parameters
    wait_time_retry = 15
    # step 0.1: checks on structure
    assert isinstance(series, str), "Invalid type for series, must be a string."
    assert isinstance(subject, str) or isinstance(
        subject, list
    ), "Invalid type for subject, must be a string or list."
    assert isinstance(country, str) or isinstance(
        country, list
    ), "Invalid type for country, must be a string or list."
    assert (
        isinstance(measure, str) or isinstance(measure, list) or measure is None
    ), "Invalid type for measure, must be a string, list or None."
    assert isinstance(frequency, str), "Invalid type for frequency, must be a string."
    assert (
        isinstance(extra_args, str) or extra_args is None
    ), "Invalid type for extra_args, must be a string or None."
    assert potential_time_cols is None or isinstance(
        potential_time_cols, list
    ), "Invalid type for potential_time_cols."
    assert potential_data_cols is None or isinstance(
        potential_data_cols, list
    ), "Invalid type for potential_time_cols."
    assert columns_prefix is None or isinstance(
        columns_prefix, str
    ), "Invalid type for columns_prefix."

    # step 0.2: set default time and data column names
    if potential_time_cols is None:
        potential_time_cols = [
            "TIME",
            "YEAR",
        ]  # hierarchy to choose time axis from: left first (choose only one)
    if potential_data_cols is None:
        potential_data_cols = [
            "SUBJECT",
            "MEASURE",
            "INDICATOR",
        ]  # choose all available
    if columns_prefix is None:
        columns_prefix = ""

    # step 1: parse inputs
    dict_input = {
        "subject": subject,
        "country": country,
        "measure": measure,
        "frequency": frequency,
        "extra_args": extra_args,
    }
    for input_var in ["subject", "country", "measure", "frequency"]:
        if isinstance(dict_input[input_var], str):
            dict_input[input_var] = dict_input[input_var] + "."
        elif isinstance(
            dict_input[input_var], list
        ):  # else the input has to be overwritten
            dict_input[input_var] = "+".join(dict_input[input_var]) + "."
        else:  # if empty (i.e. None), no dot
            dict_input[input_var] = ""
    if isinstance(dict_input["extra_args"], str):
        if dict_input["extra_args"].find("?") == -1:
            raise ValueError("extra_args must contain a '?' somewhere.")
    else:
        dict_input["extra_args"] = "?"

    # step 2: download data and parse
    # step 1: try to download the data (a couple of times)
    n = 0
    while n < retries:
        """
        old: https://stats.oecd.org/sdmx-json/data/
        new: https://sdmx.oecd.org/public/rest/data/
        """
        # set up SSL context with legacy connection
        url = (
            f'https://stats.oecd.org/sdmx-json/data/{series}/{dict_input["subject"]}'
            f'{dict_input["country"]}{dict_input["measure"]}{dict_input["frequency"][:-1]}'  # Remove last '.'
            f'{dict_input["extra_args"]}'  # there must be one '?' before the end of the last part
            f"contentType=csv"
        )
        ctx = create_urllib3_context()
        ctx.load_default_certs()
        ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT
        try:
            with urllib3.PoolManager(ssl_context=ctx) as http:
                r = http.request(method="GET", url=url)
                df = pd.read_csv(BytesIO(r.data), low_memory=False)
            break  # if succeeded, break the loop
        except URLError or pd.errors.ParserError or RemoteDisconnected:
            print("Trying downloading again... ", end="")
            n += 1
            df = pd.DataFrame()
            time.sleep(wait_time_retry)
    # step 2.2: assertions
    if n >= retries:  # raise exception if no data was found
        raise ConnectionError("Could not establish connection to the server")
    if len(df) == 0:  # raise exception if no data was found
        raise LookupError("Table not found, check and/or adjust input parameters.")

    # step 3: Selection of columns
    # step 3.1: Figure out time column in potential_time_cols that exists (most-left element is chosen)
    mask = pd.Index(potential_time_cols).isin(df.columns)
    try:  # time column was found
        time_col = list(compress(potential_time_cols, mask))[0]
    except IndexError:
        raise Exception("No time column was found in data.")
    # step 3.2: Figure out which data columns in potential_data_cols that exists
    mask = pd.Index(potential_data_cols).isin(df.columns)
    data_cols = list(compress(potential_data_cols, mask))
    if len(data_cols) == 0:
        raise Exception("No data columns were found.")

    # step 4: transform data to a different format useful for our purposes: One column with data for each point in time
    df[time_col] = df[time_col].astype(str)
    dfg = df.groupby("LOCATION")
    df = pd.DataFrame(
        {time_col: df[time_col].unique()}
    )  # create new df; take the longest possible time period
    for country, dfc in dfg:
        dfg_tmp = dfc.groupby(data_cols)
        for x, df_tmp in dfg_tmp:
            # save values for each subject and measure column combination
            df = df.merge(df_tmp[["Value", time_col]], on=time_col, how="left")
            # rename columns: prepare first a list of columns
            name_list = []
            for col in data_cols:
                name_list.append(df_tmp[col.capitalize()].iloc[0])
            str_name = "_".join(name_list)
            df = df.rename(columns={"Value": f"{str_name}_{country}"})

    # step 5: parse dates, set index and add prefix
    # step 5.1: parse and convert dates
    df = df.rename(columns={time_col: "date"})
    if frequency == "D":
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    elif frequency == "M":
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m")
    elif frequency == "Q":
        df["date"] = pd.PeriodIndex(df["date"], freq="Q").to_timestamp()
    elif frequency == "Y":
        df["date"] = pd.PeriodIndex(df["date"], freq="Y").to_timestamp()
    else:
        df["date"] = pd.to_datetime(df["date"], format="mixed")
    df = df.set_index("date").sort_index(ascending=True)  # Sort old --> new
    # step 5.2: add prefix
    df = df.add_prefix(columns_prefix)

    return df


# %%  Data series already defined
# ---------------------------------------------------------------- #
# Key short-term economic indicators (KEI series)
# ---------------------------------------------------------------- #
def key_shortterm_indicators_summary(
    countries: Union[list, str],
    frequency: str = "M",
    exclude_existing_mf_subjects: bool = False,
):
    """
    Key short-term economic indicators summary.

    Extra data on CPIs, GDP, retail trade and unemployment can be disabled.

    Monthly data available for: AUS, EA19, USA
    """
    if (
        exclude_existing_mf_subjects
    ):  # exclude data available in other macroeconomic features class
        # unemployment rate, consumer prices indices (all items), GDP (ratio to trend), retail trade
        str_extra = ""
    else:  # do not exclude, get all KEI data
        str_extra = "+PP+PI+PIEAMP01+CP+CPALTT01+LRHUTTTT+NAEXKP01+SLRTTO01"
    # all key indices
    return OECD_query(
        series="KEI",
        subject="PS+PR+PRINTO01+PRMNTO01+PRCNTO01+SL+SLRTCR03+OD+ODCNPI03+CI+LO+LOLITOAA+BS"
        "+BSCICP02+LORSGPRT+CS+CSCICP02+LI+LF+LFEMTTTT+LR+LC+LCEAMN01+UL+ULQEUL01+FI+MA"
        "+MANMM101+MABMM301+IR+IRSTCI01+IR3TIB01+IRLTLT01+SP"
        "+SPASTT01+CCUSMA02+XT+XTEXVA01+XTIMVA01+BP+B6BLTT02+NA+NAEXKP02+NAEXKP03"
        "+NAEXKP04+NAEXKP06+NAEXKP0" + str_extra,
        country=countries,
        measure="ST",  # level, ratio or index
        frequency=frequency,
        columns_prefix="KEI_",
    )


# ---------------------------------------------------------------- #
# Monthly Economic Indicators (MEI series)
# ---------------------------------------------------------------- #
# Main Economic Indicators (summary)
def MEI_leading_indicators(countries: Union[list, str]):
    """
    Main Economic Indicators (MEI), leading indicators (original series only)

    Excludes GDP and unemployment data
    """

    return OECD_query(
        series="MEI",
        country="LO+LOLI+LOLITO+LOLITOAA+LOLITOTR+LOCO+LOCOPA+LOCOPAOR+LOCOAB+LOCOABOR+LOCOBS+LOCOBSOR"
        "+LOCOBU+LOCOBUOR+LOCOBD+LOCOBDOR+LOCOBE+LOCOBEOR+LOCOBX+LOCOBXOR+LOCOBF+LOCOBFOR"
        "+LOCOBO+LOCOBOOR+LOCOBI+LOCOBIOR+LOCOBP+LOCOBPOR+LOCOBC+LOCOBCOR+LOCOBK+LOCOBKOR"
        "+LOCOVR+LOCOVROR+LOCODW+LOCODWOR+LOCOPC+LOCOPCOR+LOCOCI+LOCOCIOR+LOCOCE+LOCOCEOR"
        "+LOCOEX+LOCOEXOR+LOCOTX+LOCOTXOR+LOCOXG+LOCOXGOR+LOCOHS+LOCOHSOR+LOCOTM+LOCOTMOR"
        "+LOCOMG+LOCOMGOR+LOCOIS+LOCOISOR+LOCOLT+LOCOLTOR+LOCOMA+LOCOMAOR+LOCONT+LOCONTOR"
        "+LOCOOD+LOCOODOR+LOCOPP+LOCOPPOR+LOCOPB+LOCOPBOR+LOCOPE+LOCOPEOR+LOCOPG+LOCOPGOR"
        "+LOCOPQ+LOCOPQOR+LOCOPM+LOCOPMOR+LOCOSL+LOCOSLOR+LOCOSP+LOCOSPOR+LOCOST+LOCOSTOR"
        "+LOCOSI+LOCOSIOR+LOCOSK+LOCOSKOR+LOCOTT+LOCOTTOR+LOCOTA+LOCOTAOR+LOCOS3OR",
        subject=countries,
        measure="ST+STSA+IXOB+IXOBSA+GY+GYSA",
        frequency="M+Q",
        extra_args="/all?",
        columns_prefix="MELI_",
    )


# ---------------------------------------------------------------- #
# Consumer price indices (PRICES_CPI series)
# ---------------------------------------------------------------- #
# Consumer price indices
def consumer_price_indices_all(countries: Union[list, str]):
    """
    All types of CPIs with all measures.

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

    Measures:
        - Index
        - Index, s.a
        - Percentage change on the same period of the previous year
        - Percentage change from previous period
        - National Index
        - Per thousands of the National CPI Total
        - Contribution to annual inflation
    """
    return OECD_query(
        series="PRICES_CPI",
        subject=countries,
        country="CP00+CPALTT01+CPALTT02+CP010000+CP020000+CP030000+CP040000+CP040100+CP040200+CP040300"
        "+CP040400+CP040500+CP050000+CP060000+CP070000+CP070200+CP080000+CP090000+CP100000"
        "+CP110000+CP120000+CPSDCTGY+CPGR+CPGREN01+CPGRLE01+CPGRSE01+CPGRGO01+CPGRHO01+CPGRHO02"
        "+CPGRLH01+CPGRLH02+PWCP+PWCP0000+PWCP0100+PWCP0200+PWCP0300+PWCP0400+PWCP0410+PWCP0420"
        "+PWCP0430+PWCP0440+PWCP0450+PWCP0500+PWCP0600+PWCP0700+PWCP0722+PWCP0800+PWCP0900"
        "+PWCP1000+PWCP1100+PWCP1200+PWCPEN01+PWCPLE01+PWCPSE01+PWCPGO01+PWCPHO01+PWCPHO05"
        "+PWCPLR01+CP18+CP18GR+CP18GREN+CP18GRLE+CP18GRSE+CP18GRGO+CP18GRH1+CP18GRH2+CP18GRL1"
        "+CP18GRL2+CP18ALTT+CP180100+CP180200+CP180300+CP180400+CP180410+CP180420+CP180430"
        "+CP180440+CP180450+CP180500+CP180600+CP180700+CP180800+CP180900+CP181000+CP181100"
        "+CP181200+CP181300+CP18SDCT+PW18+PW180000+PW180100+PW180200+PW180300+PW180400+PW180500"
        "+PW180600+PW180700+PW180800+PW180900+PW181000+PW181100+PW181200+PW181300+PW18GREN"
        "+PW18GRLE+PW18GRSE+PW18GRGO+PW18GRH2+PW18GRL2+CPHP+CPHPTT01+CPHP0100+CPHP0200+CPHP0300"
        "+CPHP0400+CPHP0401+CPHP0403+CPHP0404+CPHP0405+CPHP0500+CPHP0600+CPHP0700+CPHP0702"
        "+CPHP0800+CPHP0900+CPHP1000+CPHP1100+CPHP1200+CPHPEN01+CPHPLA01+CPHPSE01+CPHPGD01+PWHP"
        "+PWHP0000+PWHP0100+PWHP0200+PWHP0300+PWHP0400+PWHP0410+PWHP0430+PWHP0440+PWHP0450"
        "+PWHP0500+PWHP0600+PWHP0700+PWHP0722+PWHP0800+PWHP0900+PWHP1000+PWHP1100+PWHP1200"
        "+PWHPEN01+PWHPLE01+PWHPSE01+PWHPGO01",
        measure="IXOB+IXOBSA+GY+GP+IXNB+AL+CTGY",
        frequency="M",
        extra_args=None,
        columns_prefix="",
    )


def consumer_price_indices_NI(countries: Union[list, str]):
    """
    National index data CPIs only.

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
    """
    return OECD_query(
        series="PRICES_CPI",
        subject=countries,
        country="CP00+CPALTT01+CPALTT02+CP010000+CP020000+CP030000+CP040000+CP040100+CP040200+CP040300"
        "+CP040400+CP040500+CP050000+CP060000+CP070000+CP070200+CP080000+CP090000+CP100000"
        "+CP110000+CP120000+CPSDCTGY+CPGR+CPGREN01+CPGRLE01+CPGRSE01+CPGRGO01+CPGRHO01+CPGRHO02"
        "+CPGRLH01+CPGRLH02+PWCP+PWCP0000+PWCP0100+PWCP0200+PWCP0300+PWCP0400+PWCP0410+PWCP0420"
        "+PWCP0430+PWCP0440+PWCP0450+PWCP0500+PWCP0600+PWCP0700+PWCP0722+PWCP0800+PWCP0900"
        "+PWCP1000+PWCP1100+PWCP1200+PWCPEN01+PWCPLE01+PWCPSE01+PWCPGO01+PWCPHO01+PWCPHO05"
        "+PWCPLR01+CP18+CP18GR+CP18GREN+CP18GRLE+CP18GRSE+CP18GRGO+CP18GRH1+CP18GRH2+CP18GRL1"
        "+CP18GRL2+CP18ALTT+CP180100+CP180200+CP180300+CP180400+CP180410+CP180420+CP180430"
        "+CP180440+CP180450+CP180500+CP180600+CP180700+CP180800+CP180900+CP181000+CP181100"
        "+CP181200+CP181300+CP18SDCT+PW18+PW180000+PW180100+PW180200+PW180300+PW180400+PW180500"
        "+PW180600+PW180700+PW180800+PW180900+PW181000+PW181100+PW181200+PW181300+PW18GREN"
        "+PW18GRLE+PW18GRSE+PW18GRGO+PW18GRH2+PW18GRL2+CPHP+CPHPTT01+CPHP0100+CPHP0200+CPHP0300"
        "+CPHP0400+CPHP0401+CPHP0403+CPHP0404+CPHP0405+CPHP0500+CPHP0600+CPHP0700+CPHP0702"
        "+CPHP0800+CPHP0900+CPHP1000+CPHP1100+CPHP1200+CPHPEN01+CPHPLA01+CPHPSE01+CPHPGD01+PWHP"
        "+PWHP0000+PWHP0100+PWHP0200+PWHP0300+PWHP0400+PWHP0410+PWHP0430+PWHP0440+PWHP0450"
        "+PWHP0500+PWHP0600+PWHP0700+PWHP0722+PWHP0800+PWHP0900+PWHP1000+PWHP1100+PWHP1200"
        "+PWHPEN01+PWHPLE01+PWHPSE01+PWHPGO01",
        measure="IXNB",
        frequency="M",
        extra_args=None,
        columns_prefix="",
    )
