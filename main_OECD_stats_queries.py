from OECDapi.OECD_stats_queries import OECD_query, key_shortterm_indicators_summary, \
    MEI_leading_indicators, consumer_price_indices_all, consumer_price_indices_NI

# ---------------------------------------------------------------- #
# Key short-term economic indicators (KEI series)
# ---------------------------------------------------------------- #
df1 = key_shortterm_indicators_summary(countries="USA")

# ---------------------------------------------------------------- #
# Monthly Economic Indicators (MEI series)
# ---------------------------------------------------------------- #
df2 = MEI_leading_indicators(countries=["USA", "EA19", "OECD"])

# ---------------------------------------------------------------- #
# Consumer price indices (PRICES_CPI series)
# ---------------------------------------------------------------- #
# all types (growth etc.)
df3 = consumer_price_indices_all(countries="USA")
# national index only data
df4 = consumer_price_indices_NI(countries="USA")

# %% Not yet integrated data series that are useful
# general simple request for MEI_CLI, no parsing
request = (
    "MEI_CLI/.AUS+AUT+BEL+CAN+CHL+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LUX+MEX+NLD+NZL"
    "+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+G4E+G-7+NAFTA+OECDE+OECD+ONM+A5M+BRA+CHN+IND+IDN+RUS"
    "+ZAF.M/all"
)

# Business Tendency and Consumer Opinion Surveys: Manufacturing, Production
df5 = OECD_query(
    series="MEI_BTS_COS",
    subject="BS+BSPR+BSPRTE+BSBUCT",
    country="AUS",
    measure="BLSA",
    frequency="Q",
    columns_prefix="Manufacturing_Production_",
)
# Business Tendency and Consumer Opinion Surveys: Manufacturing, Finished goods stocks
df6 = OECD_query(
    series="MEI_BTS_COS",
    subject="BS+BSFG+BSFGLV",
    country="AUS",
    measure="BLSA",
    frequency="Q",
    columns_prefix="MFG_",
)
