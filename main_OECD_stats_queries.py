from modules.oecd import OECD
# Legacy mode (no task queuing with a celery backend)
#oecd = OECD(task_queuing="legacy", silent=False)
oecd = OECD(task_queuing="celery_wait", tasks_module="modules.databases.tasks", silent=False)
# oecd = OECD(task_queuing="celery_submit", silent=False)
# d_out = oecd.download_all_data()
# oecd.update_db(d=d_out, database="standard")
#
# # general query
# df0 = oecd.oecd_query(
#     agency_identifier="OECD.SDD.STES",
#     dataflow_identifier="DSD_STES@DF_BTS",
#     dataflow_version=None,  # choose newest
#     filter_expression="USA.M.EM......",  # in v1 format; will be converted to v2 format
#     # optional_parameters=None,
#     optional_parameters={"c[TIME_PERIOD]": "ge:2018+le:2024"},
#     sdmx_version=2,
# )
# ---------------------------------------------------------------- #
# Key short-term economic indicators (KEI series)
# ---------------------------------------------------------------- #
df1 = oecd.key_economic_indicators()
df2 = oecd.key_economic_indicators(countries=["USA", "EA19", "OECD"])

# ---------------------------------------------------------------- #
# Monthly Economic Indicators (MEI series)
# ---------------------------------------------------------------- #
df3 = oecd.business_tendency_surveys()
df4 = oecd.composite_leading_indicators()
df5 = oecd.financial_indicators()
df6 = oecd.production_and_sales()

# ---------------------------------------------------------------- #
# Consumer price indices (CPI series)
# ---------------------------------------------------------------- #
df7a = oecd.consumer_price_indices()
df7b = oecd.consumer_price_indices(
    countries="EA20", frequency="M", methodology="HICP"
)

# ---------------------------------------------------------------- #
# Batch
# ---------------------------------------------------------------- #
d_out = oecd.celery_submit_several_jobs_all_data()
