# ---------------------------------------------------------------- #
# Initialisation
# ---------------------------------------------------------------- #
from modules.oecd import OECD
# Celery backend with asynchronous execution
oecd = OECD(task_queuing="celery_submit", tasks_module="modules.databases.tasks", silent=False)

# ---------------------------------------------------------------- #
# Submit general OECD queries
# ---------------------------------------------------------------- #
d = oecd.oecd_query(agency_identifier="OECD.DAF.CM",
                    dataflow_identifier="DSD_FP@DF_FPS",
                    sdmx_version=1,
                    filter_expression=".A.1000.._T._T._T",
                    optional_parameters=None)
d_processed = oecd.process_celery_results(d={"query-name":{"async_object": d}}, combine_level1=True)
oecd.update_db(d=d_processed, database="standard")

# ---------------------------------------------------------------- #
# Key short-term economic indicators (KEI series)
# ---------------------------------------------------------------- #
d1 = oecd.key_economic_indicators(countries=["USA", "EA19"])
d2 = oecd.key_economic_indicators(countries=["OECD"])
d_processed1 = oecd.process_celery_results(d={"KEI1": d1, "KEI2": d2}, combine_level1=True)
oecd.update_db(d=d_processed1, database="standard")

# ---------------------------------------------------------------- #
# Monthly Economic Indicators (MEI series)
# ---------------------------------------------------------------- #
d3 = oecd.business_tendency_surveys()
d4 = oecd.composite_leading_indicators()
d5 = oecd.financial_indicators()
d6 = oecd.production_and_sales()
d_processed2 = oecd.process_celery_results(d={"MEI3": d3, "MEI4": d4, "MEI5": d5, "MEI6": d6}, combine_level1=True)
oecd.update_db(d=d_processed2, database="standard")

# ---------------------------------------------------------------- #
# Consumer price indices (CPI series)
# ---------------------------------------------------------------- #
d6 = oecd.consumer_price_indices()
d_processed3 = oecd.process_celery_results(d={"CPI": d6}, combine_level1=True)
oecd.update_db(d=d_processed3, database="standard")

# ---------------------------------------------------------------- #
# Batch Download of all defined data series
# ---------------------------------------------------------------- #
d_out = oecd.download_all_data()
oecd.update_db(d=d_out, database="standard")