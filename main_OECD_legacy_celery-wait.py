# ---------------------------------------------------------------- #
# Initialisation
# ---------------------------------------------------------------- #
from modules.oecd import OECD
# Legacy mode (no task queuing with a celery backend)
oecd = OECD(task_queuing="legacy", silent=False)
# Celery backend, but with synchronous execution
oecd = OECD(task_queuing="celery_wait", tasks_module="modules.databases.tasks", silent=False)

# ---------------------------------------------------------------- #
# Submit general OECD queries
# ---------------------------------------------------------------- #
data_message = oecd.oecd_query(agency_identifier="OECD.DAF.CM",
                    dataflow_identifier="DSD_FP@DF_FPS",
                    sdmx_version=1,
                    filter_expression=".A.1000.._T._T._T",
                    optional_parameters=None)
df_sdmx_processed = oecd.helper_process_sdmx(data=data_message, columns_prefix="prefix",column_name_from_dict_key=False)
oecd.update_db(d={"query-name":{"df": df_sdmx_processed}}, database="standard")

# ---------------------------------------------------------------- #
# Key short-term economic indicators (KEI series)
# ---------------------------------------------------------------- #
df1 = oecd.key_economic_indicators()
df2 = oecd.key_economic_indicators(countries=["USA", "EA19", "OECD"])
oecd.update_db(d={"KEI1_": {"df": df1}, "KEI2_": {"df": df2}}, database="standard")

# ---------------------------------------------------------------- #
# Monthly Economic Indicators (MEI series)
# ---------------------------------------------------------------- #
df3 = oecd.business_tendency_surveys()
df4 = oecd.composite_leading_indicators()
df5 = oecd.financial_indicators()
df6 = oecd.production_and_sales()
oecd.update_db(d={"MEI3_": {"df": df3}, "MEI4_": {"df": df4}, "MEI5_": {"df": df5}, "MEI6_": {"df": df6}}, database="standard")

# ---------------------------------------------------------------- #
# Consumer price indices (CPI series)
# ---------------------------------------------------------------- #
df7 = oecd.consumer_price_indices()
oecd.update_db(d={"CPI1_": {"df": df7}}, database="standard")
