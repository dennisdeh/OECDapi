from OECDapi.OECD_stats_queries import OECD_query, key_shortterm_indicators_summary, \
    MEI_leading_indicators, consumer_price_indices_all, consumer_price_indices_NI
import pandas as pd

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
# all types (growth
df3 = consumer_price_indices_all(countries="USA")
# national index only data
consumer_price_indices_NI(countries="USA")


# Business Tendency and Consumer Opinion Surveys: Manufacturing, Production
df1 = OECD_query(series="MEI_BTS_COS",
                 subject="BS+BSPR+BSPRTE+BSBUCT",
                 country="AUS",
                 measure="BLSA",
                 frequency="Q",
                 columns_prefix="Manufacturing_Production_")
# Business Tendency and Consumer Opinion Surveys: Manufacturing, Finished goods stocks
df1 = OECD_query(series="MEI_BTS_COS",
                 subject="BS+BSFG+BSFGLV",
                 country="AUS",
                 measure="BLSA",
                 frequency="Q",
                 columns_prefix="MFG_")
# total value in exports
df2 = OECD_query(series="TIVA_2021_C1",
                 subject="EXGR+EXGR_FNL+EXGR_INT+EXGR_DVA+EXGR_DDC+EXGR_IDC+EXGR_RIM+EXGR_FVA+IMGR+IMGR_FNL"
                         "+IMGR_INT+IMGR_DVA+BALGR+REII+PROD+VALU+FFD_DVA+DFD_FVA+BALVAFD+FD_VA+CONS_VA+GFCF_VA"
                         "+EXGR_DVASH+EXGR_FVASH+EXGR_DVAFXSH+EXGR_FNLDVASH+EXGR_INTDVASH+EXGR_INTDVAPSH+EXGRPSH"
                         "+EXGR_DVAPSH+EXGR_TDVAIND+EXGR_TFVAIND+EXGR_SERV_DVASH+EXGR_SERV_FVASH+IMGRINT_REII"
                         "+IMGR_DVASH+IMGRPSH+FFD_DVAPSH+DFD_FVAPSH+VALU_FFDDVA+PROD_VASH+FD_VASH+CONS_VASH"
                         "+GFCF_VASH+DEXFVAPSH+FEXDVAPSH",
                 country=["AUS", "USA"],
                 measure="DTOTAL+D01T03+D01T02+D03+D05T09+D05T06+D07T08",
                 frequency="Q")
