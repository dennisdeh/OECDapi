### OECDapi
Fetches the newest data available for a given statistical series (one data column only) from the OECD.Stats and returns a pd.DataFrame. The rows are per default the time axis (i.e. TIME, YEAR etc.).
The request is submitted in the form: 'series/subject/country(.measure)(.frequency)' followed by the optional 'extra_args'.

Find more data series here: https://stats.oecd.org/#
