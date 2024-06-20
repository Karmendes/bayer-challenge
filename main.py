from src.etl_layers import ETLProcessAmericas,ETLProcessEMEA,ETLProcessAsia,ETLProcessForecast,ETLProcessAggregation,ETLProcessJoining
from src.extractors.csv import ExtractCSV,ExtractCSVs
from src.extractors.xlsx import ExtractXLSX
from src.loaders.csv import LoadCSV,LoadCSVs
from src.loaders.sqlite import LoadSQLite


################################ Process for bronze layer ###################################################
etl = ETLProcessAmericas(ExtractCSV('data/raw/americas.csv'),LoadCSV('data/bronze/americas.csv'))
etl.run()

etl = ETLProcessEMEA(ExtractCSV('data/raw/emea.csv'),LoadCSV('data/bronze/emea.csv'))
etl.run()

etl = ETLProcessAsia(ExtractXLSX('data/raw/asia.xlsx'),LoadCSV('data/bronze/asia.csv'))
etl.run()

etl = ETLProcessForecast(ExtractCSV('data/raw/forecast.csv'),LoadCSV('data/bronze/forecast.csv'))
etl.run()
################################ Process for silver layer #################################################### 
etl = ETLProcessAggregation(ExtractCSVs(
    {
        'asia':'data/bronze/asia.csv',
        'americas':'data/bronze/americas.csv',
        'emea':'data/bronze/emea.csv',
        'forecast':'data/bronze/forecast.csv'
    }
),LoadCSVs(
    {
        'asia':'data/silver/asia.csv',
        'americas':'data/silver/americas.csv',
        'emea':'data/silver/emea.csv',
        'forecast':'data/silver/forecast.csv'
    }
))

etl.run()
########################################## process for gold layer ##############################################
etl = ETLProcessJoining(
    ExtractCSVs(
    {
        'asia':'data/silver/asia.csv',
        'americas':'data/silver/americas.csv',
        'emea':'data/silver/emea.csv',
        'forecast':'data/silver/forecast.csv'
    }),
    LoadSQLite('data/gold/datamart.db','forecast_x_results','replace')
    )

etl.run()