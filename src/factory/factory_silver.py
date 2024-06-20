import copy 
from src.pandas_processor.processor_pandas import ProcessGroupAndAggregate,ProcessCreateNewColumnFixed,ProcessTransformColumnforDate,ProcessTransformColumnDateinYear,ProcessDropNAColumn,ProcessorBySource,ProcessSelectColumns



# For EMEA aggregation
processor_base_aggregate = ProcessorBySource()
processor_base_aggregate.add_process(ProcessDropNAColumn(['MATERIAL_NBR']))
processor_base_aggregate.add_process(ProcessTransformColumnforDate('PERIOD'))
processor_base_aggregate.add_process(ProcessTransformColumnDateinYear('PERIOD'))
processor_base_aggregate.add_process(ProcessGroupAndAggregate(
    ['MATERIAL_NBR', 'COMMERCIAL_COUNTRY_NAME', 'COMMERCIAL_SUBREGION_DESC','REGION_DESCRIPTION','PERIOD'],
    {'GROSS_SALES': 'sum'}
))


# For Asia
processor_asia = copy.deepcopy(processor_base_aggregate)
processor_asia.add_process(ProcessCreateNewColumnFixed('COMMERCIAL_WORLD_AREA_CD','ASIA'))

# For EMEA
processor_emea = copy.deepcopy(processor_base_aggregate)
processor_emea.add_process(ProcessCreateNewColumnFixed('COMMERCIAL_WORLD_AREA_CD','EMEA'))

# For America
processor_america = ProcessorBySource()
processor_america.add_process(ProcessSelectColumns(['MATERIAL_NBR', 'COMMERCIAL_COUNTRY_NAME', 'COMMERCIAL_SUBREGION_DESC','REGION_DESCRIPTION','PERIOD','GROSS_SALES']))
processor_america.add_process(ProcessCreateNewColumnFixed('COMMERCIAL_WORLD_AREA_CD','AMERICAS'))

# For Forecast
processor_forecast = ProcessorBySource()
processor_forecast.add_process(ProcessSelectColumns(['MATERIAL_NBR', 'COMMERCIAL_COUNTRY_NAME', 'COMMERCIAL_SUBREGION_DESC','REGION_DESCRIPTION','PERIOD','FORECAST_VAL','COMMERCIAL_WORLD_AREA_CD']))


factory = {
    'asia': processor_asia,
    'emea': processor_emea,
    'americas': processor_america,
    'forecast': processor_forecast
}