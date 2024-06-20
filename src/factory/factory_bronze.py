import copy
from src.pandas_processor.processor_pandas import ProcessorBySource,ProcessAddTimestamp,ProcessFloatToString,ProcessNanValues,ProcessRemovePunctuation,ProcessReplacePattern,ProcessStringToUpper,ProcessData,ProcessRenameColumn,ProcessRenameColumnFromDict,ProcessReduceDataFrame



# Base processor for all
processor_base = ProcessorBySource()
processor_base.add_process(ProcessNanValues('MATERIAL_NBR'))
processor_base.add_process(ProcessFloatToString('COMMERCIAL_SALES_TERRITORY_CODE'))
processor_base.add_process(ProcessFloatToString('COMPANY_CODE'))
processor_base.add_process(ProcessFloatToString('REGION_CODE'))
processor_base.add_process(ProcessStringToUpper('COMMERCIAL_DISTRICT_DESCRIPTION'))
processor_base.add_process(ProcessRemovePunctuation('COMMERCIAL_COUNTRY_NAME'))
processor_base.add_process(ProcessAddTimestamp())


# Create processor america
processor_america = copy.deepcopy(processor_base)
processor_america.add_process(ProcessReplacePattern('COMMERCIAL_AREA_DESCRIPTION','&',' and '))
processor_america.add_process(ProcessStringToUpper('COMMERCIAL_AREA_DESCRIPTION'))

# Create processor EMEA 
processor_emea = copy.deepcopy(processor_base)
processor_emea.add_process(ProcessData('PERIOD'))
processor_emea.add_process(ProcessRenameColumn({'Commercial_SUBREGION_DESC':'COMMERCIAL_SUBREGION_DESC'}))

# Create processor for Asia
processor_asia = ProcessorBySource()
processor_asia.add_process(ProcessRenameColumnFromDict({
    'SALES_SUB_REGION_DESC':'COMMERCIAL_SUBREGION_DESC',
    'Commercial_SUBREGION_DESC':'COMMERCIAL_SUBREGION_DESC'
}))
processor_asia.add_process(ProcessReduceDataFrame())
processor_asia.add_process(ProcessData('PERIOD'))
processor_asia.add_list_process(processor_base.process_list)

# Create processor for forecast
processor_forecast = ProcessorBySource()
processor_forecast.add_process(ProcessRenameColumn(
    {
        'MATERIAL_NUMBER':'MATERIAL_NBR',
        'CMRCL_RGN_NM': 'REGION_DESCRIPTION',  
        'CMRCL_CNTRY_DSC': 'COMMERCIAL_COUNTRY_NAME',
        'CMRCL_SUBRGN_NM' : 'COMMERCIAL_SUBREGION_DESC',
        'CMRCL_WRLD_AREA_CD': 'COMMERCIAL_WORLD_AREA_CD',
        'YEAR':'PERIOD'
    }
))



dict_processors = {
    'americas': processor_america,
    'emea': processor_emea,
    'asia':processor_asia,
    'forecast':processor_forecast
    }
