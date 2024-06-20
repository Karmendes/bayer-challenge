from src.pandas_processor.processor_pandas import ProcessorBySource,ProcessJoinDataFrames,ProcessRowBind,ProcessDropNAColumn



processor_binding = ProcessorBySource()
processor_binding.add_process(ProcessRowBind(['asia','americas','emea']))
processor_joining_gold = ProcessorBySource()
processor_joining_gold.add_process(ProcessJoinDataFrames(
    ['MATERIAL_NBR', 'COMMERCIAL_COUNTRY_NAME', 'COMMERCIAL_SUBREGION_DESC',
    'REGION_DESCRIPTION', 'PERIOD', 'COMMERCIAL_WORLD_AREA_CD'],'forecast'))
processor_joining_gold.add_process(ProcessDropNAColumn('GROSS_SALES'))

factory = {
    'binding':processor_binding,
    'joining':processor_joining_gold
}