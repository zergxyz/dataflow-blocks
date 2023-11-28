import argparse
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# User defined DoFn 
class TempObj(beam.DoFn):
    def process(self, element):
        return [len(element)]

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True,
                        help='Input file to process.')
    parser.add_argument(
        '--output', required=True, help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
  
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True 
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p 
            | 'read' >> ReadFromText(known_args.input)  
            | 'grab_targets' >> beam.ParDo(TempObj()) 
            | 'write' >> WriteToText(known_args.output) 
        )
      
# Main entrance here
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
