from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam

import argparse
import logging


def run(argv=None, save_main_session=True):
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--dataset',
        help='BQ dataset',
        required=True
    )
    parser.add_argument(
        '--input',
        help='BQ input table',
        required=True
    )
    parser.add_argument(
        '--output',
        help='BQ output table',
        required=True
    )
    
    args, beam_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(beam_args)
    
    with beam.Pipeline(options=pipeline_options) as p:
        
        table_schema = "country:STRING,total_cases:INTEGER"
        
        ( p| 'Read delta' >> beam.io.ReadFromBigQuery(table=f"{args.dataset}.{args.input}")
           | 'Group' >> beam.Map(lambda record: (record['country_name'], record['new_confirmed']))
           | 'Combine' >> beam.CombinePerKey(sum)
           | 'Format' >> beam.Map(lambda rec: {'country': rec[0], 'total_cases': int(rec[1])})
           | 'Writing results to BQ' >> beam.io.WriteToBigQuery(
                    f"{args.dataset}.{args.output}",
                    schema=table_schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

