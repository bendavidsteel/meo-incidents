

import polars as pl

import stancemining

def main():
    transcript_df = pl.read_parquet('./data/transcripts.parquet.zstd')

    transcript_df = transcript_df.with_columns(pl.col('result').struct.field('segments').list.eval(pl.col('').struct.field('text')).list.join('').alias('transcript'))

    sm = stancemining.StanceMining()

    document_df = sm.fit_transform(transcript_df['transcript'].to_list(), targets=['Charlie Kirk'])

    document_df.write_parquet('./data/stance_documents.parquet.zstd', compression='zstd')

if __name__ == "__main__":
    main()