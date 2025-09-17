import os

import dotenv
import polars as pl

from stancemining.utils import get_transcripts_from_video_files

def main():
    dotenv.load_dotenv()
    hf_token = os.getenv('HF_TOKEN')

    transcript_path = './data/transcripts.parquet.zstd'
    if os.path.exists(transcript_path):
        transcript_df = pl.read_parquet(transcript_path)
    else:
        transcript_df = pl.DataFrame()

    mp4_dir_path = './data/mp4s'
    mp4_filenames = os.listdir(mp4_dir_path)
    mp4_paths = [os.path.join(mp4_dir_path, fn) for fn in mp4_filenames if fn.endswith('.mp4')]

    mp4_paths = [p for p in mp4_paths if p not in transcript_df['path'].to_list()]

    new_transcript_df = get_transcripts_from_video_files(mp4_paths, hf_token, skip_errors=True)

    transcript_df = pl.concat([transcript_df, new_transcript_df], how='diagonal_relaxed')
    transcript_df.write_parquet('./data/transcripts.parquet.zstd', compression='zstd')

if __name__ == "__main__":
    main()