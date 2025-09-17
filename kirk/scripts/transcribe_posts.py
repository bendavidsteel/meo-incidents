import os

import dotenv

from stancemining.utils import get_transcripts_from_video_files

def main():
    dotenv.load_dotenv()
    hf_token = os.getenv('HF_TOKEN')

    mp4_dir_path = './data/mp4s'
    mp4_paths = os.listdir(mp4_dir_path)

    transcript_df = get_transcripts_from_video_files(mp4_paths, hf_token)
    transcript_df.write_parquet('./data/transcripts.parquet.zstd', compression='zstd')

if __name__ == "__main__":
    main()