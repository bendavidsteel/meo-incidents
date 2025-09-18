import asyncio
import datetime
import os

import polars as pl
import requests
from tqdm import tqdm



def fetch_video_bytes(bytes_url: str):
    bytes_headers = {
        'sec-ch-ua': '"HeadlessChrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"', 
        'referer': 'https://www.tiktok.com/', 
        'accept-encoding': 'identity;q=1, *;q=0', 
        'sec-ch-ua-mobile': '?0', 
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.4 Safari/537.36', 
        'range': 'bytes=0-', 
        'sec-ch-ua-platform': '"Windows"'
    }
    # cookies = await self.parent._context.cookies()
    # cookies = {cookie['name']: cookie['value'] for cookie in cookies}
    r = requests.get(bytes_url, headers=bytes_headers)#, cookies=cookies)
    if r.content is not None or len(r.content) > 0:
        return r.content

async def scrape_pytok(sample_df):
    from pytok.tiktok import PyTok
    async with PyTok() as api:
        all_related_posts = []
        all_comments = []
        all_posts = []
        for post in tqdm(sample_df.to_dicts()):
            video_api = api.video(id=post['id'])
            try:
                video_info = await video_api.info()
                video_info['scraped_at'] = datetime.date.today().isoformat()
                all_posts.append(video_info)

                bytes_file_path = f"./data/mp4s/{post['id']}.mp4"
                if not os.path.exists(bytes_file_path):
                    video_bytes = await video_api.bytes()
                    with open(bytes_file_path, "wb") as f:
                        f.write(video_bytes)

                # async for comment in video_api.comments(count=50):
                #     comment['scraped_at'] = datetime.date.today().isoformat()
                #     all_comments.append(comment)

                async for post in video_api.related_videos(count=50):
                    post['scraped_at'] = datetime.date.today().isoformat()
                    all_related_posts.append(post)
            except Exception as e:
                print(f"Error fetching related videos for post {post['id']}: {e}")
                error_message = str(e).split(':')[1].strip() if ':' in str(e) else str(e)
                all_posts.append({'id': post['id'], 'unavailable_as_of': datetime.date.today().isoformat(), 'unavailable_reason': error_message})

    return all_posts, all_related_posts, all_comments

async def scrape_douyin_tiktok(sample_df):
    from douyin_scraper.tiktok.web.web_crawler import TikTokWebCrawler
    crawler = TikTokWebCrawler()

    all_related_posts = []
    all_comments = []
    all_posts = []

    for post in tqdm(sample_df.to_dicts()):
        try:
            video_id = post['id']
            resp_json = await crawler.fetch_one_video(video_id)
            if resp_json['statusCode'] != 0:
                error_message = resp_json['status_msg']
                status_code = resp_json['statusCode']
                all_posts.append({'id': post['id'], 'unavailable_as_of': datetime.date.today().isoformat(), 'unavailable_reason': error_message})
                continue

            post_info = resp_json['itemInfo']['itemStruct']
            post_info['scraped_at'] = datetime.date.today().isoformat()
            all_posts.append(post_info)

            bytes_file_path = f"./data/mp4s/{post['id']}.mp4"
            if not os.path.exists(bytes_file_path) and 'video' in post_info:
                download_url = post_info['video']['downloadAddr'] if 'downloadAddr' in post_info['video'] else post_info['video']['playAddr']
                video_bytes = fetch_video_bytes(download_url)
                with open(bytes_file_path, "wb") as f:
                    f.write(video_bytes)

            # try:
            resp = await crawler.fetch_related_videos(video_id)
            all_related_posts.extend(resp['itemList'])
        except Exception as e:
            print(f"Error fetching related videos for post {post['id']}: {e}")
            continue

    return all_posts, all_related_posts, all_comments

async def main():
    while True:
        comment_path = './data/charlie_kirk_comments.parquet.zstd'
        post_df = pl.read_parquet('./data/charlie_kirk_posts.parquet.zstd')

        related_post_path = './data/charlie_kirk_related_posts.parquet.zstd'
        if os.path.exists(related_post_path):
            related_df = pl.read_parquet(related_post_path)
            post_df = pl.concat([post_df, related_df], how='diagonal_relaxed').unique(subset=['id'])

        post_df.with_columns(pl.when(pl.col('unavailable_reason').str.contains('Content is not available with'))\
            .then(pl.lit(None))\
            .otherwise(pl.col('unavailable_reason')).alias('unavailable_reason'))

        sample_df = post_df.filter(pl.col('locationCreated').is_null() & pl.col('unavailable_reason').is_null()).sample(50)
        all_posts, all_related_posts, all_comments = await scrape_douyin_tiktok(sample_df)

        if len(all_comments) > 0:
            comment_df = pl.from_dicts(all_comments)
            if os.path.exists(comment_path):
                existing_comments = pl.read_parquet(comment_path)
                comment_df = pl.concat([existing_comments, comment_df], how='diagonal_relaxed').unique(subset=['id'])
            comment_df.write_parquet(comment_path, compression='zstd')

        dfs = [post_df]
        if all_related_posts:
            new_related_df = pl.from_dicts(all_related_posts)
            dfs.append(new_related_df)
        if all_posts:
            new_post_df = pl.from_dicts(all_posts)
            dfs.append(new_post_df)
        
        post_df = pl.concat(dfs, how='diagonal_relaxed').group_by('id').agg(pl.all().drop_nulls().first())
        post_df = post_df.filter(pl.col('desc').str.to_lowercase().str.contains_any(['kirk']))
        post_df.write_parquet(related_post_path, compression='zstd')
        post_df.write_parquet(related_post_path.replace('posts', 'posts_backup'), compression='zstd')

if __name__ == "__main__":
    asyncio.run(main())