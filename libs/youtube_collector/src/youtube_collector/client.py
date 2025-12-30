"""
YouTube API client for fetching raw video data and downloading thumbnails.
Optimized for data collection: captures raw metrics, handles quotas gracefully,
and prevents bias via randomization.
"""

import csv
import logging
import os
import re
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Import config helpers (assuming config.py exists in the same folder)
from .config import get_api_key, get_output_dir

logger = logging.getLogger(__name__)

class YouTubeClient:
    """
    Lightweight YouTube API client for raw data collection.
    """

    # Default categories to search (ID: Name)
    DEFAULT_CATEGORIES = {
        '1': 'Film & Animation', '2': 'Autos & Vehicles',
        '10': 'Music', '15': 'Pets & Animals', '17': 'Sports', '19': 'Travel & Events',
        '20': 'Gaming', '22': 'People & Blogs', '23': 'Comedy', '24': 'Entertainment',
        '25': 'News & Politics', '26': 'Howto & Style', '27': 'Education',
        '28': 'Science & Technology', '29': 'Nonprofits & Activism',
    }

    # Region presets
    REGION_PRESETS = {
        'US': ['US'],
        'EU': ['GB', 'IE', 'DE', 'FR', 'NL', 'SE', 'DK', 'FI', 'NO', 'AT', 'BE', 'IT', 'ES', 'PT', 'PL'],
        'US_EU': ['US', 'GB', 'IE', 'DE', 'FR', 'NL', 'SE', 'DK', 'FI', 'NO'],
    }

    def __init__(self, api_key: Optional[str] = None):
        """Initialize the YouTube client."""
        self.api_key = get_api_key(api_key)
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        self._session = requests.Session()
        logger.info("YouTube client initialized")

    def fetch_batch(
        self,
        days_ago: int = 7,
        videos_per_category: int = 5,
        categories: Optional[List[str]] = None,
        region: str = "US",
        min_subscribers: int = 1000,
        min_views: int = 0,
        min_view_ratio: float = 0.0,
        min_duration_seconds: int = 60,
        video_duration: str = "medium",
    ) -> List[Dict[str, Any]]:
        """
        Fetch a batch of videos across random categories and regions.
        """
        # 1. Resolve Regions
        if region in self.REGION_PRESETS:
            region_codes = self.REGION_PRESETS[region]
        else:
            region_codes = [region]

        # CRITICAL: Shuffle regions to prevent bias if quota runs out
        random.shuffle(region_codes)
        
        logger.info(f"Fetching batch (regions={len(region_codes)}, days_ago={days_ago})")

        all_videos = []
        
        # Calculate per-region limit to maintain total count roughly
        per_region_limit = max(1, videos_per_category // max(len(region_codes), 1))

        for region_code in region_codes:
            try:
                videos = self._fetch_videos_by_date(
                    days_ago=days_ago,
                    max_results=per_region_limit,
                    categories=categories,
                    region_code=region_code,
                    min_duration=min_duration_seconds,
                    duration_filter=video_duration,
                )
                all_videos.extend(videos)
            except HttpError as e:
                if e.resp.status in [403, 429]:
                    logger.warning(f"Quota exceeded or rate limit hit on region {region_code}. Stopping batch.")
                    break
                else:
                    logger.error(f"Error fetching region {region_code}: {e}")
                    continue

        # Deduplicate by video_id
        seen = set()
        unique_videos = []
        for v in all_videos:
            if v['video_id'] not in seen:
                # Late Filter: Check subs/views here
                # Quality Control: Ensure video meets both absolute and relative view thresholds
                required_views = max(min_views, int(v['channel_subscribers'] * min_view_ratio))

                if v['channel_subscribers'] >= min_subscribers and v['views'] >= required_views:
                    seen.add(v['video_id'])
                    unique_videos.append(v)

        logger.info(f"Fetched {len(unique_videos)} unique videos")
        return unique_videos

    def _fetch_videos_by_date(
        self,
        days_ago: int,
        max_results: int,
        categories: Optional[List[str]],
        region_code: str,
        min_duration: int,
        duration_filter: str,
    ) -> List[Dict[str, Any]]:
        
        target_date = datetime.now() - timedelta(days=days_ago)
        published_after = target_date.replace(hour=0, minute=0, second=0).isoformat() + 'Z'
        published_before = target_date.replace(hour=23, minute=59, second=59).isoformat() + 'Z'

        if categories is None:
            categories = list(self.DEFAULT_CATEGORIES.keys())

        # CRITICAL: Shuffle categories to prevent bias
        random.shuffle(categories)

        results = []

        for category in categories:
            try:
                # 1. Search (Costs 100 quota)
                search_response = self.youtube.search().list(
                    part="id",
                    publishedAfter=published_after,
                    publishedBefore=published_before,
                    maxResults=max_results,
                    order="date", # Random-ish sampling (newest)
                    type="video",
                    videoCategoryId=category,
                    regionCode=region_code,
                    videoDuration=duration_filter,
                ).execute()

                video_ids = [item['id']['videoId'] for item in search_response.get('items', [])]
                if not video_ids: continue

                # 2. Get Video Details (Costs 1 quota)
                videos_response = self.youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=','.join(video_ids)
                ).execute()

                # 3. Get Channel Details (Costs 1 quota)
                channel_ids = list(set(v['snippet']['channelId'] for v in videos_response['items']))
                channels_response = self.youtube.channels().list(
                    part="statistics",
                    id=','.join(channel_ids)
                ).execute()

                # Store full channel stats object
                channel_stats = {
                    c['id']: c['statistics']
                    for c in channels_response['items']
                }

                # 4. Extract Data
                for item in videos_response['items']:
                    data = self._extract_data(item, channel_stats)
                    if data['duration_seconds'] >= min_duration:
                        results.append(data)

            except HttpError as e:
                # Bubble up quota errors to stop the main loop
                if e.resp.status in [403, 429]:
                    raise e
                logger.error(f"Error in category {category}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error in category {category}: {e}")
                continue

        return results

    def _extract_data(self, video: Dict, channel_stats: Dict) -> Dict[str, Any]:
        """Extract raw metadata including tags and channel context."""
        snippet = video['snippet']
        stats = video.get('statistics', {})
        content = video.get('contentDetails', {})
        
        vid = video['id']
        cid = snippet['channelId']
        c_stat = channel_stats.get(cid, {})
        
        # Get best thumbnail
        thumbnails = snippet.get('thumbnails', {})
        thumb_url = (
            thumbnails.get('maxres', {}).get('url') or
            thumbnails.get('high', {}).get('url') or
            thumbnails.get('medium', {}).get('url') or
            ""
        )

        # Tags (Top 10 only)
        tags_list = snippet.get('tags', [])
        tags_str = "|".join(tags_list[:10])

        return {
            'video_id': vid,
            'title': snippet['title'],
            'category_id': snippet.get('categoryId'),
            'category_name': self.DEFAULT_CATEGORIES.get(snippet.get('categoryId'), 'Unknown'),
            
            # Engagement
            'views': int(stats.get('viewCount', 0)),
            'likes': int(stats.get('likeCount', 0)),
            'comments': int(stats.get('commentCount', 0)),
            
            # Channel Context
            'channel_id': cid,
            'channel_subscribers': int(c_stat.get('subscriberCount', 0)),
            'channel_total_views': int(c_stat.get('viewCount', 0)),
            'channel_video_count': int(c_stat.get('videoCount', 0)),

            # Content Metadata
            'tags': tags_str,
            'description_len': len(snippet.get('description', '')),
            'duration_seconds': self._parse_duration(content.get('duration', 'PT0S')),
            'definition': content.get('definition', 'sd'),
            'language': snippet.get('defaultAudioLanguage', 'en'),

            # Admin
            'published_at': snippet['publishedAt'],
            'captured_at': datetime.utcnow().isoformat(),
            'video_url': f"https://www.youtube.com/watch?v={vid}",
            'thumbnail_url': thumb_url,
        }

    def download_thumbnails_bulk(
        self,
        videos: List[Dict[str, Any]],
        output_dir: Optional[str] = None
    ) -> None:
        """Download images using ID as filename."""
        if not output_dir:
            output_dir = get_output_dir()
            
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Downloading {len(videos)} thumbnails...")

        for v in videos:
            try:
                if not v['thumbnail_url']: continue
                
                filename = f"{v['video_id']}.jpg"
                filepath = out_path / filename
                
                if filepath.exists(): continue

                resp = self._session.get(v['thumbnail_url'], timeout=10)
                resp.raise_for_status()
                
                with open(filepath, 'wb') as f:
                    f.write(resp.content)
                    
            except Exception as e:
                logger.warning(f"Failed {v['video_id']}: {e}")

    def _parse_duration(self, duration: str) -> int:
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
        if not match: return 0
        h, m, s = match.groups()
        return int(h or 0) * 3600 + int(m or 0) * 60 + int(s or 0)

    def save_to_csv(self, videos: List[Dict[str, Any]], filename: str) -> None:
        """Save video list to CSV."""
        if not videos: return
        
        file_exists = os.path.isfile(filename)
        mode = 'a' if file_exists else 'w'

        with open(filename, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=videos[0].keys())
            if not file_exists:
                writer.writeheader()
            writer.writerows(videos)