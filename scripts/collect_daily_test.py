"""
TEST MODE: Daily data collection script.
Configured for minimal downloads and fast rotation.
"""
import os
import sys
from pathlib import Path
from youtube_collector import YouTubeClient

# --- TEST CONFIGURATION ---
BATCH_LIMIT = 3          # Rotate after just 3 images!
TEST_CATEGORY = ['20']   # Only search 'Gaming' (saves 90% quota)
TEST_REGION = "US"       # Only search 'US' (saves 90% quota)
# --------------------------

def count_samples(metadata_file):
    """Count total samples in metadata CSV."""
    if not metadata_file.exists():
        return 0
    with open(metadata_file, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f) - 1  # Subtract header

def get_next_batch_number(batches_dir):
    """Find the next batch number by counting .dvc files."""
    batches_dir.mkdir(exist_ok=True)
    existing_dvc_files = list(batches_dir.glob("batch_*.dvc"))
    
    if existing_dvc_files:
        versions = []
        for dvc_file in existing_dvc_files:
            try:
                name_no_ext = dvc_file.stem
                num = int(name_no_ext.replace("batch_", ""))
                versions.append(num)
            except ValueError:
                continue
        if versions:
            return max(versions) + 1
    return 1

def main():
    client = YouTubeClient()

    current_dir = Path("current")
    batches_dir = Path("batches")
    current_dir.mkdir(exist_ok=True)

    # 1. Fetch minimal videos (Low Quota Usage)
    print("🧪 RUNNING IN TEST MODE")
    print("Fetching ~1-2 videos...")

    videos = client.fetch_batch(
        days_ago=7,                  # Search past week (more likely to find videos)
        videos_per_category=2,       # Try to get 2 videos
        categories=TEST_CATEGORY,    # Only 1 category (Gaming)
        region=TEST_REGION,          # Only 1 region (US)
        min_subscribers=100,         # Very low barrier for test
        min_views=10,                # Very low views requirement
        min_duration_seconds=30,     # Lower duration requirement
        video_duration="medium",
    )

    if not videos:
        print("No videos found. (This happens sometimes in strict test mode).")
        sys.exit(0)

    # 2. Download
    print(f"\n⬇️ Downloading {len(videos)} thumbnails to current/...")
    client.download_thumbnails_bulk(
        videos,
        output_dir=str(current_dir)
    )

    # 3. Save Metadata
    metadata_file = current_dir / "metadata.csv"
    client.save_to_csv(videos, filename=str(metadata_file))
    print(f"✓ Appended {len(videos)} videos")

    # 4. Check Rotation
    total = count_samples(metadata_file)
    print(f"📊 Total in current/: {total}/{BATCH_LIMIT}")

    if total >= BATCH_LIMIT:
        next_batch = get_next_batch_number(batches_dir)
        rotate_flag = Path(".rotate")
        rotate_flag.write_text(f"batch_{next_batch:03d}")
        
        print(f"\n🔄 TEST ROTATION TRIGGERED")
        print(f"📝 Flag created: .rotate → batch_{next_batch:03d}")
    else:
        print(f"✅ Collection complete (No rotation yet)")

if __name__ == "__main__":
    main()