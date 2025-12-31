"""
Daily data collection script.
1. Downloads HQ images to current/ for R2 (Training).
2. Resizes images in memory for W&B (Visualization) to save space.
3. Prunes W&B runs > 350 to match the R2 Rolling Window.
"""
import os
import sys
import wandb
from pathlib import Path
import math
from PIL import Image
from youtube_collector import YouTubeClient

# CONSTANTS
BATCH_LIMIT = 500
MAX_WANDB_RUNS = 350  # Match R2 window

def count_samples(metadata_file):
    if not metadata_file.exists():
        return 0
    with open(metadata_file, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f) - 1

def get_next_batch_number(batches_dir):
    batches_dir.mkdir(exist_ok=True)
    existing_dvc_files = list(batches_dir.glob("batch_*.dvc"))
    if existing_dvc_files:
        versions = [int(f.stem.replace("batch_", "")) for f in existing_dvc_files if "batch_" in f.stem]
        if versions:
            return max(versions) + 1
    return 1


def prune_old_wandb_runs(project_name, max_runs):
    """Deletes oldest runs to keep W&B storage inside the 5GB limit."""
    try:
        api = wandb.Api()
        runs = api.runs(path=f"{api.default_entity}/{project_name}")
        if len(runs) > max_runs:
            runs_to_delete = len(runs) - max_runs
            print(f"🧹 W&B Pruning: Found {len(runs)} runs. Deleting {runs_to_delete} oldest...")
            sorted_runs = sorted(runs, key=lambda run: run.created_at)
            for i in range(runs_to_delete):
                print(f"   - Deleting run: {sorted_runs[i].name}")
                sorted_runs[i].delete()
            print("✅ W&B Pruning Complete.")
    except Exception as e:
        print(f"⚠️ W&B Pruning failed: {e}")

def main():
    client = YouTubeClient()
    current_dir = Path("current")
    batches_dir = Path("batches")
    current_dir.mkdir(exist_ok=True)

    # 1. Setup Versioning
    next_batch_num = get_next_batch_number(batches_dir)
    target_batch_name = f"batch_{next_batch_num:03d}"
    print(f"🎯 Target Version: {target_batch_name}")

    # 2. Fetch & Download (HQ for R2)
    print("Fetching videos...")
    videos = client.fetch_batch(
        days_ago=7,
        videos_per_category=5,
        region="US_EU",
        min_subscribers=10000,
        min_views=100,
        min_view_ratio=0.0001,  # 0.01% of subs (e.g. 27M subs -> 2700 views)
        video_duration="medium",
    )
    if not videos:
        print("No videos found today. Exiting.")
        sys.exit(0)

    print(f"⬇️ Downloading {len(videos)} HQ thumbnails...")
    client.download_thumbnails_bulk(videos, output_dir=str(current_dir))

    for video in videos:
        # User asked for SAME columns as CSV.
        # batch_version is usually W&B only, but we add it to CSV for consistency.
        # but if we want it in CSV, we should add it here.
        # However, the original code had it only in W&B.
        # User asked for SAME columns. W&B has 'batch_version'.
        video['batch_version'] = target_batch_name
        # is_clickbait is REMOVED.

    metadata_file = current_dir / "metadata.csv"
    client.save_to_csv(videos, filename=str(metadata_file))

    # 3. Log to W&B (Resized for Storage)
    try:
        print("🚀 Logging to Weights & Biases (Compressed)...")
        wandb.login(key=os.getenv("WANDB_API_KEY"))

        run = wandb.init(
            project="youtube-thumbnails-dataset",
            job_type="daily_collection",
            tags=["production", "daily", target_batch_name],
            config={"batch_version": target_batch_name}
        )

        table = wandb.Table(columns=[
            "thumbnail", "video_id", "title", "category_id", "category_name",
            "views", "likes", "comments",
            "channel_id", "channel_subscribers", "channel_total_views", "channel_video_count",
            "tags", "description_len", "duration_seconds", "definition", "language",
            "published_at", "captured_at", "video_url", "thumbnail_url",
            "batch_version"
        ])

        for video in videos:
            img_path = current_dir / f"{video['video_id']}.jpg"
            if img_path.exists():
                # Metrics already calculated and in 'video' dict

                # --- RESIZING MAGIC ---
                with Image.open(img_path) as im:
                    # Convert to RGB
                    im = im.convert('RGB')
                    # Resize to fit within a 400x400 box, MAINTAINING aspect ratio.
                    # A typical 16:9 thumbnail will become roughly 400x225 pixels.
                    # This happens in RAM, doesn't touch the file on disk.
                    im.thumbnail((400, 400))

                    table.add_data(
                        wandb.Image(im),
                        video['video_id'], video['title'], video['category_id'], video['category_name'],
                        video['views'], video['likes'], video['comments'],
                        video['channel_id'], video['channel_subscribers'],
                        video['channel_total_views'], video['channel_video_count'],
                        video['tags'], video['description_len'], video['duration_seconds'],
                        video['definition'], video['language'],
                        video['published_at'], video['captured_at'],
                        video['video_url'], video['thumbnail_url'],
                        target_batch_name
                    )
                # ----------------------

        wandb.log({"collected_batch": table})
        wandb.finish()
        print("✅ W&B Logging Complete")

        # 4. Prune W&B (Keep in sync with R2)
        prune_old_wandb_runs("youtube-thumbnails-dataset", max_runs=MAX_WANDB_RUNS)

    except Exception as e:
        print(f"⚠️ W&B Logging/Pruning failed: {e}")

    # 5. Rotation Logic (R2)
    total = count_samples(metadata_file)
    print(f"📊 Total in current/: {total}/{BATCH_LIMIT} samples")

    if total >= BATCH_LIMIT:
        Path(".rotate").write_text(target_batch_name)
        print(f"🔄 ROTATION NEEDED: {target_batch_name}")
        print(f"📝 Flag created: .rotate → {target_batch_name}")
        print(f"⚠️  Workflow will handle DVC rotation")
    else:
        print(f"✅ Daily collection complete")

if __name__ == "__main__":
    main()
