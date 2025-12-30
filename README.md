# YouTube Thumbnails Collection Pipeline

Automated daily collection of YouTube thumbnails with DVC versioning and W&B logging.

**Services:**
- **Data Collection**: YouTube Data API v3
- **Storage**: Cloudflare R2 (S3-compatible object storage)
- **Versioning**: DVC (Data Version Control)
- **Visualization**: Weights & Biases (W&B)
- **Automation**: GitHub Actions

## What This Does

Daily at 8 AM UTC (or manual trigger):
1. Pull current data from R2
2. Fetch ~50 YouTube videos + thumbnails
3. Add to `current/` folder
4. When 500 samples reached → rotate to `batches/batch_XXX/` and version
5. Delete oldest batch when 350 batches reached (rolling window)
6. Log all data to W&B (compressed for visualization)

## Structure

```
youtube-thumbnails-collection/
├── libs/youtube_collector/          # YouTube API client
├── scripts/
│   ├── collect_daily.py             # Production (500/batch, all categories)
│   └── reset_dataset.sh             # Reset R2 + W&B + git
└── .github/workflows/
    └── daily_collect.yml            # GitHub Actions workflow
```

## Setup

Add GitHub Secrets:
- `YOUTUBE_API_KEY` - Google Cloud Console
- `WANDB_API_KEY` - wandb.ai/settings
- `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` - Cloudflare R2
- `DATASET_REPO_TOKEN` - GitHub PAT

## Data Schema

24 columns per sample:
- Visual: thumbnail (1280x720 in R2, 400x400 in W&B)
- IDs: video_id, channel_id
- Metadata: title, category, views, likes, comments, subscribers, tags, duration, etc.
- Features: category_name
- Versioning: batch_version

## Active Filters
1.  **Time**: Last 7 days
2.  **Size**: >10k Subscribers
3.  **Quality**: Views > 0.01% of Subs (Removes spam/glitches)
4.  **Categories**: 15 Major YouTube Categories

## Storage & Retention

**R2 (10GB free tier):**
- Stores full-res images (1280x720) + CSV
- Rolling window: 350 batches × 500 samples = 175,000 images (~10GB)
- Auto-deletes oldest batch when limit reached via `dvc gc`

**W&B (5GB free tier):**
- Stores compressed images (400x400) + metadata
- Auto-prunes to 350 runs (matches R2 window)
- Each run ~14MB → 350 runs = ~4.9GB (stays under limit)

**Data Retention:**
- Collection scripts automatically prune old runs/batches
- No manual cleanup needed
- Stays within free tiers indefinitely

## Costs

**$0/month** (GitHub Actions + R2 + W&B + YouTube API all free tier)

## Related

- [Project Website (Live)](https://youtube-thumbnails-mlops.github.io/youtube-thumbnails-site/) - Premium Showcase
- [youtube-thumbnails-dataset](https://github.com/youtube-thumbnails-mlops/youtube-thumbnails-dataset) - DVC data repo
