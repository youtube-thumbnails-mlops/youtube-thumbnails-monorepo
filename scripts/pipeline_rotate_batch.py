"""
Batch rotation script.
1. Checks if rotation is needed (.rotate flag).
2. Moves current/ to batches/batch_XXX via DVC.
3. Prunes oldest batch if > 350 batches via DVC.
"""
import os
import sys
import shutil
import subprocess
from pathlib import Path

MAX_BATCHES = 150

def run_command(cmd, cwd=None):
    """Runs a shell command and raises error if it fails."""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, check=True, cwd=cwd, text=True)
    return result

def get_rotation_flag(cwd):
    flag_file = cwd / ".rotate"
    if flag_file.exists():
        return flag_file.read_text().strip()
    return None

def rotate_batch(cwd, batch_name):
    print(f"🔄 Rotating current/ to batches/{batch_name}...")
    
    batch_dir = cwd / "batches" / batch_name
    
    # DVC Move (Atomic file move + stage update)
    run_command(f"dvc move current batches/{batch_name}", cwd=cwd)
    
    # Recreate current/
    current_dir = cwd / "current"
    current_dir.mkdir(exist_ok=True)
    run_command("dvc add current/", cwd=cwd)
    
    # Push new batch to R2
    run_command("dvc push", cwd=cwd)

def prune_old_batches(cwd):
    batches_dir = cwd / "batches"
    # Find all batch dvc files
    batch_files = list(batches_dir.glob("batch_*.dvc"))
    
    if len(batch_files) > MAX_BATCHES:
        print(f"⚠️ Limit reached ({len(batch_files)} > {MAX_BATCHES}). Pruning oldest batch...")
        
        # Sort by name (batch_001, batch_002...)
        batch_files.sort(key=lambda x: x.name)
        oldest_batch = batch_files[0]
        oldest_name = oldest_batch.stem
        
        print(f"Removing {oldest_name} from tracking...")
        
        # Remove from DVC
        run_command(f"dvc remove {oldest_batch.name}", cwd=batches_dir)
        
        # Garbage Collection (Delete from R2)
        print("Running DVC garbage collection to delete from R2...")
        run_command("dvc gc --workspace --cloud --force", cwd=cwd)
        
        print(f"✅ {oldest_name} deleted from R2 and local tracking")
    else:
        print(f"📊 Batch count: {len(batch_files)}/{MAX_BATCHES} (no cleanup needed)")

def main():
    # Helper expects to be run from dataset root, or passed as arg
    # In CI, we run python ../scripts/rotate_batch.py from dataset dir
    dataset_dir = Path.cwd()
    
    # 1. Check Rotation
    batch_name = get_rotation_flag(dataset_dir)
    
    # Output for GitHub Actions
    if os.getenv("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            if batch_name:
                f.write(f"needs_rotation=true\n")
                f.write(f"batch_name={batch_name}\n")
            else:
                f.write("needs_rotation=false\n")

    if not batch_name:
        print("No rotation needed.")
        sys.exit(0)

    print(f"Rotation needed: {batch_name}")
    
    # 2. Perform Rotation
    rotate_batch(dataset_dir, batch_name)
    
    # 3. Prune Old Batches
    prune_old_batches(dataset_dir)
    
    # 4. Cleanup
    (dataset_dir / ".rotate").unlink()

if __name__ == "__main__":
    main()
