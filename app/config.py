import shutil
from dotenv import load_dotenv
import os

load_dotenv()

CONFIG = {
    "server_url": os.getenv('SERVER_URL'),
    "input_dir": os.getenv('INPUT_DIR'),
    "output_dir": os.getenv('OUTPUT_DIR'),
    "checkpoint_dir": os.getenv('CHECKPOINT_DIR'),
    "results_dir": os.getenv('RESULTS_DIR'),
    "watermarkDuration": os.getenv('WATERMARK_DURATION'),
    "windowDuration": os.getenv('WINDOW_DURATION'),
    "slideDuration": os.getenv('SLIDE_DURATION'),
    "output_format": os.getenv('OUTPUT_FORMAT')
}

# check if the input and output directories exist, and if they do, delete all files in the directories
for dir in [CONFIG["input_dir"], CONFIG["output_dir"], CONFIG["checkpoint_dir"], CONFIG["results_dir"]]:
    if os.path.exists(dir):
        files = os.listdir(dir)
        for file in files:
            file_path = os.path.join(dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        print(f"DEBUG: Deleted {len(files)} files in {dir}")
    else:
        os.makedirs(dir)
        print(f"DEBUG: Created folder to store updates files at: {dir}")
print("\n\n")

