from dotenv import load_dotenv
import os

load_dotenv()

CONFIG = {
    "server_url": os.getenv('SERVER_URL'),
    "input_dir": os.getenv('INPUT_DIR'),
    "watermarkDuration": os.getenv('WATERMARK_DURATION'),
    "windowDuration": os.getenv('WINDOW_DURATION'),
    "slideDuration": os.getenv('SLIDE_DURATION'),
    "output_format": os.getenv('OUTPUT_FORMAT')
}

# check if the output directory exists, and if it does, delete all files in the directory
input_dir = CONFIG["input_dir"]
if os.path.exists(input_dir):
    files = os.listdir(input_dir)
    for file in files:
        os.remove(os.path.join(input_dir, file))
    print(f"Deleted {len(files)} files in {input_dir}")
else:
    os.makedirs(input_dir)