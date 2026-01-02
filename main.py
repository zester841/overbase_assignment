import logging
from pandarallel import pandarallel
from src.pipeline import Pipeline

# Constants
CONFIG_FILE = "pipeline_config.json"
INPUT_FILE = "cmo_videos_names.csv"
OUTPUT_FILE = "final_execs.csv"

if __name__ == "__main__":
    # Setup Logging
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    # Initialize shared memory for parallelism
    pandarallel.initialize(progress_bar=True, verbose=0)
    
    # Run Application
    app = Pipeline(CONFIG_FILE, INPUT_FILE, OUTPUT_FILE)
    app.run()