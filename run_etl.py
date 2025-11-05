import os
import sys
import time
import logging
import multiprocessing as mp
from datetime import datetime
from importlib import import_module
from concurrent.futures import ProcessPoolExecutor, wait, ALL_COMPLETED

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_script(script_name):
    """Run a script module and handle any errors."""
    try:
        logger.info(f"Starting {script_name}")
        module = import_module(script_name)
        if hasattr(module, 'main'):
            module.main()
        logger.info(f"Completed {script_name}")
        return True, script_name
    except Exception as e:
        logger.error(f"Error running {script_name}: {str(e)}")
        return False, script_name

def main():
    # Add scripts directory to Python path
    scripts_dir = os.path.join(os.path.dirname(__file__), 'scripts')
    sys.path.append(scripts_dir)

    start_time = datetime.now()
    logger.info(f"Starting ETL pipeline at {start_time}")

    # First run load_raw
    success, _ = run_script('load_raw')
    if not success:
        logger.error("Pipeline failed at load_raw")
        sys.exit(1)

    # Then run all other scripts in parallel
    parallel_scripts = [
        'property',
        'hoa',
        'leads',
        'taxes',
        'rehab',
        'valuation'
    ]

    # Use ProcessPoolExecutor to run scripts in parallel
    max_workers = min(len(parallel_scripts), mp.cpu_count())
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all scripts to run in parallel
        future_to_script = {
            executor.submit(run_script, script): script 
            for script in parallel_scripts
        }
        
        # Wait for all scripts to complete
        results = wait(future_to_script.keys(), return_when=ALL_COMPLETED)
        
        # Check results
        success = True
        failed_scripts = []
        for future in results.done:
            result, script_name = future.result()
            if not result:
                success = False
                failed_scripts.append(script_name)

    end_time = datetime.now()
    duration = end_time - start_time
    
    if success:
        logger.info(f"Pipeline completed successfully in {duration}")
    else:
        logger.error(f"Pipeline failed. Failed scripts: {failed_scripts}")
        logger.error(f"Pipeline failed after running for {duration}")
        sys.exit(1)

if __name__ == '__main__':
    # Required for Windows compatibility
    mp.freeze_support()
    main()