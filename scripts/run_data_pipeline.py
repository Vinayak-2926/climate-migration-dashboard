import subprocess
import sys

def run_script(module_path):
    """Runs a Python module and prints its output in real-time."""
    print(f"--- Running {module_path} ---")
    process = subprocess.Popen(
        [sys.executable, "-m", module_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    for line in iter(process.stdout.readline, ""):
        print(line, end="")

    process.stdout.close()
    return_code = process.wait()

    if return_code:
        print(f"--- Error running {module_path} ---")
        raise subprocess.CalledProcessError(return_code, module_path)

    print(f"--- Finished {module_path} ---")

if __name__ == "__main__":
    scripts = [
        "preprocessing.acquisition.download_counties",
        "preprocessing.acquisition.download_raw_data",
        "preprocessing.cleaning.convert_xlsx_to_csvs",
        "preprocessing.analysis.historical_population",
        "preprocessing.analysis.population_forecasting",
        "preprocessing.cleaning.clean_data",
        "preprocessing.analysis.indicator_forecasting",
        "preprocessing.database.update_database",
    ]

    try:
        for script in scripts:
            run_script(script)
        print("PostgreSQL updated successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Pipeline failed at script: {e.cmd}")
        sys.exit(1)

