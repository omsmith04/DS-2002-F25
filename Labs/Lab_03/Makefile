.PHONY: all fetch analyze clean

analyze: fetch
	echo "starting data analysis..."
	./analyze_data.sh
	echo "Data analysis complete. check 'weather_report.csv' for results"

fetch:
	echo "starting data fetch..."
	./fetch_metars.sh
	echo "Data fetching complete. raw json files saved to the 'raw_metars/' directory."

clean:
	echo "cleaning up files and directories..."
	rm -rf raw_metars/weather_report.csv
	echo "clean up complete. directory/files have been removed"
