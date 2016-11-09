# NYC School Crimes

Check out the finished project [here](http://schools.brianhamilton.me/).

## Installation

1. Download Python and [PostgreSQL](https://www.postgresql.org/).

2. Install the [psycopg2](http://initd.org/psycopg/) and [geopy](https://github.com/geopy/geopy) Python libraries.

3. Set up a local database called **school_data** (or anything you'd like).

4. Clone the repository or download the zip file for this project.

5. Use terminal/cmd/powershell/something similar to navigate to the directory with the files and type the command below. This will build out the local database you've set up.

    ```
    python initialize_db.py
    ```
    
---

The geocoding and finished output/analysis function calls are commented out for simplicity. Just simply uncomment them to run them. For now, the geocoded coordinates and output files are included in this repository.

In order to run the geocoding API calls youself, you will need to get a Google Maps API key.

The rest of the project, including the source code for the Node.js/React.js, application will be added soon.
