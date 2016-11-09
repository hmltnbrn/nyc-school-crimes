import psycopg2
import csv
import time
from geopy.geocoders import GoogleV3

raw_crimes = """CREATE TABLE raw_crimes(
   School_Year                VARCHAR
  ,Building_Code              VARCHAR
  ,DBN                        VARCHAR
  ,Location_Name              VARCHAR
  ,Location_Code              VARCHAR
  ,Address                    VARCHAR
  ,Borough                    VARCHAR
  ,Geographical_District_Code INTEGER 
  ,Register                   REAL
  ,Building_Name              VARCHAR
  ,Schools                    INTEGER 
  ,Schools_in_Building        VARCHAR
  ,Major_N                    INTEGER 
  ,Oth_N                      INTEGER 
  ,NoCrim_N                   INTEGER 
  ,Prop_N                     INTEGER 
  ,Vio_N                      INTEGER 
  ,ENGroupA                   VARCHAR
  ,RangeA                     VARCHAR
  ,AvgOfMajor_N               VARCHAR
  ,AvgOfOth_N                 VARCHAR
  ,AvgOfNoCrim_N              VARCHAR
  ,AvgOfProp_N                VARCHAR
  ,AvgOfVio_N                 VARCHAR
);COPY raw_crimes FROM STDIN WITH CSV HEADER DELIMITER AS ',' NULL AS 'NULL'"""

school_info = """CREATE TABLE school_info(
   Building_Code              VARCHAR PRIMARY KEY
  ,Location_Name              VARCHAR
  ,Address                    VARCHAR
  ,Borough                    VARCHAR
  ,Geographical_District_Code INTEGER 
  ,Register                   REAL
  ,Schools                    INTEGER 
  ,Schools_in_Building        VARCHAR
  ,ENGroupA                   VARCHAR
  ,RangeA                     VARCHAR
);WITH r1 AS (SELECT building_code, location_name, address, borough, geographical_district_code, register, schools, schools_in_building, engroupa, rangea
    FROM raw_crimes
    WHERE school_year = '2014-15' AND major_n IS NOT NULL AND oth_n IS NOT NULL AND nocrim_n IS NOT NULL AND prop_n IS NOT NULL AND vio_n IS NOT NULL
    ORDER BY building_code)
INSERT INTO school_info SELECT * FROM r1;"""

school_crimes = """CREATE TABLE school_crimes(
   Building_Code              VARCHAR PRIMARY KEY
  ,Major_N                    REAL 
  ,Oth_N                      REAL 
  ,NoCrim_N                   REAL 
  ,Prop_N                     REAL 
  ,Vio_N                      REAL 
);WITH r2 AS (SELECT building_code, sum(major_n) as major_n, sum(oth_n) as oth_n, sum(nocrim_n) as nocrim_n, sum(prop_n) as prop_n, sum(vio_n) as vio_n
    FROM raw_crimes
    WHERE school_year = '2014-15'
    GROUP BY building_code)
INSERT INTO school_crimes SELECT * FROM r2;"""

school_crime_avg = """(SELECT cr.building_code, co.location, cr.major_n/i.register*r.register_avg AS major_n, 
cr.oth_n/i.register*r.register_avg AS oth_n, cr.nocrim_n/i.register*r.register_avg AS nocrim_n, 
cr.prop_n/i.register*r.register_avg AS prop_n, cr.vio_n/i.register*r.register_avg AS vio_n 
FROM school_info i, school_crimes cr, school_coords co, (SELECT AVG(register) AS register_avg FROM school_info) r 
WHERE cr.building_code = co.building_code AND i.building_code = cr.building_code)"""

borough_avg = """(SELECT i.borough, AVG(n.major_n) AS major_n_avg, AVG(n.oth_n) AS oth_n_avg, AVG(n.nocrim_n) AS nocrim_n_avg, AVG(n.prop_n) AS prop_n_avg, AVG(n.vio_n) as vio_n_avg
  FROM (SELECT cr.building_code, cr.major_n/i.register*r.register_avg AS major_n, cr.oth_n/i.register*r.register_avg AS oth_n,
  cr.nocrim_n/i.register*r.register_avg AS nocrim_n, cr.prop_n/i.register*r.register_avg AS prop_n,
  cr.vio_n/i.register*r.register_avg AS vio_n from school_info i, school_crimes cr, (SELECT AVG(register) AS register_avg FROM school_info) r 
  WHERE cr.building_code = i.building_code) n, school_info i 
  WHERE i.building_code = n.building_code AND i.borough IN ('M','K','Q','X','R') 
  GROUP BY i.borough 
  ORDER BY i.borough)"""

district_avg = """(SELECT i.geographical_district_code, AVG(n.major_n) AS major_n_avg, AVG(n.oth_n) AS oth_n_avg, AVG(n.nocrim_n) AS nocrim_n_avg, AVG(n.prop_n) AS prop_n_avg, AVG(n.vio_n) AS vio_n_avg 
  FROM (SELECT cr.building_code, cr.major_n/i.register*r.register_avg AS major_n, cr.oth_n/i.register*r.register_avg AS oth_n, 
  cr.nocrim_n/i.register*r.register_avg AS nocrim_n, cr.prop_n/i.register*r.register_avg AS prop_n, 
  cr.vio_n/i.register*r.register_avg AS vio_n from school_info i, school_crimes cr, (SELECT AVG(register) AS register_avg FROM school_info) r 
  WHERE cr.building_code = i.building_code) n, school_info i 
  WHERE i.building_code = n.building_code AND i.geographical_district_code IS NOT NULL AND i.geographical_district_code <> 0 
  GROUP BY i.geographical_district_code 
  ORDER BY i.geographical_district_code)"""

def process_file(conn, table_name, is_file, file_object):

    cur = conn.cursor()

    if(is_file == True):
        cur.copy_expert(sql=table_name, file=file_object)
    else:
        cur.execute(table_name)

    conn.commit()
    cur.close()

def dump_csv(conn, table_name_out):

    cur = conn.cursor()

    if table_name_out == 'borough_avg':
        table_name = borough_avg
    elif table_name_out == 'district_avg':
        table_name = district_avg
    elif table_name_out == 'school_crime_avg':
        table_name = school_crime_avg
    else:
        table_name = table_name_out

    with open('output/' + table_name_out + '.csv', 'wb+') as f:
        cur.copy_expert(sql="COPY " + table_name + " TO STDOUT WITH CSV HEADER", file=f)

    cur.close()

def clean_raw_data():

    raw_crime_data = []

    with open('input/School_Safety_Report.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            raw_crime_data.append(row)

    new_raw_crime_data = []
    line = []

    for i in xrange(len(raw_crime_data)):
        for j in xrange(len(raw_crime_data[i])):
            if (raw_crime_data[i][j] == 'N/A' or raw_crime_data[i][j] == '#N/A' or raw_crime_data[i][j] == ''):
                newJ = 'NULL'
            elif (j==8 and i!=0):
                newJ = int(raw_crime_data[i][j].replace(',', ''))
            else:
                newJ = raw_crime_data[i][j]
            line.append(newJ)

        new_raw_crime_data.append(line)
        line = []

    with open('cleaned/cleaned_raw.csv', 'wb+') as fp:
        a = csv.writer(fp, delimiter=',')
        a.writerows(new_raw_crime_data)

def geocode(geolocator, conn):

    cur = conn.cursor()
    cur.execute("SELECT * FROM school_info;")
    raw_crime_data = cur.fetchall()
    cur.close()

    raw_crime_data_coord = []

    for i in xrange(len(raw_crime_data)):
        raw_crime_data_coord.append([raw_crime_data[i][0], raw_crime_data[i][2]])

    for i in xrange(len(raw_crime_data_coord)):
        time.sleep(2)
        print raw_crime_data_coord[i][0]
        location = geolocator.geocode(raw_crime_data_coord[i][1] + " New York City")
        raw_crime_data_coord[i].append(str(location.latitude) + ", " + str(location.longitude))

    with open('cleaned/schools_coords.csv', 'wb+') as fp:
        a = csv.writer(fp, delimiter=',')
        a.writerows(raw_crime_data_coord)

def insert_geocode(conn):

    raw_crime_data_coord = open("cleaned/schools_coords.csv")

    cur = conn.cursor()
    query = """CREATE TABLE school_coords(building_code VARCHAR PRIMARY KEY, address VARCHAR, location VARCHAR);COPY school_coords(building_code, address, location) FROM STDIN WITH CSV HEADER DELIMITER AS ',' NULL AS 'NULL'"""
    cur.copy_expert(sql=query, file=raw_crime_data_coord)
    conn.commit()
    cur.close()

if __name__ == "__main__":

    clean_raw_data()

    raw_crime_file = open("cleaned/cleaned_raw.csv")

    # connection for PostgreSQL database -- change to whatever you have
    conn = psycopg2.connect(database="school_data", user="postgres", password="password")

    # for the initial creation of the database tables
    is_file = False
    file_object = None
    for i in [raw_crimes, school_info, school_crimes]:
        if i == raw_crimes:
            is_file = True
            file_object = raw_crime_file
        process_file(conn, i, is_file, file_object)

    # geolocator = GoogleV3(api_key='<API key goes here>', domain='maps.googleapis.com', scheme='https', timeout=10000)

    # geocode(geolocator, conn) #grab data from database and geocode address
    insert_geocode(conn) #insert geocoded data from csv into database

    # for dumping tables and analysis to CSV
    # for i in ['raw_crimes', 'school_info', 'school_crimes', 'school_coords', 'school_crime_avg', 'borough_avg', 'district_avg']:
    #     dump_csv(conn, i)

    conn.close()
