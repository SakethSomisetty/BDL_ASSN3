# %% [markdown]
# # Big Data Lab - Assignment 2
# Submitted By - Saketh Somisetty (NA20B065)

"""
Python packages needed:
airflow
pandas
numpy
beautifulsoup4==4.12.3
apache-beam==2.54.0
geopandas==0.14.3
geodatasets==2023.12.0
geopandas==0.14.3
matplotlib==3.8.3

os packages needed:
unzip

"""


# %% [markdown]
# ## Task 1 (DataFetch Pipeline)

# %%
# Importing required modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime
from bs4 import BeautifulSoup # For parsing HTML
import random
import os
import urllib
import shutil # For archive making and moving

# %%
base_url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/'
year = 2001
num_files = 2
archive_output_dir = '/tmp/archives' 

# %%
# following are not changeable in conf of run_dag should be changed only here
data_file_output_dir = '/tmp/data/' + '{{params.year}}/' # give a place to save csv data files if required or else uses a temp location
HTML_file_save_dir = '/tmp/html/' # give a place to save html if required or else uses a temp location

# %%
# Creating a default conf dictionary
conf = dict(
    base_url = base_url,
    year = Param(year, type="integer", minimum=1901,maximum=2024),
    num_files = Param(num_files, type="integer", minimum=1),
    archive_output_dir = archive_output_dir,  
)

# %%
# Define DAG properties
dag_id_t1 = "fetch_ncei_data"
default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Instantiate DAG
dag1 = DAG(
    dag_id = dag_id_t1,
    default_args=default_args,
    params = conf,
    description='A simple data fetch pipeline',
    schedule=None,  # Adjust the schedule as needed
)


# %% [markdown]
# ### Tasks

# %% [markdown]
# 1. Fetch the page containing the location wise datasets for that year. (Bash Operator with
# wget or curl command)

# %%
# Define a dictionary to store parameters for fetching the page
fetch_pg_param = dict(
    base_url = "{{ dag_run.conf.get('base_url', params.base_url) }}",
    file_save_dir = HTML_file_save_dir
    )

# Define a BashOperator task to fetch the page using wget
fetch_page_task = BashOperator(
    task_id=f"download_html_data",
    bash_command="curl {{params.base_url}}{{params.year }}/ --create-dirs -o {{params.file_save_dir}}{{params.year}}.html",
    params = fetch_pg_param,
    dag=dag1,
)


# %% [markdown]
# fetch_page_task.run() to run the fetch code

# %% [markdown]
# 2. Based on the required number of data files, select the data files randomly from the available
# list of files. (Python Operator)

# %%
def parse_page_content(page_data, base_url, year):
    """
    Extracts CSV links from HTML page data for a specific year.

    Args:
        page_data (str): HTML page data containing links.
        base_url (str): The Base Url given for the page
        year (int): The year for which CSV links are to be extracted.

    Returns:
        list: A list of CSV file URLs.
    """
    # Initialize an empty list to store the extracted CSV file URLs
    res = []

    # Create Page URL for result
    page_url = f"{base_url}/{year}/"

    # Parse the HTML page using BeautifulSoup
    soup = BeautifulSoup(page_data, 'html.parser')

    # Find all hyperlinks in the HTML page
    hyperlinks = soup.find_all('a')

    # Iterate through each hyperlink
    for link in hyperlinks:
        # Get the 'href' attribute of the hyperlink
        href = link.get('href')

        # Check if the href contains ".csv"
        if ".csv" in href:
            # Create the full CSV file URL by combining the base page URL and href
            file_url = f'{page_url}{href}'
            
            # Append the CSV file URL to the result list
            res.append(file_url)

    # Return the list of extracted CSV file URLs
    return res

# %%
def select_random_files(num_files, base_url, year_param,file_save_dir,**kwargs):
    # getting data from the downloaded html file from the previous bash comment
    filename = f"{file_save_dir}{year_param}.html"
    with open(filename, "r") as f:
        pages_content = f.read()
    # Parse the page content to extract the list of available files
    available_files = parse_page_content(pages_content,base_url=base_url,year=year_param)

    # Select random files
    selected_files_url = random.sample(available_files, int(num_files))
    
    return selected_files_url


# Define a dictionary to store parameters for selecting files
select_fil_params = dict(
    num_files = "{{ dag_run.conf.get('num_files', params.num_files) }}",
    year_param = "{{ dag_run.conf.get('year', params.year) }}",
    base_url = "{{ dag_run.conf.get('base_url', params.base_url) }}",
    file_save_dir = HTML_file_save_dir
)
# Define Select files task
select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_random_files,
    op_kwargs=select_fil_params,
    dag=dag1,
)


# select_files_task.run() to run the task


# 3. Fetch the individual data files (Bash or Python Operator)

# %%
def download_file(file_url, csv_output_dir):
    # Create directory if not present
    os.makedirs(csv_output_dir, exist_ok=True)
    
    # Extract the file name from the URL and decode any URL-encoded characters
    file_name = urllib.parse.unquote(os.path.basename(file_url))
    
    # Construct the full path where the file will be saved
    file_path = os.path.join(csv_output_dir, file_name)
    
    # Use the curl command to download the file from the given URL and save it to the specified path
    os.system(f"curl {file_url} -o {file_path}")
    
    # Return the name of the downloaded file for potential use in the calling code
    return file_name


# %%
def fetch_individual_files(csv_output_dir,**kwargs):
    ti = kwargs['ti']
    selected_files = ti.xcom_pull(task_ids='select_files')

    for file_url in selected_files:
        # Implementing logic to download each file
        download_file(file_url, csv_output_dir)

# %%

# Defining parameters to pass
fetch_if_param = dict( csv_output_dir = data_file_output_dir )

# Task to download CSV
fetch_files_task = PythonOperator(
    task_id='fetch_files',
    python_callable=fetch_individual_files,
    op_kwargs=fetch_if_param,
    dag=dag1,
)

# %% [markdown]
# 4. Zip them into an archive. (Python Operator)

# %%
def zip_files(output_dir, archive_path, **kwargs):
    # Zip the files in the output directory into an archive
    shutil.make_archive(archive_path, 'zip', output_dir)

#%% 
archive_path = data_file_output_dir[:-1] if data_file_output_dir[-1]=='/' else data_file_output_dir
# %%
#Params for zip_files function
zip_files_params = dict(output_dir = data_file_output_dir,
                        archive_path = archive_path)

# Creating Task
zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    op_kwargs=zip_files_params,
    dag=dag1,
)

# %%
def move_archive(archive_path, target_location, **kwargs):
    # Move the archive to the required location
    os.makedirs(target_location, exist_ok=True)
    # giving exact file path to overwrite if present
    shutil.move(archive_path + '.zip', os.path.join(target_location , str(kwargs['dag_run'].conf.get('year'))) + '.zip')

# %%
# Params for move_archive function
move_archive_params = dict(target_location = "{{ dag_run.conf.get('archive_output_dir', params.archive_output_dir) }}",
                        archive_path = archive_path)

# Creating Task
move_archive_task = PythonOperator(
    task_id='move_archive',
    python_callable=move_archive,
    op_kwargs=move_archive_params,
    dag=dag1,
)

# %%
# Define task dependencies
fetch_page_task >> select_files_task >> fetch_files_task >> zip_files_task >> move_archive_task





#------------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------
# Task 2 (analytic pipeline)

# from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import apache_beam as beam
import pandas as pd
import geopandas as gpd
from geodatasets import get_path
import matplotlib.pyplot as plt
import shutil
import os
import numpy as np
from ast import literal_eval as make_tuple


# Path to your archive file
archive_path = "/tmp/archives/2001.zip"
required_fields = "WindSpeed, BulbTemperature"

conf = dict(
    archive_path = archive_path,
    required_fields = required_fields
)

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'analytics_pipeline',
    default_args=default_args,
    description='An analytics pipeline for data visualization',
    params = conf,
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
)

# Task 1: Wait for the archive to be available
wait_for_archive_task = FileSensor(
    task_id = 'wait_for_archive',
    mode="poke",
    poke_interval = 5,  # Check every 5 seconds
    timeout = 5,  # Timeout after 5 seconds
    filepath = "{{params.archive_path}}",
    dag=dag,
    fs_conn_id = "my_file_system", # File path system must be defined
)

# Task 2: Unzip the archive
unzip_archive_task = BashOperator(
    task_id='unzip_archive',
    bash_command="unzip -o {{params.archive_path}} -d /tmp/data2",
    dag=dag,
)



# Task 3: Extract CSV contents and filter data
def parseCSV(data):
        # Parsing fields from the CSV
        df = data.split('","')
        df[0] = df[0].strip('"')
        df[-1] = df[-1].strip('"')
        return list(df)


class ExtractAndFilterFields(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers_csv):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(ind)
        self.headers = {i:ind for ind,i in enumerate(headers_csv)} # defining headers

    def process(self, element):
        headers = self.headers 
        # Extract required fields from the CSV
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.required_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
        # Return a tuple of the form <Lat, Lon, [[Windspeed]]>
            yield ((lat, lon), data)



def process_csv_files(required_fields,**kwargs):
    required_fields = list(map(lambda a:a.strip(),required_fields.split(",")))
    # Logic to process CSV files using Apache Beam
    os.makedirs('/tmp/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        # Apache Beam pipeline code
        # Extract, filter, and create tuples
        result = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('/tmp/data2/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'FilterAndCreateTuple' >> beam.ParDo(ExtractAndFilterFields(required_fields=required_fields))
            | 'CombineTuple' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )

        # Do something with the result, for example, save it to a file
        result | 'WriteToText' >> beam.io.WriteToText('/tmp/results/result.txt')
required_f = dict(
    required_fields = "{{ params.required_fields }}",
)

process_csv_files_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv_files,
    op_kwargs = required_f,
    dag=dag,
)

class ExtractFieldsWithMonth(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers_csv):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(ind)

        self.headers = {i:ind for ind,i in enumerate(headers_csv)} # defining headers

    def process(self, element):
        headers = self.headers 
        # Extract required fields from the CSV
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.required_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
            # Extracting Month
            Measuretime = datetime.strptime(element[headers['DATE']],'%Y-%m-%dT%H:%M:%S')
            Month_format = "%Y-%m"
            Month = Measuretime.strftime(Month_format)
            # Return a tuple of the form <Lat, Lon, [[Windspeed]]>
            yield ((Month, lat, lon), data)

# Task 4: Compute monthly averages

def compute_averages(data):
    # Function to compute monthly averages
    val_data = np.array(data[1])
    val_data_shape = val_data.shape
    val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') # converting to float removing empty string and replace with nan
    val_data = np.reshape(val_data,val_data_shape)
    masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
    res = np.ma.average(masked_data, axis=0)
    res = list(res.filled(np.nan))
    return ((data[0][1],data[0][2]),res)

def compute_monthly_averages(required_fields, **kwargs):
    # Logic to compute monthly averages using Apache Beam
    required_fields = list(map(lambda a:a.strip(),required_fields.split(",")))
    os.makedirs('/tmp/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        # Apache Beam pipeline code
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/tmp/data2/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'CreateTupleWithMonthInKey' >> beam.ParDo(ExtractFieldsWithMonth(required_fields=required_fields))
            | 'CombineTupleMonthly' >> beam.GroupByKey()
            | 'ComputeAverages' >> beam.Map(lambda data: compute_averages(data))
            | 'CombineTuplewithAverages' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )
        # save result to a file
        result | 'WriteAveragesToText' >> beam.io.WriteToText('/tmp/results/averages.txt')
        

compute_monthly_averages_task = PythonOperator(
    task_id='compute_monthly_averages',
    python_callable=compute_monthly_averages,
    op_kwargs = required_f,
    dag=dag,
)

class Aggregated(beam.CombineFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for i in headers_csv:
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(i.replace('Hourly',''))

    def create_accumulator(self):
        return []
    
    def add_input(self, accumulator, element):
        accumulator2 = {key:value for key,value in accumulator}
        data = element[2]
        val_data = np.array(data)
        val_data_shape = val_data.shape
        val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') # converting to float removing empty string and replace with nan
        val_data = np.reshape(val_data,val_data_shape)
        masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
        res = np.ma.average(masked_data, axis=0)
        res = list(res.filled(np.nan))
        for ind,i in enumerate(self.required_fields):
            accumulator2[i] = accumulator2.get(i,[]) + [(element[0],element[1],res[ind])]

        return list(accumulator2.items())
    
    def merge_accumulators(self, accumulators):
        merged = {}
        for a in accumulators:
                a2 = {key:value for key,value in a}
                for i in self.required_fields:
                    merged[i] = merged.get(i,[]) + a2.get(i,[])

        return list(merged.items())
    
    def extract_output(self, accumulator):
        return accumulator
    

def plotgeomaps(values):
    rmindx = []
    for i in range(len(values[1])):
        if values[1][i][0] == '':
            rmindx.append(i)

    for i in rmindx[::-1]:
        del values[1][i]

    d1 = np.array(values[1],dtype='float')
    # Load a sample Map of world to plot
    world = gpd.read_file(get_path('naturalearth.land'))

    # Create a GeoDataFrame with some random data for demonstration
    data = gpd.GeoDataFrame({
        values[0]:d1[:,2]
    }, geometry=gpd.points_from_xy(*d1[:,(1,0)].T))
    # Plotting
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))

    world.plot(ax=ax, color='white', edgecolor='black')
    # Plot Field1 heatmap
    data.plot(column=values[0], cmap='viridis', marker='o', markersize=150, ax=ax, legend=True)
    ax.set_title(f'{values[0]} Heatmap')
    os.makedirs('/tmp/results/plots', exist_ok=True)
    # Save the plot to PNG
    plt.savefig(f'/tmp/results/plots/{values[0]}_heatmap_plot.png')

# Task 5: Create visualization using geopandas
def create_heatmap_visualization(required_fields,**kwargs):
    # Logic to create heatmaps using geopandas
    required_fields = list(map(lambda a:a.strip(),required_fields.split(",")))
    with beam.Pipeline(runner='DirectRunner') as p:
        # Apache Beam pipeline code
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/tmp/results/averages.txt*')
            | 'preprocessParse' >>  beam.Map(lambda a:make_tuple(a.replace('nan', 'None')))
            | 'Global aggregation' >> beam.CombineGlobally(Aggregated(required_fields = required_fields))
            | 'Flat Map' >> beam.FlatMap(lambda a:a) 
            | 'Plot Geomaps' >> beam.Map(plotgeomaps)            
        )

create_heatmap_visualization_task = PythonOperator(
    task_id='create_heatmap_visualization',
    python_callable=create_heatmap_visualization,
    op_kwargs = required_f,
    dag=dag,
)

# Task 7: Delete CSV file
def delete_csv_file(**kwargs):
    # Logic to delete the CSV file after successful completion
    shutil.rmtree('/tmp/data2',ignore_errors=True)

delete_csv_file_task = PythonOperator(
    task_id='delete_csv_file',
    python_callable=delete_csv_file,
    dag=dag,
)

# Set task dependencies
wait_for_archive_task >> unzip_archive_task >> process_csv_files_task
process_csv_files_task >> delete_csv_file_task
unzip_archive_task >> compute_monthly_averages_task
compute_monthly_averages_task >> create_heatmap_visualization_task
create_heatmap_visualization_task >> delete_csv_file_task
