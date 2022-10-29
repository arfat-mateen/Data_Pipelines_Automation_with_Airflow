<div id="top"></div>

[![LinkedIn][linkedin-shield]][linkedin-url]
![Generic badge](https://img.shields.io/badge/Project-Pass-green.svg)

<!-- PROJECT HEADER -->
<br />
<div align="center">
  <a href="#">
    <img src="images/udacity.svg" alt="Logo" width="200" height="200">
  </a>

  <h3 align="center">Data Pipelines Automation with Airflow</h3>

  <p align="center">
    Database Schema & ETL pipeline for Song Play Analysis 
    <br />
    <br />
    -----------------------------------------------
    <br />
    <br />
    Data Engineer for AI Applications Nanodegree
    <br />
    Bosch AI Talent Accelerator Scholarship Program
  </p>
</div>

<br />

<!-- TABLE OF CONTENTS -->
<details open>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li><a href="#datasets">Datasets</a></li>
    <li><a href="#file-structure">File Structure</a></li>
    <li><a href="#how-to-run">How To Run</a></li>
    <li><a href="#database-schema-design">Database Schema Design</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<br/>

<!-- ABOUT THE PROJECT -->

## About The Project

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and has come to the conclusion that the best tool to achieve this is Apache Airflow.

The startup wants to create high-grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that data quality plays a big part when analyses are executed on top of the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The project defines dimension and fact tables for a star schema and creates an ETL pipeline that transforms data from JSON files present in an S3 bucket into these database tables hosted on Redshift for a particular analytic focus. The project utilises custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

<p align="right">(<a href="#top">back to top</a>)</p>

### Built With

-   [![AIRFLOW][airflow-shield]][airflow-url]
-   [![AWS][aws-shield]][aws-url]
-   [![Python][python-shield]][python-url]
-   [![Jupyter][jupyter-shield]][jupyter-url]
-   [![VSCode][vscode-shield]][vscode-url]

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- FILE STRUCTURE -->

## Datasets

Dataset available in following S3 buckets:

1.  Song data: s3://udacity-dend/song_data
2.  Log data: s3://udacity-dend/log_data

    -   `Song Dataset`: Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

        ```
        song_data/A/B/C/TRABCEI128F424C983.json
        song_data/A/A/B/TRAABJL12903CDCF1A.json
        ```

        And below is an example of what a single song file looks like.

        ![Song Data][song-dataset]

         <br />

    -   `Log Dataset`: These files are also in JSON format and contains user activity data from a music streaming app. The log files are partitioned by year and month. For example, here are filepaths to two files in this dataset.

        ```
        log_data/2018/11/2018-11-12-events.json
        log_data/2018/11/2018-11-13-events.json
        ```

        And below is an example of what the data in a log file looks like.

        ![Log Data][log-dataset]

         <br />

<p align="right">(<a href="#top">back to top</a>)</p>

## File Structure

1. `create_tables.sql` Contains the SQL queries used to create all the required tables in Redshift.

2. `sparkify_dag.py` Contains the task code and dependencies of the DAG.

3. `load_dim_table_subdag.py` Contains the code for the SUBDAG task to load data for the dimensions table.

4. `sql_queries.py` Contains the SQL queries used in the ETL process.

5. `stage_redshift.py` Contains code for `StageToRedshiftOperator` which copies JSON data from S3 to staging tables on Redshift.

6. `load_dimension.py` Contains code for `LoadDimensionOperator` which loads data for dimension tables from the data present in the staging tables.

7. `load_fact.py` Contains code for `LoadFactOperator` which loads data for the fact table from the data present in the staging tables.

8. `data_quality.py` Contains code for `DataQualityOperator` which runs a data quality check by passing an SQL query and expected result as arguments, failing if the expected results don't match.

9. `README.md` provides details on the project.

<p align="right">(<a href="#top">back to top</a>)</p>

## How To Run

### Prerequisite

-   AWS Account.

-   IAM role with `Programmatic access` and permissions set to following:

    -   AdministratorAccess
    -   AmazonRedshiftFullAccess
    -   AmazonS3FullAccess

-   Redshift cluster - VPC Security group with inbound rules appropriately set as below:

    ```
    Type: Custom TCP Rule.
    Protocol: TCP.
    Port Range: 5439,
    Source: Custom IP, with 0.0.0.0/0
    ```

-   Setup parameters in Airflow web UI

    -   Go to Admin > Connections apge and `Create` the two connections as following:

        -   Conn Id: `aws_credentials`
        -   Conn Type: `Amazon Web Services`
        -   Login: `<AWS_ACCESS_KEY_ID>`
        -   Password: `<AWS_SECRET_ACCESS_KEY>`

        Click `Save` and add another

        -   Conn Id: `redshift`
        -   Conn Type: `Postgres`
        -   Host: `<Redshift cluster endpoint from redshift>`
        -   Schema: `dev`
        -   Login: `awsuser`
        -   Password: `<Redshift db password from redshift>`
        -   Port: `5439`

        Click `Save`

    -   Go to Admin > Variables apge and `Create` the two variables as following:

        -   key: `s3_bucket`
        -   value: `udacity-dend`

        Click `Save` and add another

        -   key: `region`
        -   value: `us-west-2`

        Click `Save`

### Running scripts

-   Execute the sparkify_dag inside an Airflow environment and run the dag on Airflow Web UI.

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- DATABASE SCHEMA & ETL PIPELINE -->

## Database Schema Design

Database schema consist five tables with the following fact and dimension tables:

-   Fact Table

    1. `songplays`: records in log data associated with song plays filter by `NextSong` page value.
       The table contains songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location and user_agent columns.

<br/>

-   Dimension Tables

    2. `users`: stores the user data available in the app. The table contains user_id, first_name, last_name, gender and level columns.

    3. `songs`: contains songs data. The table consist of the following columns song_id, title, artist_id, year and duration.

    4. `artists`: artists in the database. The table contains artist_id, name, location, latitude and longitude columns.

    5. `time`: timestamps of records in `songplays` broken down into specific units with the following columns start_time, hour, day, week, month, year and weekday.

    <br/>

    ![Sparkifydb ERD][sparkifydb-erd]

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->

## Acknowledgments

-   [Udacity](https://www.udacity.com/)
-   [Bosch AI Talent Accelerator](https://www.udacity.com/scholarships/bosch-ai-talent-accelerator)
-   [Img Shields](https://shields.io)
-   [Best README Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->

[linkedin-shield]: https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white
[airflow-shield]: https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white
[aws-shield]: https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white
[jupyter-shield]: https://img.shields.io/badge/Made%20with-Jupyter-orange?style=for-the-badge&logo=Jupyter
[vscode-shield]: https://img.shields.io/badge/Visual%20Studio%20Code-0078d7.svg?style=for-the-badge&logo=visual-studio-code&logoColor=white
[linkedin-url]: https://www.linkedin.com/in/arfat-mateen
[python-url]: https://www.python.org/
[airflow-url]: https://airflow.apache.org/
[aws-url]: https://aws.amazon.com/
[jupyter-url]: https://jupyter.org/
[vscode-url]: https://code.visualstudio.com/
[song-dataset]: images/song_data.png
[log-dataset]: images/log_data.png
[sparkifydb-erd]: images/sparkifydb_erd.png
[example-query]: images/example_query.png
