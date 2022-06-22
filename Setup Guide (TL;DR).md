## Setting Up Guide

In here, we will explore more about the detailed steps for the Data Pipeline we have in this Spotify ETL Project.

### Spotify Web API
The very first thing we need to do is to setup Spotify Web API to get the desired content from Spotify. If you don't have a spotify account yet, sign up first in www.spotify.com since you can't have access to Spotify Web API without having an account

These are the three(3) steps needed to setup our Spotify Web API:
1. Register an [application](https://developer.spotify.com/documentation/general/guides/authorization/app-settings/) with Spotify.
-- make sure that you save your client_id, redirect_uri and client_secret as this will be used in the next steps.
3. Authenticate a user and get authorization to access user data </br>
There are 3 types of authentication a user can use:
a. Authorization Code Flow
b. Client Credentials
c. Implicit grant
-- In order to make our app have infinite access to our Spotify Web API, we will use the [Authorization Code Flow](https://developer.spotify.com/documentation/general/guides/authorization/code-flow/) If authentication and authorization is successful, an access token will be given which can be used to request a refresh token everytime the app will run. Keep in mind that the refresh token is set to expire in an hour. 
5. Retrieve the data from a Web API endpoint. In this project, we use the [me/player/recently-played](https://developer.spotify.com/documentation/web-api/reference/#/operations/get-recently-played) endpoint

For the first step,


### Docker and Airflow
### Airflow DAGs
Since we will be using Airflow as our orchestration tool, we will use DAGs. In this project, the DAG setup will have 2 steps.
<a href="https://drive.google.com/uc?export=view&id="><img src="https://drive.google.com/uc?export=view&id=1TNmztfTaKUKNzUp2sgzMy2N_GPf8U3X_" style="width: 360px; max-width: 100%; height: auto" title="Click for the larger version." /></a>

In the first part of the dags, which is the 'run_spotify_etl', it will start from getting the refresh token using POST request to Spotify Web API. Then, using the token, GET request to the API to get the response which contains the data from the recently_played_tracks of the Spotify user. This response will be converted to JSON object. Using a for loop, needed data will be stored to set of lists which will be converted to a dataframe. Then, data validation will be done to check if there are (a) empty or has blank data received, (b) the song is played yesterday. If not, remove the records with playtime not matching yesterday, (c) There are duplicates in the data received. Duplicate data are removed
Check if there are null data. Remove records containing null values

### SQL Server
Before running the ETL Pipeline in Airflow, make sure that the SQL Server Database is already setup. In this project, one database, 2 tables, 1 stored procedure, and 1 job is setup in the SQL Server Management Studio.

In creating the database, the following SQL code is used.
```
CREATE DATABASE database_name;
```

For the staging table, we will create a table with the following columns and data types.
```
CREATE TABLE table_name (
        song_name VARCHAR(200),
        played_at VARCHAR(200),
        song_duration_ms
        song_type VARCHAR(200),
        is_explicit BIT,
        is_local BIT,
        popularity INT,
        artist_name VARCHAR(200),
        artist_type VARCHAR(200),
        album_name VARCHAR(200),
        album_type VARCHAR(200),
        album_releasedate VARCHAR(200)
);
```
For the final table, almost similar with the staging table, we created a table with the same fields but using DATETIME2 data type for the played_at and additional of album_releasedate_rem field with VARCHAR datatype
```
CREATE TABLE table_name (
    song_name VARCHAR(200),
    played_at DATETIME2,
    song_duration_ms
    song_type VARCHAR(200),
    is_explicit BIT,
    is_local BIT,
    popularity INT,
    artist_name VARCHAR(200),
    artist_type VARCHAR(200),
    album_name VARCHAR(200),
    album_type VARCHAR(200),
    album_releasedate VARCHAR(200),
    album_releasedate_rem VARCHAR(13)
);
```
After creating the two tables, a stored procedure is created as preparation for the regularly running SQL Server Job which will convert the played_at from iso timestamp to datetime and create new column based from the album_releasedate.
```
USE database_name
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[stored_procedure_name]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

SELECT
    played_at
INTO #not_yet_included
FROM [spotify_project].[dbo].[staging_table_name]
WHERE CONVERT(DATETIME2(0), played_at, 102) NOT IN (
    SELECT
        played_at
    FROM [spotify_project].[dbo].[final_table_name]
    )

INSERT INTO [spotify_project].[dbo].[final_table_name]
SELECT
    song_name,
    CONVERT(DATETIME2(0), played_at, 102) played_at,
    song_duration_ms,
    song_type,
    is_explicit,
    is_local,
    popularity,
    artist_name,
    artist_type,
    album_name,
    album_type,
    (CASE WHEN LEN(album_releasedate) = 10 THEN CAST(album_releasedate AS DATE)
      ELSE CAST(CONCAT(album_releasedate,'-01-01') AS DATE) END) album_releasedate,
    (CASE WHEN LEN(album_releasedate) = 10 THEN 'complete_date'
      ELSE 'year_only' END) album_releasedate_rem
FROM [staging_table_name]
WHERE played_at IN (
    SELECT played_at
    FROM #not_yet_included)

END
```

As the last part in setting up SQL Server, a job is created using SQL Server Agent. Just follow the official [documentation](https://docs.microsoft.com/en-us/sql/ssms/agent/create-a-job?view=sql-server-ver16) of microsoft.
In the 'steps' tab, input a command that will execute a command which will run the stored procedure created from previous step:
> EXECUTE [dbo].[stored_procedure_name]

And in the 'schedule' tab, set the preferred schedule in running the step set in the job (in this project, just executing the stored_procedure created).
In my pipeline, I set it to 8:30AM and 8:00PM

### Power BI
