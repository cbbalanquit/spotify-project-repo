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
### Airflow Dags
### SQL Server
### Power BI
