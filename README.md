## Running the docker

To get started run ```docker-compose up --force-recreate --build``` in root directory.

## Task: Data ETL
The data generated above needs to be pulled, transformed and saved into a new database
environment. Create an ETL pipeline that does the following:
- Pull the data from PostgresSQL
- Calculate the following data aggregations:
  - a. The maximum temperatures measured for every device per hours.
  - b. The amount of data points aggregated for every device per hours.
  - c. Total distance of device movement for every device per hours.
- Store this aggregated data into the provided MySQL database <br>

To determine the distance between two locations, you can utilize the following formula or a
relevant python/postgresql package:

``` distance = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * 6371 (where 6371 represents the radius of the Earth in kilometers). ```

For assistance with this task, you may find this link helpful:
https://geopy.readthedocs.io/en/stable/#module-geopy.distance

This ETL should live inside the provided docker container and run by the docker-compose
command.

## Script Running
![image](https://github.com/Matheus-Barros/pair-finance-challenge/assets/51465352/221013a5-71d0-4055-b594-f228e573c245)


