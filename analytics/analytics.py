from os import environ
from time import sleep
from sqlalchemy import create_engine, text, Column, Integer, String, DateTime
from sqlalchemy.exc import OperationalError
from datetime import datetime, timedelta
import json
from math import radians, sin, cos, acos
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect

# Waiting for the data generator
print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

# Establish connection to PostgreSQL
while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgreSQL successful.')

# Create a connection engine to MySQL
engine = create_engine(environ["MYSQL_CS"])

# Create a model class
Base = declarative_base()

class MyTableResult(Base):
    """
    Model class representing the 'devices_data_agg' table in the MySQL database.
    """
    __tablename__ = 'devices_data_agg'
    device_id = Column(String(100), primary_key=True)
    time = Column(DateTime)
    max_temperature = Column(Integer)
    data_points = Column(Integer)
    total_distance = Column(Integer)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Check if the table exists
inspector = inspect(engine)
if inspector.has_table(MyTableResult.__tablename__):
    # Drop the table if it exists
    MyTableResult.__table__.drop(engine)
    print(f'Table {MyTableResult.__tablename__} dropped.')

# Create the table
Base.metadata.create_all(engine)
print(f'Table {MyTableResult.__tablename__} created.')


def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two coordinates using the Haversine formula.

    Args:
        lat1 (float): Latitude of the first coordinate.
        lon1 (float): Longitude of the first coordinate.
        lat2 (float): Latitude of the second coordinate.
        lon2 (float): Longitude of the second coordinate.

    Returns:
        float: Distance between the two coordinates in kilometers.
    """
    lat1 = radians(float(lat1))
    lon1 = radians(float(lon1))
    lat2 = radians(float(lat2))
    lon2 = radians(float(lon2))

    distance = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * 6371  # Earth's radius in kilometers
    return distance


def etl():
    """
    Perform the ETL process of pulling, transforming, and loading data.

    This function continuously pulls data from PostgreSQL, performs data aggregations,
    and stores the aggregated data in the MySQL database.
    """
    while True:
        # Pull data from PostgreSQL
        with psql_engine.connect() as conn:
            query = text("SELECT device_id, temperature, location, time FROM devices")
            result = conn.execute(query)
            rows = result.fetchall()

            for row in rows:
                device_id = row[0]
                location = json.loads(row[2])
                time = datetime.utcfromtimestamp(int(row[3]))  # Convert to integer

                hour = int(time.replace(minute=0, second=0).timestamp())
                next_hour = time.replace(minute=0, second=0) + timedelta(hours=1)
                next_hour = int(next_hour.timestamp())


                # Calculate maximum temperature per hour
                max_temp_query = text(f"SELECT MAX(temperature) FROM devices WHERE device_id = '{device_id}' AND time >= '{str(hour)}' AND time < '{str(next_hour)}'")
                max_temp_result = conn.execute(max_temp_query)
                max_temp = max_temp_result.scalar()
                

                # Calculate the number of data points per hour
                count_query = text(f"SELECT COUNT(*) FROM devices WHERE device_id = '{device_id}' AND time >= '{str(hour)}' AND time < '{str(next_hour)}'")
                count_result = conn.execute(count_query)
                count = count_result.scalar()


                # Calculate total distance of device movement per hour
                prev_lat = None
                prev_lon = None
                total_distance = 0.0
                location_query = text(f"""
                SELECT location, time FROM devices
                WHERE device_id = '{device_id}'
                AND time >= '{str(hour)}' 
                AND time < '{str(next_hour)}'
                ORDER BY time
                """)
                location_result = conn.execute(location_query)
                location_rows = location_result.fetchall()
                for loc_row in location_rows:
                    curr_location = json.loads(loc_row[0])
                    curr_time = datetime.utcfromtimestamp(int(loc_row[1]))
                    if prev_lat is not None and prev_lon is not None:
                        distance = calculate_distance(prev_lat, prev_lon, curr_location['latitude'], curr_location['longitude'])
                        total_distance += distance
                    prev_lat = curr_location['latitude']
                    prev_lon = curr_location['longitude']

                # Check if the device_id already exists in the MySQL table
                existing_entry = session.query(MyTableResult).get(device_id)

                if existing_entry:
                    # Update the existing entry
                    existing_entry.time = time
                    existing_entry.max_temperature = max_temp
                    existing_entry.data_points = count
                    existing_entry.total_distance = total_distance
                else:
                    # Create a new entry in the MySQL table
                    new_entry = MyTableResult(
                        device_id=device_id,
                        time=time,
                        max_temperature=max_temp,
                        data_points=count,
                        total_distance=total_distance
                    )
                    session.merge(new_entry)

                session.commit()

                # Print results
                print(f"Device ID: {device_id}")
                print(f"Time: {time}")
                print(f"Max Temperature (per hour): {max_temp}")
                print(f"Data Points (per hour): {count}")
                print(f"Total Distance: {total_distance} km")
                print("---")


if __name__ == '__main__':
    etl()
