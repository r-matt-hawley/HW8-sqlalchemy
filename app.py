import numpy as np
import pandas as pd
import re
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Helper Functions
#################################################
def calc_temps(start_date, end_date=0):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVG, and TMAX
    """
    
    if end_date==0:
        # No end date was given.
        result = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
            .filter(Measurement.date >= start_date)\
            .all()
    else:
        # Both a start and an end date are given.
        result = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
        .filter(Measurement.date >= start_date)\
        .filter(Measurement.date <= end_date)\
        .all()

    if result == [(None,None,None)]:
        return []
    else:
        return result

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite?check_same_thread=False")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Convert the query results to a Dictionary using `date` as the key and `prcp` as the value."""
  
    # Calculate the date 1 year ago from the last data point in the database
    latest_date = session.query(func.max(Measurement.date)).scalar()
    last_year = (dt.datetime.strptime(latest_date, "%Y-%m-%d") - dt.timedelta(days=365)) #datedelta.YEAR)

    # Perform a query to retrieve the data and precipitation scores
    last_year_records = session.query(Measurement.date, Measurement.prcp)\
        .filter(Measurement.date >= last_year)\
        .all()

    # Save the query results as a Pandas DataFrame and set the index to the date column
    prcp_df = pd.DataFrame(last_year_records, columns=["Date", "Precipitation"])

    # Sort the dataframe by date
    prcp_df = prcp_df.sort_values("Date")

    # Set the date column to be the index
    prcp_df = prcp_df.set_index("Date", inplace=False)

    return jsonify(prcp_df.to_dict())

@app.route("/api/v1.0/stations")
def stations():
    """Return a JSON list of stations from the dataset."""
    
    station_names = session.query(Measurement.station)\
        .distinct(Measurement.station)\
        .all()

    return jsonify(station_names)

@app.route("/api/v1.0/tobs")
def tobs():
    """ Query for the dates and temperature observations from a year from the last data point.
        Return a JSON list of Temperature Observations (tobs) for the previous year."""

    # Calculate the date 1 year ago from the last data point in the database
    latest_date = session.query(func.max(Measurement.date)).scalar()
    last_year = (dt.datetime.strptime(latest_date, "%Y-%m-%d") - dt.timedelta(days=365))
    
    tobs_last_year = session.query(Measurement.tobs)\
        .filter(Measurement.date >= last_year)\
        .all()
    return jsonify(list(tobs_last_year))

@app.route("/api/v1.0/<start>")
def calc_temps_start(start):
    """Return a JSON list of the minimum temperature, the average temperature, 
    and the max temperature for a given start or start-end range.
    When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` 
    for all dates greater than and equal to the start date."""

    # Check for invalid data
    if not re.match("^[0-9]{4}-[0-9]{2}-[0-9]{2}", start):
        return f"Your search of '{start}' is not in the correct format.<br/>\
            Please, search for a start date in the form yyyy-mm-dd (year-month-day)."
    
    result = calc_temps(start)
    
    if not result:
        return f"Your search of {start} did not match any records.\
            Please, search for an earlier date.<br/>"
    else:
        return jsonify(result)

@app.route("/api/v1.0/<start>/<end>")
def calc_temps_start_end(start, end):
    """Return a JSON list of the minimum temperature, the average temperature, 
    and the max temperature for a given start or start-end range.
    When given the start and the end date, calculate the `TMIN`, `TAVG`, and `TMAX` 
    for dates between the start and end date inclusive."""

    # Check for invalid data
    if not re.match("^[0-9]{4}-[0-9]{2}-[0-9]{2}", start)\
        or not re.match("^[0-9]{4}-[0-9]{2}-[0-9]{2}", end):
        return f"Your search beginning with '{start}' and ending with '{end}'\
            is not in the correct format.<br/>Please, verify that both start and end dates\
            are in the form yyyy-mm-dd (year-month-day)."

    # If the start date comes after end date, call calc_temps with the reverse order.
    if start > end:
        result = calc_temps(end, start)
    else:
        result = calc_temps(start, end)

    if not result:
        return f"Your search beginning with '{start}' and ending with '{end}'\
            did not match any records.  Please, search for a different date range.<br/>"
    else:
        return jsonify(result)

if __name__ == '__main__':
    app.run()


 