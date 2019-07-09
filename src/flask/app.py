import os
import pandas as pd
import numpy as np

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

from flask import Flask, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func,inspect,MetaData, Table, Column, Integer, String, Float
app = Flask(__name__)


#################################################
# Database Setup
#################################################

engine = create_engine('redshift+psycopg2://CLUSTER_NAME:5439/DB_NAME')
metadata = MetaData(engine)
metadata.reflect(engine)

Table('billing', metadata, Column('record_id', Integer, primary_key=True),extend_existing=True)
Table('demographics', metadata, Column('record_id', Integer, primary_key=True),extend_existing=True)
Table('agg_infection', metadata, Column('record_id', Integer, primary_key=True),extend_existing=True)
Table('perscription', metadata, Column('record_id', Integer, primary_key=True),extend_existing=True)

# reflect an existing database into a new model
Base = automap_base(metadata=metadata)
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Billing = Base.classes.billing
Demographics = Base.classes.demographics
Infection = Base.classes.agg_infection
Perscription = Base.classes.perscription

session = Session(bind=engine)

@app.route("/")
def index():
    """Return the homepage."""
    return render_template("index.html")

@app.route("/names")
def names():
    """Return a list of sample names."""

    # perform the sql query
    record_lsit = session.query(Infection.record_id).distinct().all()

    # Return a list of the column names (sample names)
    return jsonify(record_lsit)


@app.route("/metadata/<sample>")
def sample_metadata(sample):
    """Return the MetaData for a given sample."""
    sel = [
        Demographics.admit_date,
        Demographics.campus_name,
        Demographics.gender,
        Demographics.age,
        Demographics.diagnosis_count,
        Demographics.icdtitle,
        Demographics.patient_type,
        Demographics.icu_flage
    ]

    results = session.query(*sel).filter(Demographics.record_id == sample).all()

    # Create a dictionary entry for each row of metadata information
    sample_metadata = {}
    for result in results:
        sample_metadata["Admit Date"] = result[0]
        sample_metadata["Hospital"] = result[1]
        sample_metadata["Gender"] = result[2]
        sample_metadata["Age"] = result[3]
        sample_metadata["Diagnosis Count"] = result[4]
        sample_metadata["ICD Title"] = result[5]
        sample_metadata["Patient Type"] = result[6]
        sample_metadata["ICU Flage"] = result[7]

    return jsonify(sample_metadata)


@app.route("/samples/<sample>")
def samples(sample):
    """Return `otu_ids`, `otu_labels`,and `sample_values`."""
    sel = [
        Infection.all_otu,
        Infection.all_bacteria
    ]

    results = session.query(*sel).filter(Infection.record_id == sample).all()

    # Create a dictionary entry for each row of metadata information
    infection_metadata = {}
    for result in results:
        infection_metadata["sample_values"] = [int(item) for item in result[0].split(',')]
        infection_metadata["otu_labels"] = [item for item in result[1].split(',')]

    return jsonify(infection_metadata)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000,debug=True)
