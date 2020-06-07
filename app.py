from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import pandas as pd
from flask import jsonify
from flask_cors import CORS
import config
import geojson


app = Flask(__name__)
app.config.from_object(config.ProductionConfig())
CORS(app)
engine = create_engine(URL(**app.config['DATABASE']))


@app.route('/get_neighborhood/')
def get_neighborhood():
    res = pd.read_csv("neighbourhoods.csv")
    res = res.where(res.notnull(), None)
    res = res.to_dict('records')
    final_res = dict()
    for item in res:
        if item['neighbourhood_group'] not in final_res:
            final_res[item['neighbourhood_group']] = [item['neighbourhood']]
        else:
            final_res[item['neighbourhood_group']].append(item['neighbourhood'])

    return jsonify(final_res)


@app.route('/get_neighborhood_geo/')
def get_neighborhood_geo():
    with open('neighbourhoods.geojson') as f:
        res = geojson.load(f)

    return jsonify(res)


@app.route('/get_listings_by_description/<words>')
def get_listings_by_description(words):
    qry = f"""select * from dbo.listings where description like '%%{words}%%' limit 100"""
    res = pd.read_sql(qry, con=engine)
    res = res.where(res.notnull(), None)
    res = res.to_dict('records')

    return jsonify(res)


@app.route('/get_listings_by_neighborhood/<neighborhood>')
def get_listings_by_neighborhood(neighborhood):
    qry = f"""select * from dbo.listings where neighbourhood like '%%{neighborhood}%%'"""
    res = pd.read_sql(qry, con=engine)
    res = res.where(res.notnull(), None)
    res = res.to_dict('records')
    return jsonify(res)


@app.route('/get_listings_by_reviews/<words>')
def get_listings_by_reviews(words):
    qry = f"""select distinct L.id, name, summary, space, description, neighborhood_overview, transit, access, neighbourhood, neighbourhood_cleansed, zipcode, market, latitude, longitude, room_type, property_type, number_of_reviews, review_scores_rating, review_scores_location from dbo.listings as L inner join dbo.reviews as R on L.id = R.listing_id where R.comments like 
 '%%{words}%%'"""
    res = pd.read_sql(qry, con=engine)
    res = res.where(res.notnull(), None)
    res = res.to_dict('records')
    return jsonify(res)


@app.route('/get_neighbor_cluster_count')
def get_neighbor_cluster_count():
    qry = '''SELECT neighborhood, "0", "1", "2", "3", "4", "5", "6", "7", "8"
 FROM dbo.neighborhood_cluster_counts'''
    res = pd.read_sql(qry, con=engine)
    res = res.to_dict('records')
    return jsonify(res)


if __name__ == '__main__':
    app.run()
