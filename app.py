from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import pandas as pd
from flask import jsonify
from flask_cors import CORS
import config
import geojson
import yaml


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
    neighborhood = neighborhood.replace("'", "''")
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


@app.route('/get_group_mapping')
def get_group_mapping():
    return {'0': 'Foodies',
            '1': 'Wanderers',
            '2': 'Park Lovers',
            '3': 'Quiet Seekers',
            '4': 'Subway Riders',
            '5': 'Architecture Lovers',
            '6': 'Everything Bagel',
            '7': 'Artists',
            '8': 'Café-goers'}


@app.route('/get_neighbor_cluster_count/<neighborhood>')
def get_neighbor_cluster_count(neighborhood):
    neighborhood = neighborhood.replace("'", "''")
    qry = f"""SELECT "0", "1", "2", "3", "4", "5", "6", "7", "8"
 FROM dbo.neighborhood_cluster_counts where neighborhood = '{neighborhood}'"""
    res = pd.read_sql(qry, con=engine)

    res = res.rename(columns=get_group_mapping())

    res = res.where(res.notnull(), None)
    res = res / res.sum(axis=1).squeeze()
    res = res.to_dict('records')[0]
    final_res = []
    for key, val in res.items():
        if val != 0:
            final_res.append({'key': key, 'val': val})
    return jsonify(final_res)


@app.route('/get_neighbor_intro/<neighborhood>')
def get_neighbor_intro(neighborhood):
    df = pd.read_csv('neighborhoodsintro.csv')
    df.loc[:, 'img'] = 'https://images.nycgo.com/image/fetch/q_65,c_fill,f_auto,w_1920/https://www.nycgo.com/images/neighborhoods/71290/dumbo-brooklyn-nyc-julienne-schaer-nyc-and-company-207.jpg'
    df_tmp = df[df['neighborhood_name'] == neighborhood]
    if df_tmp.empty:
        return
    target = df_tmp.to_dict('record')[0]
    return jsonify(target)


@app.route('/get_keywords/<group>')
def get_keywords(group):

    dct = get_group_mapping()
    inv_dct = {v: k for k, v in dct.items()}
    group_code = inv_dct[group]

    with open("cluster_description.yaml", 'r') as stream:
        try:
            return jsonify(yaml.safe_load(stream).get(f'Cluster {group_code}', []))
        except yaml.YAMLError as exc:
            print(exc)
            return None


if __name__ == '__main__':
    app.run()
