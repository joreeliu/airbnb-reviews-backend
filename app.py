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
    qry = """select * from dbo.neighbourhoods"""
    res = pd.read_sql(qry, con=engine)
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


@app.route('/get_top_clusters_groups/<group>')
def get_top_clusters_groups(group):
    dct = get_group_mapping()
    inv_dct = {v: k for k, v in dct.items()}
    group_code = inv_dct[group]

    qry = f"""SELECT neighborhood, "0", "1", "2", "3", "4", "5", "6", "7", "8"
    FROM dbo.neighborhood_cluster_counts"""
    res = pd.read_sql(qry, con=engine)
    res.set_index('neighborhood', inplace=True)
    res.loc[:, 'category'] = res.idxmax(1)
    res = res[res['category'] == str(group_code)]
    if res.empty:
        res = []
    else:
        res = res.sort_values(by=str(group_code), ascending=False).head(5).reset_index()['neighborhood']
    return jsonify(list(res))


@app.route('/get_neighbor_intro/<neighborhood>')
def get_neighbor_intro(neighborhood):
    qry = """select * from dbo.neighborhoodsintro"""
    df = pd.read_sql(qry, con=engine)
    df_tmp = df[df['neighborhood_name'] == neighborhood]
    if df_tmp.empty:
        target = dict(img='', neighborhood_borough='', neighborhood_description='', neighborhood_name=neighborhood, neighborhood_url='')
    else:
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


@app.route('/get_neighborhood_score/<neighborhood>')
def get_neighborhood_score(neighborhood):
    qry = f"""SELECT neighbourhood_cleansed neighborhood, avg(review_scores_rating) score FROM dbo.listings group by neighbourhood_cleansed having neighbourhood_cleansed = '{neighborhood}';"""
    df = pd.read_sql(qry, con=engine)

    if df.empty:
        return None
    return jsonify(df.to_dict('record')[0])


if __name__ == '__main__':
    app.run()
