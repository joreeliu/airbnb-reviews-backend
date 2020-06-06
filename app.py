from flask import Flask
from sqlalchemy import create_engine
import pandas as pd
from flask import jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
engine = create_engine('postgresql+psycopg2://zheyu:mypass@localhost/airbnb')


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


@app.route('/get_listings_by_description/<words>')
def get_listings_by_description(words):
    qry = f"""select * from public.listings where description like '%%{words}%%' limit 100"""
    res = pd.read_sql(qry, con=engine)
    res = res.where(res.notnull(), None)
    res = res.to_dict('records')

    return jsonify(res)

@app.route('/get_listings_by_neighborhood/<neighborhood>')
def get_listings_by_neighborhood(neighborhood):
    qry = f"""select * from public.listings where neighbourhood like '%%{neighborhood}%%'"""
    res = pd.read_sql(qry, con=engine)
    res = res.where(res.notnull(), None)
    res = res.to_dict('records')
    return jsonify(res)

@app.route('/get_listings_by_reviews/<words>')
def get_listings_by_reviews(words):
    qry = f"""select distinct L.id, name, summary, space, description, neighborhood_overview, transit, access, neighbourhood, neighbourhood_cleansed, zipcode, market, latitude, longitude, room_type, property_type, number_of_reviews, review_scores_rating, review_scores_location from public.listings as L inner join public.reviews as R on L.id = R.listing_id where R.comments like 
 '%%{words}%%'"""
    res = pd.read_sql(qry, con=engine)
    res = res.where(res.notnull(), None)
    res = res.to_dict('records')
    return jsonify(res)


if __name__ == '__main__':
    app.run()