#psql --host=zheyu-database-airbnb.cu2ky11rrces.us-east-2.rds.amazonaws.com --port=5432 --username=postgres --password --dbname=postgres


class Config(object):
    DEBUG = False
    TESTING = False
    DATABASE = {
        'drivername': 'postgres',
        'host': 'localhost',
        'port': '5432',
        'username': 'zheyu',
        'password': 'zheyuliu',
        'database': 'airbnb'
    }


class ProductionConfig(Config):
    DATABASE = {
        'drivername': 'postgres',
        'host': 'zheyu-database-airbnb.cu2ky11rrces.us-east-2.rds.amazonaws.com',
        'port': '5432',
        'username': 'zheyu',
        'password': 'zheyuliu',
        'database': 'airbnb'
    }


class DevelopmentConfig(Config):
    DEBUG = True