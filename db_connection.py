import yaml
from sqlalchemy import create_engine

# create the connection to the database from ten config.yml file
def get_database_connections():
    try:
        with open('config.yml', 'r') as f:
            config = yaml.safe_load(f)   
            config_rf = config['fuente'] 
            config_etl = config['bodega']

        # connection urls
        url_rf = (f"{config_rf['drivername']}://{config_rf['user']}:{config_rf['password']}@"
                 f"{config_rf['host']}:{config_rf['port']}/{config_rf['dbname']}")
        
        url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@"
                  f"{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}")
        
        # create the SQLAlchemy engines
        ra_fu = create_engine(url_rf)
        etl_conn = create_engine(url_etl)
        
        # ra_fu      -> source database
        # etl_conn   -> warehouse databasse
        return ra_fu, etl_conn
    
    except FileNotFoundError:
        raise Exception("The config.yml file was not found")
    except KeyError as e:
        raise Exception(f"Key {str(e)} is missing in the config.yml file")
    except Exception as e:
        raise Exception(f"Error creating connections: {str(e)}")

if __name__ == "__main__":
    get_database_connections()