from datetime import date
from sqlalchemy import create_engine
import pandas as pd
import holidays
import yaml

dim_fecha = pd.DataFrame({
    "date": pd.date_range(start='1/1/2023', end='1/1/2025', freq='D')
})
dim_fecha.head()

dim_fecha["year"] = dim_fecha["date"].dt.year
dim_fecha["month"] = dim_fecha["date"].dt.month
dim_fecha["day"] = dim_fecha["date"].dt.day
dim_fecha["weekday"] = dim_fecha["date"].dt.weekday
dim_fecha["quarter"] = dim_fecha["date"].dt.quarter

dim_fecha.head()


dim_fecha["day_of_year"] = dim_fecha["date"].dt.day_of_year
dim_fecha["day_of_month"] = dim_fecha["date"].dt.days_in_month
dim_fecha["month_str"] = dim_fecha["date"].dt.month_name() # run locale -a en unix 
dim_fecha["day_str"] = dim_fecha["date"].dt.day_name() # locale = 'es_CO.UTF8'
dim_fecha["date_str"] = dim_fecha["date"].dt.strftime("%d/%m/%Y")
dim_fecha.head()

co_holidays = holidays.CO(language="es")
dim_fecha["is_Holiday"] = dim_fecha["date"].apply(lambda x:  x in co_holidays)
dim_fecha["holiday"] = dim_fecha["date"].apply(lambda x: co_holidays.get(x))
dim_fecha["saved"] = date.today()
dim_fecha["weekend"] = dim_fecha["weekday"].apply(lambda x: x>4)
dim_fecha.head()

with open('../config.yml', 'r') as f:
    config = yaml.safe_load(f)    
    config_etl = config['bodega']

# Construct the database URL
url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
           f"{config_etl['port']}/{config_etl['dbname']}")
# Create the SQLAlchemy Engine
etl_conn = create_engine(url_etl)


dim_fecha.to_sql('dim_fecha', etl_conn, if_exists='replace',index_label='key_dim_fecha')