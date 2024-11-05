from dimensiones.dim_ciudad import run_etl_dim_ciudad
from dimensiones.dim_cliente import run_etl_dim_cliente
from dimensiones.dim_fecha import run_etl_dim_fecha
from dimensiones.dim_hora import run_etl_dim_hora
from dimensiones.trans_servicio import run_etl_trans_servicio
from dimensiones.dim_mensajero import run_etl_dim_mensajero
from dimensiones.dim_tipo_servicio import run_etl_dim_tipo_servicio
from dimensiones.dim_sede import run_etl_dim_sede
from hechos.hecho_servicios_acum_snap import run_etl_servicio_acumulado

def main():
    
    """ETL dimensiones"""
    # run_etl_dim_hora()
    # run_etl_dim_ciudad()
    # run_etl_dim_cliente()
    # run_etl_dim_fecha()
    # run_etl_dim_mensajero()
    # run_etl_dim_tipo_servicio()
    # run_etl_dim_sede()
    # run_etl_trans_servicio()
    
    # """ETL hechos"""    
    run_etl_servicio_acumulado()

if __name__ == "__main__":
    main()
