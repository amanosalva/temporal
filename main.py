from parameters import Parameters
from database_connect import Oracle
from log import Log


import pandas as pd
import numpy as np
from datetime import datetime
import logging
from os import path
from dateutil.relativedelta import relativedelta
from parameters import Parameters
from sklearn.model_selection import train_test_split

if __name__ == "__main__":


    # Fecha de hoy
    hoy = datetime.now()
    hoy_str_f1 = hoy.strftime("%d-%m-%Y")
    hoy_str_f2 = hoy.strftime("%d/%m/%Y")

    # Conexión a base de datos
    oracle = Oracle(Parameters.USER, 
                    Parameters.PASSWORD, 
                    Parameters.HOST,
                    Parameters.PORT, 
                    Parameters.SID)

    
    log = Log(Parameters.LOG_FOLDER, Parameters.LOG_FILE, Parameters.LOG_CONTEXT)
    logger = log.getLoggerObject()

    logger.info("Conectándose a la base de datos")
    oracle.connect()
                            
    
    logger.info("Conexión realizada con éxito")


    # Ejecución del procedimiento almacenado    
    #oracle.execute_sp("SP_CVM_CA_CANDIDATOS", parameters_in=())

    # Obteniendo candidatos
    logger.info("Obteniendo Candidatos a campañas")
    candidatos = pd.read_sql(Parameters.QUERY_CAND, oracle.getConnect())

    # Obteniendo configuración de campañas
    logger.info("Obteniendo Configuración de campañas")
    conf_camp = pd.read_sql(Parameters.QUERY_CONF_C, oracle.getConnect())
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        logger.info(conf_camp)

    ### Ejecución provisional, limite superior no puede ser infinito para la comparación, se considerará un número muy alto.
    conf_camp["MES_LIMSUP"] = conf_camp["MES_LIMSUP"].fillna(9999999)
    ### Obteniendo todos los valores de Pack y Sim directo de la base de datos de configuración
    pack_sim_values = np.unique(conf_camp["VCHNAME"].to_list())

    # Asignación de Campañas a los clientes
    campos_clave = ['VCHRUCCOMPANIA', 'VCHTELEFONO', 'VCHPACKCHIP']
    cand_camp = pd.DataFrame()
    for index, row in conf_camp.iterrows():
        if row["ESTADO"] == "ON":
            data = candidatos.loc[(candidatos["NEW_MESES_CLIENTE"] >= int(row["MES_LIMINF"])) & (candidatos["NEW_MESES_CLIENTE"] <= int(row["MES_LIMSUP"])) & (candidatos["NEW_VCHPACKCHIP"] == row["VCHNAME"])]
            data.reset_index(drop=True, inplace=True)
            data["CAMPAIGN"]            = row["VCHIDNAME_CAMP"] 
            data["SUBCAMPAIGN"]         = row["VCHIDNAME_SUBCAMP"] 
            data["PRIORIDAD"]           = row["PRIORIDAD"]
            data["FECGENCANDIDATOS"]    = hoy_str_f2
            
            cand_camp = cand_camp.append([data], ignore_index=True)

    # Selección de Campañas por prioridad
    ### Quedarse con el que tenga mejor prioridad (donde 1 es la mejor prioridad)
    cand_camp = cand_camp.sort_values(["PRIORIDAD"], ascending=True).reset_index(drop=True)
    cand_camp = cand_camp.drop_duplicates(subset=campos_clave, keep='first') 


    # Generación del Grupo de Control para cada campaña y tipo de cliente (PACK/CHIP)
    cand_camp["CAMP_SUBCAMP"] = cand_camp["CAMPAIGN"] + "_" + cand_camp["SUBCAMPAIGN"] + "_" + cand_camp["VCHPACKCHIP"]
    camp_subcamp_ps = np.unique(cand_camp["CAMP_SUBCAMP"].to_list())
    # Grupo de Control
    final_cand_camp = pd.DataFrame()
    for cs in camp_subcamp_ps:
        filtro = cand_camp.loc[cand_camp["CAMP_SUBCAMP"] == cs]
        
        gestion, control = train_test_split(filtro, test_size=0.10, random_state=2021)
        gestion.reset_index(drop=True, inplace=True)
        control.reset_index(drop=True, inplace=True)
        
        gestion["GRUPO"] = 'GESTION'
        control["GRUPO"] = 'CONTROL'
        
        final_cand_camp = final_cand_camp.append([gestion, control], ignore_index=True)  


    for csps in camp_subcamp_ps:
        rows_gestion = final_cand_camp.loc[(final_cand_camp["CAMP_SUBCAMP"] == "renovacion_decide_SIM") & (final_cand_camp["GRUPO"] == 'GESTION')].shape[0]
        rows_control =  final_cand_camp.loc[(final_cand_camp["CAMP_SUBCAMP"] == "renovacion_decide_SIM") & (final_cand_camp["GRUPO"] == 'CONTROL')].shape[0]
        print("La campaña: {} tiene el porcentaje de grupo de control: {}".format(csps, str(rows_control/rows_gestion)))  

    final_cand_camp = final_cand_camp.drop(["VCHTECNOCOMERCIAL", "MESES_CLIENTE", "MESES_ULT_RENO", "VCHPACKCHIP", "PRIORIDAD", "CAMP_SUBCAMP", "FECACTIVACIONCONTRATO"], axis=1)


    # Subir información al oracle
    final_cand_camp = final_cand_camp.where(pd.notnull(final_cand_camp), None)
    records = final_cand_camp.to_dict('records')
    insert_query = "INSERT INTO CVM_CAMPAIGN_RESULTS (VCHRUCCOMPANIA, VCHTELEFONO, NEW_VCHPACKCHIP, NEW_MESES_CLIENTE, CAMPAIGN, SUBCAMPAIGN, FECGENCANDIDATOS, GRUPO) VALUES (:VCHRUCCOMPANIA, :VCHTELEFONO, :NEW_VCHPACKCHIP, :NEW_MESES_CLIENTE, :CAMPAIGN, :SUBCAMPAIGN, :FECGENCANDIDATOS, :GRUPO)"
    connection = oracle.getConnect()
    cursor = oracle.getCursor()

    # Borrando la tabla
    cursor.execute("TRUNCATE TABLE CVM_CAMPAIGN_RESULTS")
    connection.commit()

    i=0
    long_records = len(records)
    r = 5000
    while(i < long_records):
        logger.info("Inserción rango de registros: {} - {}".format(str(i), str(i+len(records[i:i+r])-1)))
        cursor.executemany(insert_query, records[i:i+r])
        i = i + r

    connection.commit()
    logger.info("\nCommit realizado con éxito")
    logger.info("\nInserción finalizada con éxito. Total de Registros insertados: {}".format(str(long_records)))

    logger.info("\nProceso terminado con éxito")
    connection.close()


