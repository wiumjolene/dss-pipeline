import logging
import datetime
import os

import pandas as pd
from src.utils import config
from src.utils.connect import DatabaseModelsClass


class GetPlanningData:
    """ Class to extract planning data"""
    logger = logging.getLogger(f"{__name__}.GetPlanningData")
    database_central = DatabaseModelsClass('MYSQLCENTRAL')

    def get_planningdata(self):
        self.logger.info(f"---  Getting planning data from central")
        """ Get blok plan for future. """
        sql_script = """
            SELECT *, now() as extract_datetime
            FROM pt_admin1718.vview_0091_demand_union_planned_for_grapes
            WHERE (left(demand_arrivalweek, 2) = 21 AND right(demand_arrivalweek, 2) > 30)
                OR (left(demand_arrivalweek, 2) = 22 AND right(demand_arrivalweek, 2) < 30);
        """
        df = self.database_central.select_query(sql_script)
        return df

    def get_harvest_estimate(self):
        self.logger.info(f"---  Getting harvest estimate data from central")
        """ Get harvest estimate """
        sql_script = """
            SELECT *, now() as extract_datetime
            FROM pt_admin1718.vview_0238_harvest_estimates_for_grapes_budgets
            WHERE Season = 2022;
        """
        df = self.database_central.select_query(sql_script)
        return df

    def get_harvest_estimate_quickadj(self):
        self.logger.info(f"---  Getting harvest estimate data from central")
        """ Get harvest estimate """
        sql_script = """
            SELECT *, now() as extract_datetime 
            FROM pt_admin1718.harvest_estimates_quickadj
            WHERE season = 2022;
        """
        df = self.database_central.select_query(sql_script)
        return df

    def get_harvest_estimate_0638(self):
        self.logger.info(f"---  Getting harvest estimate data from central")
        """ Get harvest estimate """
        sql_script = """
            SELECT *, now() as extract_datetime
            FROM pt_admin1718.vview_0638_all_harvest_estimates_guid
            WHERE season=2022;
        """
        df = self.database_central.select_query(sql_script)
        return df

    def get_pack_capacity(self):
        self.logger.info(f"---  Getting harvest estimate data from central")
        """ Get pack capacity """
        sql_script = """
            SELECT *, now() as extract_datetime 
            FROM pt_admin1718.packingcapacity
            WHERE (left(packweek, 2) = 21 AND right(packweek, 2) > 30)
                OR (left(packweek, 2) = 22 AND right(packweek, 2) < 30);
        """
        df = self.database_central.select_query(sql_script)
        return df

class GetLocalData:
    """ Class to extract planning data"""
    logger = logging.getLogger(f"{__name__}.GetLocalData")
    database_dss = DatabaseModelsClass('MYSQLLINUX')

    def get_local_he(self):
        sql1="""
            SELECT dim_block.id as block_id
                , dim_va.id as va_id
                , Week as packweek
                , ROUND(SUM(GrossEstimate * 1000),1) as kg_raw
            FROM dss.harvest_estimate_data
            LEFT JOIN dim_fc ON (harvest_estimate_data.Grower = dim_fc.name)
            LEFT JOIN dim_block ON (harvest_estimate_data.ProductionUnit = dim_block.name AND dim_block.fc_id = dim_fc.id)
            LEFT JOIN dim_va ON (harvest_estimate_data.Variety = dim_va.name)
            WHERE extract_datetime = (SELECT MAX(extract_datetime)FROM dss.harvest_estimate_data)
            AND estimatetype = 'BUDGET2'
            GROUP BY dim_fc.id, dim_block.id, dim_va.id, Week;        
        """
        sql="""
            SELECT dim_block.id as block_id
                , dim_va.id as va_id
                , Week as packweek
                -- , ROUND(SUM(GrossEstimate * 1000),1) as kg_raw
                , SUM(kgGross) as kg_raw
            FROM dss.harvest_estimate_0638_data as he
            LEFT JOIN dim_fc ON (he.Grower = dim_fc.name)
            LEFT JOIN dim_block ON (he.Farm = dim_block.name AND dim_block.fc_id = dim_fc.id)
            LEFT JOIN dim_va ON (he.Variety = dim_va.name)
            WHERE extract_datetime = (SELECT MAX(extract_datetime) FROM dss.harvest_estimate_0638_data)
            AND estimatetype = 'ADJUSTMENT'
            AND kgGross > 0
            GROUP BY dim_fc.id, dim_block.id, dim_va.id, Week
            ;         
        """
        df = self.database_dss.select_query(sql)
        return df

    def get_he_fc(self):
        sql1="""
            SELECT DISTINCT Grower as name
                , dim_fc.id
            FROM dss.harvest_estimate_data
            LEFT JOIN dim_fc ON (harvest_estimate_data.Grower = dim_fc.name)
            WHERE dim_fc.id is NULL
            AND extract_datetime = (SELECT MAX(extract_datetime) FROM dss.harvest_estimate_data);      
        """
        sql="""
            SELECT DISTINCT Grower as name
                , dim_fc.id
            FROM dss.harvest_estimate_0638_data
            LEFT JOIN dim_fc ON (harvest_estimate_0638_data.Grower = dim_fc.name)
            WHERE dim_fc.id is NULL
            AND extract_datetime = (SELECT MAX(extract_datetime) FROM dss.harvest_estimate_0638_data);   
        """
        df = self.database_dss.select_query(sql)
        df['add_datetime'] = datetime.datetime.now()
        return df

    def get_he_block(self):
        sql1="""
            SELECT DISTINCT ProductionUnit as name
                , dim_fc.id as fc_id
            FROM dss.harvest_estimate_data
            LEFT JOIN dim_fc ON (harvest_estimate_data.Grower = dim_fc.name)
            LEFT JOIN dim_block ON (harvest_estimate_data.ProductionUnit = dim_block.name AND dim_block.fc_id = dim_fc.id)
            WHERE dim_block.id is NULL
            AND extract_datetime = (SELECT MAX(extract_datetime)FROM dss.harvest_estimate_data);;   
        """
        sql="""
            SELECT DISTINCT Farm as name
                , dim_fc.id as fc_id
            FROM dss.harvest_estimate_0638_data
            LEFT JOIN dim_fc ON (harvest_estimate_0638_data.Grower = dim_fc.name)
            LEFT JOIN dim_block ON (harvest_estimate_0638_data.Farm = dim_block.name AND dim_block.fc_id = dim_fc.id)
            WHERE dim_block.id is NULL
            AND extract_datetime = (SELECT MAX(extract_datetime)FROM dss.harvest_estimate_0638_data);  
        """
        df = self.database_dss.select_query(sql)
        df['add_datetime'] = datetime.datetime.now()
        return df

    def get_he_va(self):
        sql1="""
             SELECT DISTINCT Variety as name
            FROM dss.harvest_estimate_data
            LEFT JOIN dim_va ON (harvest_estimate_data.Variety = dim_va.name)
            WHERE dim_va.id is NULL
            AND extract_datetime = (SELECT MAX(extract_datetime)FROM dss.harvest_estimate_data); 
        """
        sql="""
			SELECT DISTINCT Variety as name
            FROM dss.harvest_estimate_0638_data
            LEFT JOIN dim_va ON (harvest_estimate_0638_data.Variety = dim_va.name)
            WHERE dim_va.id is NULL
            AND extract_datetime = (SELECT MAX(extract_datetime)FROM dss.harvest_estimate_0638_data); 
        """
        df = self.database_dss.select_query(sql)
        df['add_datetime'] = datetime.datetime.now()
        return df

    def get_dp_client(self):
        sql="""
            SELECT DISTINCT targetmarket as name
            FROM dss.planning_data
            LEFT JOIN dim_client ON (planning_data.targetmarket = dim_client.name)
            WHERE dim_client.id is NULL
            AND extract_datetime = (SELECT MAX(extract_datetime)FROM dss.planning_data)
            AND recordtype = 'DEMAND';
        """
        df = self.database_dss.select_query(sql)
        df['add_datetime'] = datetime.datetime.now()
        return df

    def get_local_dp(self):
        sql="""
            SELECT demandid as id
                , dim_client.id as client_id
                , dim_vacat.id as vacat_id
                , dim_pack_type.id as pack_type_id
                , IF(priority= "-", 0, priority) as priority
                , demand_arrivalweek as arrivalweek
                , dim_time.week as packweek
                , dim_client.transitdays as transitdays
                , round(qty_standardctns) as stdunits
            FROM dss.planning_data
            LEFT JOIN dim_client ON (planning_data.targetmarket = dim_client.name)
            LEFT JOIN dim_vacat ON (planning_data.varietygroup = dim_vacat.name)
            LEFT JOIN dim_week ON (planning_data.demand_arrivalweek = dim_week.week)
            LEFT JOIN dim_time ON ((date_sub(dim_week.weekstart,  INTERVAL dim_client.transitdays DAY)) = dim_time.day)
            LEFT JOIN dim_pack_type ON ((IF((SUBSTR(cartontype, 1, 1) = 'A'),
                        IF((cartontype = 'A75F'),
                            'LOOSE',
                            'PUNNET'),
                        'LOOSE')) = dim_pack_type.name)
            WHERE extract_datetime = (SELECT MAX(extract_datetime)FROM dss.planning_data)
            AND recordtype = 'DEMAND'
            ;
        """
        df = self.database_dss.select_query(sql)
        return df

    def get_pc_packhouse(self):
        sql="""
            SELECT distinct phc as name
            FROM dss.pack_capacity_data
            LEFT JOIN dim_packhouse ON (pack_capacity_data.phc = dim_packhouse.name)
            WHERE dim_packhouse.id is NULL
            AND extract_datetime = (SELECT MAX(extract_datetime)FROM dss.pack_capacity_data);
        """
        df = self.database_dss.select_query(sql)
        df['add_datetime'] = datetime.datetime.now()
        return df

    def get_local_pc(self):
        sql="""
            SELECT dim_packhouse.id as packhouse_id
                , dim_pack_type.id as pack_type_id
                , packweek
                , noofstdcartons as stdunits
            FROM dss.pack_capacity_data
            LEFT JOIN dim_packhouse ON (pack_capacity_data.phc = dim_packhouse.name)
            LEFT JOIN dim_pack_type ON (pack_capacity_data.packformat = dim_pack_type.name)
            WHERE extract_datetime = (SELECT MAX(extract_datetime)FROM dss.pack_capacity_data);
        """
        df = self.database_dss.select_query(sql)
        return df


class GetMarketData:
    """ Class to extract planning data"""
    logger = logging.getLogger(f"{__name__}.GetPlanningData")
    database_central = DatabaseModelsClass('MYSQLCENTRAL')

    def get_market_data(self):
        self.logger.info(f"---  Getting planning data from central")
        """ Get blok plan for future. """
        seasons = [2022, 2021]

        df = pd.DataFrame()
        for season in seasons:
            path = os.path.join(config.ROOTDIR, 'data', 'external', f'Production_Regions_{season}.xlsx')
            dft = pd.read_excel(path)
            df = pd.concat([df,dft])
            
        return df
