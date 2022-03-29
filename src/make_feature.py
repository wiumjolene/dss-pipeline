import logging
import datetime

import pandas as pd
import numpy as np
import pymsteams
import os
from dotenv import find_dotenv, load_dotenv

from src.utils import config
from src.utils.connect import DatabaseModelsClass
from src.get_data import GetPlanningData, GetLocalData


class MakeFeatures:
    """ Class to make features of ... """
    logger = logging.getLogger(f"{__name__}.MakeFeatures")
    gpd = GetPlanningData()
    database_dss = DatabaseModelsClass('MYSQLLINUX')

    def notify(self, success, issue):
        load_dotenv(find_dotenv())
        WEBHOOK = os.environ.get('TEAMSWEBHOOK')
        myTeamsMessage = pymsteams.connectorcard(WEBHOOK)

        if success:
            message = f"Success with {issue}"
            myTeamsMessage.title("SUCCESS")

        else:
            message = f"@channel: Issue with {issue}"
            myTeamsMessage.title("WARNING JOLENE")

        myTeamsMessage.text(message)
        myTeamsMessage.send()

        return

    def save_planning_data(self):
        self.logger.info(f"- Retrieve and save planning data")
        
        try:
            planning_data = self.gpd.get_planningdata()
            self.database_dss.insert_table(planning_data,"planning_data","dss",if_exists='append')
            success = True
        
        except:
            self.notify(False, 'save_planning_data')
            success = False
        
        return success

    def save_harvest_estimate_data(self):
        self.logger.info(f"- Retrieve and save harvest estimate")
        
        try:
            df = self.gpd.get_harvest_estimate()
            self.database_dss.insert_table(df,"harvest_estimate_data","dss",if_exists='append')
            success = True
            
        
        except:
            self.notify(False, 'save_harvest_estimate_data')
            success = False
        
        return success

    def save_harvest_estimate_quickadj_data(self):
        self.logger.info(f"- Retrieve and save harvest estimate")
        
        try:
            df = self.gpd.get_harvest_estimate_quickadj()
            self.database_dss.insert_table(df,"harvest_estimate_quicadj_data","dss",if_exists='append')
            success = True
            
        
        except:
            self.notify(False, 'save_harvest_estimate_quicadj_data')
            success = False
        
        return success

    def save_harvest_estimate_0638_data(self):
        self.logger.info(f"- Retrieve and save harvest estimate")
        
        try:
            df = self.gpd.get_harvest_estimate_0638()
            self.database_dss.insert_table(df,"harvest_estimate_0638_data","dss",if_exists='append')
            success = True
            
        
        except:
            self.notify(False, 'save_harvest_estimate_0638_data')
            success = False
        
        return success

    def save_pack_capacity(self):
        self.logger.info(f"- Retrieve and save pack capacity")
        
        try:
            df = self.gpd.get_pack_capacity()
            self.database_dss.insert_table(df,"pack_capacity_data","dss",if_exists='append')
            success = True
            
        
        except:
            self.notify(False, 'get_pack_capacity')
            success = False
        
        return success


class PrepModelData:
    """ Class to make features of ... """
    logger = logging.getLogger(f"{__name__}.PrepModelData")
    gld = GetLocalData()
    database_dss = DatabaseModelsClass('MYSQLLINUX')
    mf = MakeFeatures()

    def prep_harvest_estimates(self):
        self.logger.info(f"- Prep harvest estimate")

        try:
            fc = self.gld.get_he_fc()
            if len(fc) > 0:
                self.database_dss.insert_table(fc, 'dim_fc', 'dss', if_exists='append')
                self.logger.info(f"-- Added {len(fc)} fcs")

            block = self.gld.get_he_block()
            if len(block) > 0:
                self.database_dss.insert_table(block, 'dim_block', 'dss', if_exists='append')
                self.logger.info(f"-- Added {len(block)} blocks")

            va = self.gld.get_he_va()
            if len(va) > 0:
                self.database_dss.insert_table(va, 'dim_va', 'dss', if_exists='append')
                self.logger.info(f"-- Added {len(va)} vas")

            he = self.gld.get_local_he()
            self.database_dss.execute_query('TRUNCATE `dss`.`f_harvest_estimate`;')
            he['add_datetime'] = datetime.datetime.now()
            self.database_dss.insert_table(he, 'f_harvest_estimate', 'dss', if_exists='append')
            success = True

        except:
            success = False
            self.mf.notify(False, 'prep_harvest_estimates')
            self.logger.info(f"-- Prep harvest estimate failed")
            
        return success

    def prep_demand_plan(self):
        self.logger.info(f"- Prep demand plan")

        try:
            client = self.gld.get_dp_client()
            if len(client) > 0:
                self.database_dss.insert_table(client, 'dim_client', 'dss', if_exists='append')
                self.logger.info(f"-- Added {len(client)} clients")

            dp = self.gld.get_local_dp()
            self.database_dss.execute_query('TRUNCATE `dss`.`f_demand_plan`;')
            dp['add_datetime'] = datetime.datetime.now()
            self.database_dss.insert_table(dp, 'f_demand_plan', 'dss', if_exists='append')
            success = True
        
        except:
            success = False
            self.mf.notify(False, 'prep_demand_plan')
            self.logger.info(f"-- Prep demand failed")

        return success

    def prep_pack_capacity(self):
        self.logger.info(f"- Prep pack capacities")

        try:
            packhouse = self.gld.get_pc_packhouse()
            if len(packhouse) > 0:
                self.database_dss.insert_table(packhouse, 'dim_packhouse', 'dss', if_exists='append')
                self.logger.info(f"-- Added {len(packhouse)} packhouses")

            pc = self.gld.get_local_pc()
            self.database_dss.execute_query('TRUNCATE `dss`.`f_pack_capacity`;')
            pc['add_datetime'] = datetime.datetime.now()
            self.database_dss.insert_table(pc, 'f_pack_capacity', 'dss', if_exists='append')
            success = True

        except:
            success = False
            self.mf.notify(False, 'prep_pack_capacity')
            self.logger.info(f"-- Prep pack failed")

        return success

        