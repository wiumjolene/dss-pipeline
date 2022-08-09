# -*- coding: utf-8 -*-
import logging
import os
from src.utils import config
from src.make_feature import MakeFeatures, PrepModelData, MakeMarketData

class MainController:
    """ Decide which parts of the module to update. """
    logger = logging.getLogger(f"{__name__}.MainController")
    synch_data = False
    make_data = False
    market_data = True


    def pipeline_control(self):
        mf = MakeFeatures()
        pdp = PrepModelData()
        market = MakeMarketData()

        if self.synch_data:
            self.logger.info('SYNC DATA')
            plan = mf.save_planning_data()
            harvest = mf.save_harvest_estimate_data()
            harvest_quicadj = mf.save_harvest_estimate_quickadj_data()
            harvest_0638 = mf.save_harvest_estimate_0638_data()
            capacity = mf.save_pack_capacity()

            if (plan and harvest and capacity and harvest_quicadj and harvest_0638):
                mf.notify(True, 'save_planning_data and save_harvest_estimate_data and save_pack_capacity and harvest_quicadj andsave_harvest_estimate_0638_data')

        if self.make_data:
            self.logger.info('MAKE DATA')
            dp=pdp.prep_demand_plan()
            he=pdp.prep_harvest_estimates()
            pc=pdp.prep_pack_capacity()

            if (dp and he and pc):
                mf.notify(True, 'prep_demand_plan and prep_harvest_estimates and prep_pack_capacity')

        if self.market_data:
            market.save_market()

