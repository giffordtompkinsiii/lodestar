"""
This module will have views specific for the running of the business.
"""
from lodestar.database import engine

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData

import pandas as pd

landing_metadata = MetaData(bind=engine, schema='presentation')

Base = automap_base(metadata=landing_metadata)


class PresentationView:
    data = pd.DataFrame


class WaterMarksView(PresentationView):
    pass


class CurrentBelievabilityView(PresentationView):
    pass
