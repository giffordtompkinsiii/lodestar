from . import logger

from .models import Account, Api, Asset, Client, Tidemark, TidemarkType
from .functions import all_query, collection_to_dataframe 

account_map  = {a.id: a for a in all_query(Account)}
api_map      = {a.id: a for a in all_query(Api)}
asset_map    = {a.id: a for a in all_query(Asset)}
client_map   = {c.id: c for c in all_query(Client)}
tidemark_map = {t.id: t for t in all_query(Tidemark)}
account_type_map = {t.id: t for t in all_query(TidemarkType)}

