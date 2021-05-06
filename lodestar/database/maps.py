from . import logger
from .models import Api, Asset, Client, Tidemark
from .functions import all_query, collection_to_dataframe 

asset_map = {a.id: a for a in all_query(Asset)}
client_map = {c.id: c for c in all_query(Client)}
tidemark_map = {tm.id:tm for tm in all_query(Tidemark)}

tm_id_name_map = {
    tm.id: tm.tidemark for tm in all_query(Tidemark)
}
tm_name_id_map = {v:k for k,v in tm_id_name_map.items()}

account_map = {a.account: a for a in all_query(Account)}
a_map = {a.asset: a for a in all_query(Asset)}
api_map = {api.api: api for api  in all_query(Api)}

# account_map = {a.id:a for a in all_query(Account)}

