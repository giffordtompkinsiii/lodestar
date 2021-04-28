from . import logger
from .models import Asset, Tidemark
from .functions import all_query, collection_to_dataframe 

asset_map = {a.id: a for a in all_query(Asset)}
tidemark_map = {tm.id:tm for tm in all_query(Tidemark)}

tm_id_name_map = {
    tm.id: tm.tidemark for tm in all_query(Tidemark)
}
tm_name_id_map = {v:k for k,v in tm_id_name_map.items()}



# account_map = {a.id:a for a in all_query(Account)}

