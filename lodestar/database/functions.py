"""Database-to-Pandas-DataFrame compatability functions.

Many of these functions are not used elsewhere in the package. Many 
need to be altered, consolidated or deleted.

Methods
-------
compare_to_db(import_df, exist_db_model)->list, list:
    Return import or update items based on existing records.

update_database_object(import_df, db_table):
    Export items to database as insert or update statements.

collection_to_dataframe(query_results)->pandas.DataFrame
    Returns items in a models collection attribute as a dataframe.
    
all_query(table)->list:
    All-Query - Returns all records for given table object
"""
from tqdm import tqdm
import pandas as pd
import decimal
import datetime as dt
from sqlalchemy import types, UniqueConstraint, PrimaryKeyConstraint
from .models import session
from .. import logger, debug

def all_query(db_table):
    """All-Query - Returns all records for given table object."""
    return session.query(db_table)\
                  .order_by(*[c for c in db_table.__table__.primary_key.columns])\
                  .all()

def greater(db_table, col:str, val, inclusive: bool = False):
    if inclusive:
        return getattr(db_table, col) >= val
    return getattr(db_table, col) > val 

def less(db_table, col:str, val, inclusive: bool = False):
    if inclusive:
        return getattr(db_table, col) <= val
    return getattr(db_table, col) < val 

def equals(db_table, col:str, val):
    return getattr(db_table, col) == val

def filter_query(db_table, criterion):
    q = session.query(db_table)
    for criteria in criterion:
        q = q.filter(criteria)

    return q.order_by(*[c for c in db_table.__table__.primary_key.columns]).all()

    

def get_unique_cols(db_table=None, query_results=None):
    """Returns all columns under a tables Unique Constraint.
    
    These columns are used as the index columns when creating a dataframe and 
    when comparing to the database for new values.
    """
    if db_table:
        table_object = db_table.__table__
    else:
        table_object = query_results[0].__table__
    
    table_cons = table_object.constraints
    unique_cols = sorted(set([
                col.name for cols in [
                    c.columns for c in table_cons if type(c) is UniqueConstraint
                ] for col in cols]))
    primary_keys = sorted(set([
            col.name for cols in [
                c.columns for c in table_cons if type(c) is PrimaryKeyConstraint
            ] for col in cols]))
    return table_object, list(unique_cols), list(primary_keys)

def collection_to_dataframe(query_results,
                            db_table=None,
                            drop_last_modified=True)->pd.DataFrame:
    """Returns items in a models collection attribute as a dataframe.

    This function takes the list of database objects and extracts their
    column names. Returning a dataframe with the column names that 
    correspond to the database table's columns.
    
    Arguments
    ---------
    query_results : list
        A list of database objects. Often the value of a 
        `<model>`.`<dependent_table>_collection` attribute.

    Returns
    -------
    pandas.DataFrame
        A Dataframe with columns names corresponding to the objects attributes 
        passed to it Indexed by the unique key columns of the table.
    """
    # Set unique columns as index columns
    table_object, idx_cols, _ = get_unique_cols(db_table=db_table, 
                                                query_results=query_results)

    dataframe = pd.DataFrame([
        {k:v for k,v in q.__dict__.items() if k in table_object.columns} \
            for q in query_results])
            
    if drop_last_modified and ('last_modified' in dataframe.columns):
        dataframe = dataframe.drop(columns='last_modified')
    
    # Convert decimal types to float.
    dtype_dict = {
        c.name: float 
            if c.type.python_type in [decimal.Decimal, int]
            else 'datetime64' if c.type.python_type in [dt.date, dt.datetime]
            else c.type.python_type
        for c in table_object.columns if c.name in dataframe.columns}  

    try:
        df = dataframe.astype(dtype_dict)\
                      .set_index(idx_cols)\
                      .sort_index()
        return df
    except KeyError as e:
        logger.log(level=1, msg=e)
        return dataframe

def compare_to_db(import_df: pd.DataFrame, db_records: list, db_table,
                  insert_only: bool = False, ignore_nulls=False, 
                  debug=False)->(list, list):
    """Return import or update items based on existing records.

    Takes the new and/or calculated values and compares them to the current database model.
    Returns insert database objects to be imported and update objects to alter the database.

    Parameters
    ==========
    import_df: pd.DataFrame 
    db_records: sqlalchemy.ext.declarative.api.DeclarativeMeta 
        SQLAlchemy Object Relation Model that will be used as the mapper.

    Returns
    =======
    insert_objects list(dict): 
        Dictionary of mapping objects to be passed into session.
        bulk_insert_mappings() method.
    update_objects list(dict): 
        Dictionary of mapping objects to be passed into session.bulk_update_mappings() method.
    """
    # Convert database records to dataframe with unique key columns as index.

    insert_objects, update_objects = [], []
    if import_df.empty:
        return insert_objects, update_objects

    table_object, idx_cols, prmy_keys = get_unique_cols(db_table=db_table, 
                                                        query_results=db_records)
    if debug:
        print(idx_cols)

    db = collection_to_dataframe(db_records, db_table)
    if debug:
        print("Database Records:", db.shape)
    # Set index of importing dataframe to match database for comparision.
    # TODO: see if the following function replace previously used commented function
    im = import_df.loc[import_df.index.drop_duplicates()] \
                  .drop(columns=prmy_keys, errors='ignore')
    if not ignore_nulls:
        im = im.dropna()

    if debug:
        print("Cleaned import dataframe:", import_df.shape, "->", im.shape)

    if im.empty:
        return insert_objects, update_objects

    # If there are not previous database records, then return the insert objects 
    # as the entire import dataframe converted to records.
    if db.empty:
        insert_objects = im.reset_index().to_dict(orient="records")
    else:
        diff_idx = im.compare(db.reindex_like(im)).index
        insert_idx = diff_idx[~diff_idx.isin(db.index)]
        insert_objects = im.loc[insert_idx] \
                           .reset_index() \
                           .to_dict(orient="records")
        if insert_only:
            return insert_objects, []

        update_idx = diff_idx[diff_idx.isin(db.index)]
        update_objects = im.loc[update_idx]
        update_objects['id'] = db.loc[update_idx,'id']
        update_objects = update_objects.reset_index() \
                                       .to_dict(orient="records")

    return insert_objects, update_objects


def update_database_object(import_df, db_records, db_table, debug: bool = False,
                           insert_only: bool = False, refresh_object=None):
    """Export items to database as insert or update statements.
    
    This function uses the return from `compare_to_db(import_df=import_df, 
                                                      db_records=db_records, 
                                                      db_table=db_table)`
    to update the database with new records and records that need updating (rare and would only be the result of a reconfiguring of the pull process).
    
    ** NOTE: All New datarows must be free of 'Null' values or they will be 
    dropped upon insertion.**
    """
    insert_objects, update_objects = compare_to_db(import_df=import_df, 
                                                   db_records=db_records, 
                                                   db_table=db_table,
                                                   insert_only=insert_only,
                                                   debug=debug)
    if debug:
        print(f"Inserting {len(insert_objects)} and updating {len(update_objects)}")
    if insert_objects:
        for i in tqdm(iterable=range(0, len(insert_objects) // 1000 + 1),
                        position=2,
                        desc=f"Inserting Objects",
                        leave=False):
            objects_slice = insert_objects[1000 * i: 1000 * (i + 1)]
            if debug:
                print(f"Inserting {len(objects_slice)} records.")
            session.bulk_insert_mappings(db_table, objects_slice)
            session.commit()

    if update_objects:
        for i in tqdm(iterable=range(0, len(update_objects) // 100 + 1),
                        position=2,
                        desc=f"Updating Objects",
                        leave=False):
            objects_slice = update_objects[100 * i: 100 * (i + 1)]
            if debug:
                print(f"Updating {len(objects_slice)} records.")
            session.bulk_update_mappings(db_table, objects_slice)
            session.commit()
    if (insert_objects or update_objects) and refresh_object:
        #TODO Refer to prices.price() procedure for session.refresh(asset) method.
        session.refresh(refresh_object)
        return refresh_object

    return refresh_object
