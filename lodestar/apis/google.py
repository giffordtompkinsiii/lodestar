"""Module contains all methods for Google App access, modification and exports.

This module provides methods and instructions for creating the 
necessary credentials to access the requisite Google APIs and 
manipulate the objects therein.

Classes
-------
GoogleConfig
    Class for accessing and configuring Google API access.
GoogleNewReport
    A Google Sheet object to which the program will export the reports.

References
----------
API PyDoc: Documentation
    https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/index.html
Python Quickstart: Guide
    https://developers.google.com/docs/api/quickstart/python
    
Methods
-------
main(sheet_title)->str
    Export reports with given title or current date and returns URL.
"""
from __future__ import print_function

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import os
import pickle
import numpy as np
import pandas as pd
import datetime as dt
import calendar as cal

from string import ascii_uppercase
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from .. import data_file_dir, logger

class GoogleConfig(object):
    """Class for configuring Google API access.

    Creates a token.pickle from the credential file. The file token.pickle 
    stores the user's access and refresh tokens, and is created 
    automatically when the authorization flow completes for the first time.

    In this version, only `drive` and `spreadsheet` access are required. 
    Any alterations to the `SERVICES` attribute will require a new 
    authorization `token.pickle`.

    For a complete list of scopes and services go to: 
    https://developers.google.com/identity/protocols/oauth2/scopes#gmailv1.

    Attributes
    ----------
    SERVICES : list
        list of services authorized by the token
    SCOPES : list
        formatted list of SERVICES for use by google's InstalledAppFlow() 
    token_path : str
        file path for the authorization token
    creds_path : str
        file path for projectt API creedentials JSON
    creds : google.oauth2.credentials.Credentials
        A Credentials object (see google codumentation for details).
    
    Methods
    --------
    create_sheets_service():
        Build the Google Sheets service given the user's credentials.
    create_drive_service(self):
        Build the Google Drive service given the user's credentials.


    """
    SERVICES = [
        ('drive','v3'),
        ('spreadsheets','v4')
        ]

    SCOPES = ['https://www.googleapis.com/auth/' + service for service, version in SERVICES]

    def __init__(self):
        creds = None
        self.token_path = os.path.join(data_file_dir, 'token.pickle')
        self.creds_path = os.path.join(data_file_dir, 'credentials.json')
        drive_api_url = 'https://developers.google.com/drive/api/v3/quickstart/python'

        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, 
                                                          self.SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.creds_path, 
                                                                 self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_path, 'w') as token:
                token.write(creds.to_json())
        self.creds = creds

    def create_sheets_service(self):
        """Build the Google Sheets service given the user's credentials."""
        self.sheets_service = build('sheets','v4', credentials=self.creds)
        return self.sheets_service

    def create_drive_service(self):
        """Build the Google Drive service given the user's credentials."""
        self.drive_service = build('drive','v3', credentials=self.creds)
        return self.drive_service

class GoogleSheet(GoogleConfig):
    def __init__(self, sheet_id):
        self.sheet_id = sheet_id

    def update_sheet(self, 
                    sheet_name:str, 
                    data_object:pd.DataFrame,
                    include_update_time:bool = True,
                    update_time_offset:int = 0):
        data = data_object.copy()
        # for col in data.select_dtypes(include=np.datetime64).columns:
        #     data[col] = data[col].astype(str)
        data = data.astype(str)
        data_arrays = []
        data_arrays.append(data.columns.to_list())
        data_arrays.extend(data.values.tolist())

        data_range = f'{sheet_name}!A1:{ascii_uppercase[data.shape[1]]}{data.shape[0]+1}'

        body_data = {
            'valueInputOption': 'USER_ENTERED',
            'data': [
                {
                    "values": data_arrays,
                    "majorDimension":"ROWS",
                    "range":data_range
                }
            ]
            
        }
        if include_update_time:
            body_data['data'].append(
                                {
                                    "values": [[f'Last Updated {dt.datetime.now()}']],
                                    "majorDimension": "ROWS",
                                    "range":f'{sheet_name}!{ascii_uppercase[data.shape[1]+1]}1:{ascii_uppercase[data.shape[1] + update_time_offset + 2]}1'
                                }
                            )

        google = GoogleConfig()
        sheets = google.create_sheets_service()
        spreadsheets = sheets.spreadsheets()
        ss_values = spreadsheets.values()
        clear_request = ss_values.clear(
            spreadsheetId=self.sheet_id,
            range=data_range
        )
        try:
            request = ss_values.batchUpdate(
                spreadsheetId=self.sheet_id,
                body=body_data
            )
        except TypeError:
            print(sheet_name)
            print(data.dtypes)
            return
        request.execute()


class GoogleNewReport(object):
    """A Google Sheet workbook to which the reports will be exported.

    The Google Sheet object that holds all the reports after generation.
    This sheet is in a shared folder accessible by the whole team.

    Attributes
    ----------
    folder_id : str
        the id of the `Daily Report` folder in Google Drive
    file_metadata : dict
        the feature dictionary that defines the Google Sheet
    resource : dict
        feature dictionary of the report after creation
    """
    def __init__(self, drive_app, file_title):
        """
        Parameters
        ----------
        drive_app : googleapiclient.discovery.Resource
            A Resource object (see Google documentation for more information).
        file_title : str
            the name of the Google Sheet to be created
        """
        self.folder_id = '10WfMUhkSLWs-d6RzTZXbBFzkr2_x4_3K' # Should feed it to this link: https://drive.google.com/drive/folders/10WfMUhkSLWs-d6RzTZXbBFzkr2_x4_3K
        self.file_metadata = {
                'name':file_title,
                'parents':[self.folder_id],
                'mimeType':'application/vnd.google-apps.spreadsheet'
            }
        self.resource = drive_app.files() \
                                 .create(body=self.file_metadata) \
                                 .execute()



def main(sheet_title=None):
    """Export reports with given title or current date and returns URL.

    Parameters
    ----------
    sheet_title : str, optional
        the title of the sheet to create

    Returns
    -------
    str
        A printed url to access the newly exported reports.
    """
    daily_reports_file_id = "10WfMUhkSLWs-d6RzTZXbBFzkr2_x4_3K"

    if not sheet_title:
        # Create title based off the current date.
        td = dt.date.today()
        current_year = td.year
        current_month = td.month
        # TODO : Organize reports by month and year.
        # current_report_folder_name = f"{str(current_month).zfill(2)}_{cal.month_name[current_month]}"

        sheet_title = f"{str(td.day).zfill(2)}_{cal.month_abbr[td.month]}_{td.year}"

    google = GoogleConfig()
    drive = google.create_drive_service()
    sheets = google.create_sheets_service()
    report = GoogleNewReport(drive, sheet_title)

    spreadsheet_id = report.resource['id']

    report_list = [ #(Google Sheet Page Title, report_title.csv)
        ('Believability','believability.csv'),
        ('Drop Report', 'drop_report.csv'),
        ('Pop Report', 'pop_report.csv'),
        ('Qtrly Tidemarks', 'tm_qtrly.csv'),
        ('Daily Tidemarks', 'tm_daily.csv'),
        ('Tidemark By Type', 'tm_by_type.csv'),
        ('Ideal Portfolio', 'ideal_portfolio.csv')
    ]

    # Create a sheet for each report in report_list.
    requests = [
        {
            "addSheet" : {
                "properties" : {
                    "title" : r[0],
                    "sheetId": i + 1,
                    "index": i + 1,
                    "sheetType": "GRID"
                    }
                }
        } for i, r in enumerate(report_list)]

    # Delete the initial blank page automatically generated by the API
    requests += [
        {
            "deleteSheet":{
                "sheetId": 0}
        }
    ]

    # Insert data from the reports into the created sheets.
    # TODO: Confirm that reports directory is properly mapped.
    requests += [
        {
            "pasteData": {
                "coordinate": {
                    "sheetId": i + 1,
                    "rowIndex": 0,
                    "columnIndex": 0
                },
                "data": open(f'./reports/{r[1]}','r').read(),
                "type": "PASTE_VALUES",
                "delimiter": ","
            }
        } for i, r in enumerate(report_list) if os.path.exists(f'../reports/{r[1]}')]

    batch_request = {
                "requests": requests,
                "includeSpreadsheetInResponse": True
    }

    # Execute the batch request through the API
    response = sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_request).execute()
    return f"`Cmd+Click` the following link for report {title}: https://docs.google.com/spreadsheets/d/{report.resource['id']}"


def update_sheet(sheet_id: str, 
                 sheet_name:str, 
                 data_object:pd.DataFrame,
                 include_update_time:bool = True,
                 update_time_offset:int = 0):
    data = data_object.copy()
    # for col in data.select_dtypes(include=np.datetime64).columns:
    #     data[col] = data[col].astype(str)
    data = data.astype(str)
    data_arrays = []
    data_arrays.append(data.columns.to_list())
    data_arrays.extend(data.values.tolist())

    data_range = f'{sheet_name}!A1:{ascii_uppercase[data.shape[1]]}{data.shape[0]+1}'
    clear_range = f'{sheet_name}!A:{ascii_uppercase[data.shape[1] - 1]}'

    body_data = {
        'valueInputOption': 'USER_ENTERED',
        'data': [
            {
                "values": data_arrays,
                "majorDimension":"ROWS",
                "range":data_range
            }
        ]
        
    }
    offset = update_time_offset
    if include_update_time:
        range_str = f'{sheet_name}!{ascii_uppercase[data.shape[1] + offset + 2]}1'
        body_data['data'].append(
                            {
                                "values": [[f'Last Updated {dt.datetime.now()}']],
                                "majorDimension": "ROWS",
                                "range": range_str
                            }
                        )

    google = GoogleConfig()
    sheets = google.create_sheets_service()
    spreadsheets = sheets.spreadsheets()
    ss_values = spreadsheets.values()
    clear_request = ss_values.clear(
        spreadsheetId=sheet_id,
        range=clear_range
    )
    
    print(clear_request.execute()['clearedRange'], "cleared")
    try:
        request = ss_values.batchUpdate(
            spreadsheetId=sheet_id,
            body=body_data
        )
    except TypeError:
        print(sheet_name)
        print(data.dtypes)
        return
    request.execute()

if __name__=="__main__":
    main(sheet_title)