"""Module contains all methods for Google App access, modification and exports.

This module provides methods and instructions for creating the
necessary credentials to access the requisite Google APIs and
manipulate the objects therein.

Classes
-------
GoogleConfig
    Class for accessing and configuring Google API access.
GoogleNewReport
    A Google Sheet object to which the program will export the reporting.

References
----------
API PyDoc: Documentation
    https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/index.html
Python Quickstart: Guide
    https://developers.google.com/docs/api/quickstart/python

Methods
-------
main(sheet_title)->str
    Export reporting with given title or current date and returns URL.
"""

from lodestar import data_file_dir

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from string import ascii_uppercase

import datetime as dt
import pandas as pd
import os


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
        ('drive', 'v3'),
        ('spreadsheets', 'v4')
    ]

    SCOPES = ['https://www.googleapis.com/auth/' + service for service, version in SERVICES]

    def __init__(self):
        self._token_path = os.path.join(data_file_dir, 'token.pickle')
        self._creds_path = os.path.join(data_file_dir, 'credentials.json')

        creds = None
        if os.path.exists(self._token_path):
            creds = Credentials.from_authorized_user_file(self._token_path, self.SCOPES)

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self._creds_path, self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self._token_path, 'w') as token:
                token.write(creds.to_json())
        self.creds = creds

    # def create_drive_service(self):
    #     """Build the Google Drive service given the user's credentials."""
    #     self.drive_service = build('drive', 'v3', credentials=self.creds)
    #     return self.drive_service


class GoogleSheetsService(GoogleConfig):
    def create_sheets_service(self):
        """Build the Google Sheets service given the user's credentials."""
        return build('sheets', 'v4', credentials=self.creds)

    @staticmethod
    def get_data_range(sheet_name, dataframe):
        return f'{sheet_name}!A1:{ascii_uppercase[dataframe.shape[1]]}{dataframe.shape[0] + 1}'

    @staticmethod
    def data_to_str_array(data):
        return data.astype(str).to_numpy()

    @staticmethod
    def data_to_array(data):
        return data.to_numpy()

    def get_body_data(self, sheet_name, dataframe):
        data_arrays = self.data_to_str_array(dataframe)

        return {
            'valueInputOption': 'USER_ENTERED',
            'data': [{
                "values": dataframe.astype(str).values.tolist(), # data_arrays,
                "majorDimension": "ROWS",
                "range": self.get_data_range(sheet_name=sheet_name, dataframe=dataframe)
            }, {
                "values": [[f'Last Updated {dt.datetime.now()}']],
                "majorDimension": "ROWS",
                "range": (f'{sheet_name}!'
                          + f'{ascii_uppercase[data_arrays.shape[1] + 1]}1:'
                          + f'{ascii_uppercase[data_arrays.shape[1] + 2]}1')
            }]}

    def __init__(self, sheet_id):
        super().__init__()
        self.sheet_id = sheet_id

        self._spreadsheets_service = self.create_sheets_service()
        self.spreadsheets = self._spreadsheets_service.spreadsheets()
        self.values = self.spreadsheets.values()

    def _clear_sheet(self, data_range):
        self.values.clear(spreadsheetId=self.sheet_id, range=data_range)

    def update_sheet(self, sheet_name: str, dataframe: pd.DataFrame):

        body_data = self.get_body_data(sheet_name=sheet_name, dataframe=dataframe)
        data_range = self.get_data_range(sheet_name=sheet_name, dataframe=dataframe)

        # Clear sheet of data.
        self._clear_sheet(data_range=data_range)

        # Attempt to post data.
        # try:
        request = self.values.batchUpdate(
            spreadsheetId=self.sheet_id,
            body=body_data
        )
        # except TypeError:
        #     print(sheet_name)
        #     print(dataframe.dtypes)
        #     return
        request.execute()


if __name__ == '__main__':
    import pandas as pd
    import numpy as np

    test_id = '1O_zmm_H4GbPU4e07lX2jIPbCMSKuu3L_5vwlEt2OrPk'
    g = GoogleSheetsService(test_id)
    sheet_name = 'Sheet 69'
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
    g.update_sheet(sheet_name, df)
