# The Tides Group - Data Manager Protocols

# Directory Structure
``` # tree -I '*.pyc|__pycache__|*.xlsm|*.zip|*.sql|*.whl|build/' | pbcopy
.
├── bin
│   ├── connect_ip.sh
│   ├── create_dirs.sh
│   ├── remove_pyc.sh
│   ├── shutdown.sh
│   └── start-up.sh
├── creds
│   ├── credentials.json
│   └── token.pickle
├── data_manager
│   ├── database
│   │   ├── __init__.py
│   │   ├── functions.py
│   │   └── models.py
│   ├── google
│   │   ├── __init__.py
│   │   └── google_handler.py
│   ├── positions
│   │   ├── __init__.py
│   │   └── ibkr.py
│   ├── prices
│   │   ├── __init__.py
│   │   ├── pop_drop.py
│   │   └── prices.py
│   ├── reporting
│   │   ├── __init__.py
│   │   ├── believability.py
│   │   ├── ideal_portfolio.py
│   │   └── reports.py
│   ├── tests
│   │   ├── __init__.py
│   │   ├── test.py
│   │   └── test_database_connection.py
│   ├── tidemarks
│   │   ├── __init__.py
│   │   ├── daily.py
│   │   ├── quarterly.py
│   │   ├── ratios.py
│   │   ├── tm_daily.py
│   │   └── tm_qtrly.py
│   ├── variables
│   │   └── excel_import.py
│   ├── __init__.py
│   └── data_manager.py
├── LICENSE.md
├── README.md
├── requirements.txt
└── setup.py

11 directories, 37 files
```

