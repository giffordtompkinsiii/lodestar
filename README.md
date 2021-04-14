# The Tides Group - Data Manager Protocols

# Directory Structure
``` # tree -I '*.pyc|__pycache__|*.xlsm|*.zip|*.sql|*.whl|build/' | pbcopy
.
├── README.md
├── lodestar
│   ├── __init__.py
│   ├── database
│   │   ├── __init__.py
│   │   ├── functions.py
│   │   ├── maps.py
│   │   └── models.py
│   ├── pipelines
│   │   ├── __init__.py
│   │   ├── believability.py
│   │   ├── daily.py
│   │   ├── import_procedures.py
│   │   └── prices.py
│   └── tidemarks
│       ├── __init__.py
│       ├── daily.py
│       └── growth.py
├── requirements.txt
└── setup.py

4 directories, 16 files
```

