Script must be executed like this from the following location: Python\db_mods>py -m module.main

To initialize a python virtual environment
python -m venv venv or py -3 -m venv venv

To start the virtual environment:
make sure you're at this path end: Files\Python\db_mods>
run this: .\venv\Scripts\Activate

To deactivate a virtual environment:
make sure you're at this path end: Files\Python\db_mods>
run this: deactivate

.env variables needed:
    PG_DB_USER
    PG_DB_PASS
    SSMS_DB_SERVER
    SSMS_DB_DATABASE
    SSMS_DB_USERNAME
    SSMS_DB_PASS