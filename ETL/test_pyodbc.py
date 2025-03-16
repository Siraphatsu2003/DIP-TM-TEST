import pyodbc

print("Available ODBC drivers:")
for driver in pyodbc.drivers():
    print(driver)