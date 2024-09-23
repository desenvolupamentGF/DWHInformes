# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
ENVIRONMENT = 1
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!

# for logging purposes
import logging

# for hash/encrypt reasons
import hashlib

# Import needed library for HTTP requests
import requests

# extra imports
import sys
import datetime
from utils import send_email, connectSQLServer, disconnectSQLServer, connectMySQL, disconnectMySQL
from utils import calculate_access_token, calculate_json_header
import os

# End points URLs
URL_WORKERS = '/workers'
URL_COMPANIES = '/companies'
URL_DEPARTMENTS = '/departments'
URL_WORKFORCES = '/workforces'
URL_WORKERDAILYCOSTS = '/workerDailyCosts'

# Glam Suite constants
GLAMSUITE_DEFAULT_COMPANY_ID = os.environ['GLAMSUITE_DEFAULT_COMPANY_ID']

# Database constants
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']

DWH_SQLSERVER_USER = os.environ['DWH_SQLSERVER_USER']
DWH_SQLSERVER_PASSWORD = os.environ['DWH_SQLSERVER_PASSWORD']
DWH_SQLSERVER_HOST = os.environ['DWH_SQLSERVER_HOST']
DWH_SQLSERVER_DATABASE = os.environ['DWH_SQLSERVER_DATABASE']

# Other constants
CONN_TIMEOUT = 50

URL_API = os.environ['URL_API_TEST']
if ENVIRONMENT == 1:
    URL_API = os.environ['URL_API_PROD']

# TO BE USED TO DECIDE IF SOME INFORMATION CHANGED DEPENDING ON A HASH
def get_value_from_database(mycursor, correlation_id: str, endPoint):
    mycursor.execute("SELECT hash FROM Datawarehouse.dbo.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) + "' AND correlationId = '" + str(correlation_id) + "' AND deploy = " + str(ENVIRONMENT))
    myresult = mycursor.fetchall()

    hash = None
    for x in myresult:
        hash = str(x[0])

    return hash

# TO LOG ERRORS AND WARNINGS
def save_log_database(dbOrigin, mycursor, endPoint, message, typeLog):
    sql = "INSERT INTO ERP_GF.ERPIntegrationLog (dateLog, companyId, endpoint, deploy, message, typeLog) VALUES (NOW(), %s, %s, %s, %s, %s) "
    val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), str(endPoint), str(ENVIRONMENT), str(message), str(typeLog))
    mycursor.execute(sql, val)
    dbOrigin.commit()  

def get_timeDim(dbDWH, myCursorDWH, now, dbOrigin, myCursor):
    logging.info('   Creating times')

    # creating times
    try:
        # Check it times already exists for current year
        sql = "SELECT COUNT(*) FROM Datawarehouse.dbo.TimeDim WHERE year = YEAR(GETDATE()) " 
        myCursorDWH.execute(sql)
        result = myCursorDWH.fetchone()[0]
        
        if result == 0:

            # Insert all times for current year
            current_year = datetime.datetime.now().year
            first_day = datetime.datetime(current_year, 1, 1) 
            last_day = datetime.datetime(current_year, 12, 31) 

            current_day = first_day
            while current_day <= last_day:

                #logging.info('   Day is: ' + str(current_day))

                sql = ""
                sql = sql + "INSERT INTO Datawarehouse.dbo.TimeDim (Id, Year, Quarter, Month_Num, Month, Week, Day) VALUES ("
                sql = sql + " '" + str(current_day) + "', "
                sql = sql + " YEAR('" + str(current_day) + "'), "
                sql = sql + " DATEPART(QUARTER, '" + str(current_day) + "'), "
                sql = sql + " MONTH('" + str(current_day) + "'), "
                sql = sql + " FORMAT(CONVERT(DATETIME, '" + str(current_day) + "', 20), 'MMMM', 'en-US'), "
                sql = sql + " DATEPART(WEEK, '" + str(current_day) + "'), "
                sql = sql + " DAY('" + str(current_day) + "') "
                sql = sql + ") "
                myCursorDWH.execute(sql)

                current_day = current_day + datetime.timedelta(days=1)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_Num = '0' + Month_Num WHERE LEN(Month_Num) = 1 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Week = '0' + Week WHERE LEN(Week) = 1 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Gener', Month_ES = 'Enero' WHERE Month_Num = 1 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Febrer', Month_ES = 'Febrero' WHERE Month_Num = 2 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Març', Month_ES = 'Marzo' WHERE Month_Num = 3 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Abril', Month_ES = 'Abril' WHERE Month_Num = 4 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Maig', Month_ES = 'Mayo' WHERE Month_Num = 5 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Juny', Month_ES = 'Junio' WHERE Month_Num = 6 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Juliol', Month_ES = 'Julio' WHERE Month_Num = 7 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Agost', Month_ES = 'Agosto' WHERE Month_Num = 8 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Setembre', Month_ES = 'Setiembre' WHERE Month_Num = 9 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Octubre', Month_ES = 'Octubre' WHERE Month_Num = 10 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Novembre', Month_ES = 'Noviembre' WHERE Month_Num = 11 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            sql = ""
            sql = sql + "UPDATE Datawarehouse.dbo.TimeDim SET Month_CA = 'Desembre', Month_ES = 'Diciembre' WHERE Month_Num = 12 AND Year = YEAR(GETDATE()) "
            myCursorDWH.execute(sql)

            # Commit
            dbDWH.commit() 

    except Exception as e:
        message = '   Unexpected error when creating times: ' + str(e)
        save_log_database(dbOrigin, myCursor, 'ERPInformeProductivitat', message, "ERROR")
        logging.error(message)
        send_email("ERPInformeProductivitat", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbDWH)
        sys.exit(1)

def get_companiesDim(dbDWH, myCursorDWH, now, dbOrigin, myCursor):
    logging.info('   Processing companies from origin ERP (GARCIA FAURA)')

    # processing companies from origin ERP (GARCIA FAURA)
    try:
        # Calculate access token and header for the request
        token = calculate_access_token(ENVIRONMENT)
        headers = calculate_json_header(token)        

        get_req = requests.get(URL_API + URL_COMPANIES, headers=headers,
                               verify=False, timeout=CONN_TIMEOUT)
        companies = get_req.json()

        # Insert all companies
        for company in companies:

            #logging.info('   Company is: ' + str(company))

            data_hash = hashlib.sha256(str(company).encode('utf-8')).hexdigest()
            old_data_hash = get_value_from_database(myCursorDWH, str(company['id']), "CompanyDim")

            if old_data_hash is None:
                sql = "INSERT INTO Datawarehouse.dbo.CompanyDim (id, code, legalName, tradeName, taxId) VALUES (%s, %s, %s, %s, %s) "
                val = (str(company['id']), str(company['code']), str(company['fiscalName']), str(company['tradeName']), str(company['vatNumber']))
                myCursorDWH.execute(sql, val)

                sql = "INSERT INTO Datawarehouse.dbo.ERPIntegration (companyId, endpoint, correlationId, deploy, hash) VALUES (%s, %s, %s, %s, %s) "
                val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), "CompanyDim", str(company['id']), str(ENVIRONMENT), str(data_hash))
                myCursorDWH.execute(sql, val)
            else:
                if str(old_data_hash) != str(data_hash):
                    sql = "UPDATE Datawarehouse.dbo.CompanyDim SET code = %s, legalName = %s, tradeName = %s, taxId = %s WHERE id = %s "
                    val = (str(company['code']), str(company['fiscalName']), str(company['tradeName']), str(company['vatNumber']), str(company['id']))
                    myCursorDWH.execute(sql, val)                    

                    sql = "UPDATE Datawarehouse.dbo.ERPIntegration SET hash = %s WHERE companyId = %s AND endpoint = %s AND correlationId = %s AND deploy = %s "
                    val = (str(data_hash), str(GLAMSUITE_DEFAULT_COMPANY_ID), "CompanyDim", str(company['id']), str(ENVIRONMENT))
                    myCursorDWH.execute(sql, val)                    

        # Commit
        dbDWH.commit() 

    except Exception as e:
        message = '   Unexpected error when processing companies from original ERP (GARCIA FAURA): ' + str(e)
        save_log_database(dbOrigin, myCursor, 'ERPInformeProductivitat', message, "ERROR")
        logging.error(message)
        send_email("ERPInformeProductivitat", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbDWH)
        sys.exit(1)

def get_workersDim(dbDWH, myCursorDWH, now, dbOrigin, myCursor):
    logging.info('   Processing workers from origin ERP (GARCIA FAURA)')

    # processing workers from origin ERP (GARCIA FAURA)
    try:
        # Calculate access token and header for the request
        token = calculate_access_token(ENVIRONMENT)
        headers = calculate_json_header(token)        

        get_req = requests.get(URL_API + URL_WORKERS, headers=headers,
                               verify=False, timeout=CONN_TIMEOUT)
        workers = get_req.json()

        # Insert all workers
        for worker in workers:

            #logging.info('   Worker is: ' + str(worker))

            data_hash = hashlib.sha256(str(worker).encode('utf-8')).hexdigest()
            old_data_hash = get_value_from_database(myCursorDWH, str(worker['id']), "WorkerDim")

            if old_data_hash is None:
                sql = "INSERT INTO Datawarehouse.dbo.WorkerDim (id, code, name) VALUES (%s, %s, %s) "
                val = (str(worker['id']), str(worker['identificationNumber']), str(worker['name']))
                myCursorDWH.execute(sql, val)

                sql = "INSERT INTO Datawarehouse.dbo.ERPIntegration (companyId, endpoint, correlationId, deploy, hash) VALUES (%s, %s, %s, %s, %s) "
                val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), "WorkerDim", str(worker['id']), str(ENVIRONMENT), str(data_hash))
                myCursorDWH.execute(sql, val)
            else:
                if str(old_data_hash) != str(data_hash):
                    sql = "UPDATE Datawarehouse.dbo.WorkerDim SET code = %s, name = %s WHERE id = %s "
                    val = (str(worker['identificationNumber']), str(worker['name']), str(worker['id']))
                    myCursorDWH.execute(sql, val)                    

                    sql = "UPDATE Datawarehouse.dbo.ERPIntegration SET hash = %s WHERE companyId = %s AND endpoint = %s AND correlationId = %s AND deploy = %s "
                    val = (str(data_hash), str(GLAMSUITE_DEFAULT_COMPANY_ID), "WorkerDim", str(worker['id']), str(ENVIRONMENT))
                    myCursorDWH.execute(sql, val)                    

        # Commit
        dbDWH.commit() 

    except Exception as e:
        message = '   Unexpected error when processing workers from original ERP (GARCIA FAURA): ' + str(e)
        save_log_database(dbOrigin, myCursor, 'ERPInformeProductivitat', message, "ERROR")
        logging.error(message)
        send_email("ERPInformeProductivitat", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbDWH)
        sys.exit(1)

def get_departmentsDim(dbDWH, myCursorDWH, now, dbOrigin, myCursor):
    logging.info('   Processing departments from origin ERP (GARCIA FAURA)')

    # processing departments from origin ERP (GARCIA FAURA)
    try:
        # Calculate access token and header for the request
        token = calculate_access_token(ENVIRONMENT)
        headers = calculate_json_header(token)        

        get_req = requests.get(URL_API + URL_DEPARTMENTS, headers=headers,
                               verify=False, timeout=CONN_TIMEOUT)
        departments = get_req.json()

        # Insert all departments
        for department in departments:

            #logging.info('   Department is: ' + str(department))

            data_hash = hashlib.sha256(str(department).encode('utf-8')).hexdigest()
            old_data_hash = get_value_from_database(myCursorDWH, str(department['id']), "DepartmentDim")

            if old_data_hash is None:
                sql = "INSERT INTO Datawarehouse.dbo.DepartmentDim (id, name) VALUES (%s, %s) "
                val = (str(department['id']), str(department['name']))
                myCursorDWH.execute(sql, val)

                sql = "INSERT INTO Datawarehouse.dbo.ERPIntegration (companyId, endpoint, correlationId, deploy, hash) VALUES (%s, %s, %s, %s, %s) "
                val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), "DepartmentDim", str(department['id']), str(ENVIRONMENT), str(data_hash))
                myCursorDWH.execute(sql, val)
            else:
                if str(old_data_hash) != str(data_hash):
                    sql = "UPDATE Datawarehouse.dbo.DepartmentDim SET code = %s, name = %s WHERE id = %s "
                    val = (str(department['code']), str(department['name']), str(department['id']))
                    myCursorDWH.execute(sql, val)                    

                    sql = "UPDATE Datawarehouse.dbo.ERPIntegration SET hash = %s WHERE companyId = %s AND endpoint = %s AND correlationId = %s AND deploy = %s "
                    val = (str(data_hash), str(GLAMSUITE_DEFAULT_COMPANY_ID), "DepartmentDim", str(department['id']), str(ENVIRONMENT))
                    myCursorDWH.execute(sql, val)                    

        # Commit
        dbDWH.commit() 

    except Exception as e:
        message = '   Unexpected error when processing departments from original ERP (GARCIA FAURA): ' + str(e)
        save_log_database(dbOrigin, myCursor, 'ERPInformeProductivitat', message, "ERROR")
        logging.error(message)
        send_email("ERPInformeProductivitat", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbDWH)
        sys.exit(1)

def get_workforcesDim(dbDWH, myCursorDWH, now, dbOrigin, myCursor):
    logging.info('   Processing workforces from origin ERP (GARCIA FAURA)')

    # processing workforces from origin ERP (GARCIA FAURA)
    try:
        # Calculate access token and header for the request
        token = calculate_access_token(ENVIRONMENT)
        headers = calculate_json_header(token)        

        get_req = requests.get(URL_API + URL_WORKFORCES, headers=headers,
                               verify=False, timeout=CONN_TIMEOUT)
        workforces = get_req.json()

        # Insert all workforces
        for workforce in workforces:

            #logging.info('   Workforce is: ' + str(workforce))

            data_hash = hashlib.sha256(str(workforce).encode('utf-8')).hexdigest()
            old_data_hash = get_value_from_database(myCursorDWH, str(workforce['id']), "WorkforceDim")

            if old_data_hash is None:
                sql = "INSERT INTO Datawarehouse.dbo.WorkforceDim (id, name) VALUES (%s, %s) "
                val = (str(workforce['id']), str(workforce['name']))
                myCursorDWH.execute(sql, val)

                sql = "INSERT INTO Datawarehouse.dbo.ERPIntegration (companyId, endpoint, correlationId, deploy, hash) VALUES (%s, %s, %s, %s, %s) "
                val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), "WorkforceDim", str(workforce['id']), str(ENVIRONMENT), str(data_hash))
                myCursorDWH.execute(sql, val)
            else:
                if str(old_data_hash) != str(data_hash):
                    sql = "UPDATE Datawarehouse.dbo.WorkforceDim SET code = %s, name = %s WHERE id = %s "
                    val = (str(workforce['code']), str(workforce['name']), str(workforce['id']))
                    myCursorDWH.execute(sql, val)                    

                    sql = "UPDATE Datawarehouse.dbo.ERPIntegration SET hash = %s WHERE companyId = %s AND endpoint = %s AND correlationId = %s AND deploy = %s "
                    val = (str(data_hash), str(GLAMSUITE_DEFAULT_COMPANY_ID), "WorkforceDim", str(workforce['id']), str(ENVIRONMENT))
                    myCursorDWH.execute(sql, val)                    

        # Commit
        dbDWH.commit() 

    except Exception as e:
        message = '   Unexpected error when processing workforces from original ERP (GARCIA FAURA): ' + str(e)
        save_log_database(dbOrigin, myCursor, 'ERPInformeProductivitat', message, "ERROR")
        logging.error(message)
        send_email("ERPInformeProductivitat", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbDWH)
        sys.exit(1)

def get_workerDailyCostsFact(dbDWH, myCursorDWH, now, dbOrigin, myCursor):
    logging.info('   Processing worker daily costs from origin ERP (GARCIA FAURA)')

    # processing worker daily costs from origin ERP (GARCIA FAURA)
    try:
        # Calculate access token and header for the request
        token = calculate_access_token(ENVIRONMENT)
        headers = calculate_json_header(token)        

        strFrom = datetime.date.today() - datetime.timedelta(90) # Darrers tres mesos
        get_req = requests.get(URL_API + URL_WORKERDAILYCOSTS + "?startDate=" + str(strFrom), headers=headers,
                               verify=False, timeout=CONN_TIMEOUT)
        workerDailyCosts = get_req.json()

        # Insert all workerDailyCosts
        i = 0
        for workerDailyCost in workerDailyCosts:

            logging.info('   workerDailyCost is: ' + str(workerDailyCost))

            data_hash = hashlib.sha256(str(workerDailyCost).encode('utf-8')).hexdigest()
            old_data_hash = get_value_from_database(myCursorDWH, str(workerDailyCost['id']), "WorkerDailyCostsFact")

            if old_data_hash is None:
                sql = "INSERT INTO Datawarehouse.dbo.WorkerDailyCostsFact (id, date, workerId, departmentId, workforceId, companyId, hours, productiveHours, totalCost) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                val = (str(workerDailyCost['id']), str(workerDailyCost['date']), str(workerDailyCost['workerId']), str(workerDailyCost['departmentId']), str(workerDailyCost['workforceId']), str(workerDailyCost['companyId']), str(workerDailyCost['hours']), str(workerDailyCost['productiveHours']), str(workerDailyCost['totalCost']))
                myCursorDWH.execute(sql, val)

                sql = "INSERT INTO Datawarehouse.dbo.ERPIntegration (companyId, endpoint, correlationId, deploy, hash) VALUES (%s, %s, %s, %s, %s) "
                val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), "WorkerDailyCostsFact", str(workerDailyCost['id']), str(ENVIRONMENT), str(data_hash))
                myCursorDWH.execute(sql, val)
            else:
                if str(old_data_hash) != str(data_hash):
                    sql = "UPDATE Datawarehouse.dbo.WorkerDailyCostsFact SET date = %s, workerId = %s, departmentId = %s, workforceId = %s, companyId = %s, hours =%s, productiveHours = %s, totalCost = %s WHERE id = %s "
                    val = (str(workerDailyCost['date']), str(workerDailyCost['workerId']), str(workerDailyCost['departmentId']), str(workerDailyCost['workforceId']), str(workerDailyCost['companyId']), str(workerDailyCost['hours']), str(workerDailyCost['productiveHours']), str(workerDailyCost['totalCost']), str(workerDailyCost['id']))
                    myCursorDWH.execute(sql, val)                    

                    sql = "UPDATE Datawarehouse.dbo.ERPIntegration SET hash = %s WHERE companyId = %s AND endpoint = %s AND correlationId = %s AND deploy = %s "
                    val = (str(data_hash), str(GLAMSUITE_DEFAULT_COMPANY_ID), "WorkerDailyCostsFact", str(workerDailyCost['id']), str(ENVIRONMENT))
                    myCursorDWH.execute(sql, val)                    

            i = i + 1
            if i % 100 == 0:
                # Commit every 1000
                dbDWH.commit() 

        # Commit
        dbDWH.commit() 

    except Exception as e:
        message = '   Unexpected error when processing worker daily costs from original ERP (GARCIA FAURA): ' + str(e)
        save_log_database(dbOrigin, myCursor, 'ERPInformeProductivitat', message, "ERROR")
        logging.error(message)
        send_email("ERPInformeProductivitat", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbDWH)
        sys.exit(1)

def main():

    executionResult = "OK"

    # current date and time
    now = datetime.datetime.now() 

    # set up logging
    logging.basicConfig(filename=os.environ['LOG_FILE_ERPInformeProductivitat'], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START ERP Informe Productivitat - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to database (MySQL)
    db = None
    try:
        db = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
        myCursor = db.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to MySQL database: ' + str(e))
        send_email("ERPInformeProductivitat", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(db)
        sys.exit(1)

    # connecting to DWH database (SQLServer)
    dbDWH = None
    try:
        dbDWH = connectSQLServer(DWH_SQLSERVER_USER, DWH_SQLSERVER_PASSWORD, DWH_SQLSERVER_HOST, DWH_SQLSERVER_DATABASE)
        myCursorDWH = dbDWH.cursor()
    except Exception as e:
        message = '   Unexpected error when connecting to SQLServer DWH database: ' + str(e)
        save_log_database(db, myCursor, 'ERPInformeProductivitat', message, "ERROR")
        logging.error(message)
        send_email("ERPInformeProductivitat", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbDWH)
        sys.exit(1)

    get_timeDim(dbDWH, myCursorDWH, now, db, myCursor)
    get_companiesDim(dbDWH, myCursorDWH, now, db, myCursor)
    get_workersDim(dbDWH, myCursorDWH, now, db, myCursor)      
    get_departmentsDim(dbDWH, myCursorDWH, now, db, myCursor) 
    get_workforcesDim(dbDWH, myCursorDWH, now, db, myCursor) 
    get_workerDailyCostsFact(dbDWH, myCursorDWH, now, db, myCursor) 

    # Send email with execution summary
    send_email("ERPInformeProductivitat", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END ERP Informe Productivitat - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('')

    # Closing databases
    db.close()
    myCursor.close()
    myCursorDWH.close()
    dbDWH.close()

    sys.exit(0)

    #logging.debug('debug message')
    #logging.info('info message')
    #logging.warning('warn message')
    #logging.error('error message')
    #logging.critical('critical message')

if __name__ == '__main__':
    main()