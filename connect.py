import csv

import psycopg2


def write_csv_adm_JOIN_pat(cur):
    try:
        query = "SELECT admissions.hadm_id, admissions.admission_type, patients.gender, patients.dob FROM mimiciiidev.admissions INNER JOIN mimiciiidev.patients ON admissions.subject_id = patients.subject_id;"
        cur.execute(query)
        records = cur.fetchall()
        with open('admissionsLEFTJOINpatients.csv', 'w', newline='') as csvfile:
            a = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_ALL, quotechar='"', doublequote=True,
                           lineterminator='\n')
            a.writerows(records)
    except (Exception, psycopg2.Error) as error:
        print ("Error while fetching data from PostgreSQL", error)

def append_cols():
    with open('admissionsLEFTJOINpatients.csv', 'r') as csv_in, open('appendedCols.csv', 'w') as csv_out:
        for row in csv.reader(csv_in):
            # 18 Procedures, 1 class label
            for x in range(19):
                row.insert(4 + x, '0')
            csv.writer(csv_out).writerow(row)

def map_add_procedures_icd(cur):
    #insert_pro_categories
    dict_icd9_cat = {'00': '0',
                     '01': '1', '02': '1', '03': '1', '04': '1', '05': '1',
                     '06': '2', '07': '2',
                     '08': '3', '09': '3', '10': '3', '11': '3', '12': '3', '13': '3', '14': '3', '15': '3', '16': '3',
                     '17': '3A',
                     '18': '4', '19': '4', '20': '4',
                     '21': '5', '22': '5', '23': '5', '24': '5', '25': '5', '26': '5', '27': '5', '28': '5', '29': '5',
                     '30': '6', '31': '6', '32': '6', '33': '6', '34': '6',
                     '35': '7', '36': '7', '37': '7', '38': '7', '39': '7',
                     '40': '8', '41': '8',
                     '42': '9', '43': '9', '44': '9', '45': '9', '46': '9', '47': '9', '48': '9', '49': '9', '50': '9',
                     '51': '9', '52': '9', '53': '9', '54': '9',
                     '55': '10', '56': '10', '57': '10', '58': '10', '59': '10',
                     '60': '11', '61': '11', '62': '11', '63': '11', '64': '11',
                     '65': '12', '66': '12', '67': '12', '68': '12', '69': '12', '70': '12', '71': '12',
                     '72': '13', '73': '13', '74': '13', '75': '13',
                     '76': '14', '77': '14', '78': '14', '79': '14', '80': '14', '81': '14', '82': '14', '83': '14',
                     '84': '14',
                     '85': '15', '86': '15',
                     '87': '16', '88': '16', '89': '16', '90': '16', '91': '16', '92': '16', '93': '16', '94': '16',
                     '95': '16', '96': '16', '97': '16', '98': '16', '99': '16'}
    try:
        query_selectAll_procedureicds = "select hadm_id, icd9_code from mimiciiidev.procedures_icd;"
        cur.execute(query_selectAll_procedureicds)
        records_prcd_icd = cur.fetchall()
        with open('appendedRows.csv', 'r') as csv_in, open('procedures.csv', 'w') as csv_out:
            for rowcsv in csv.reader(csv_in):
                hadm_id_adm = str(rowcsv[0])
                print("Hadm_id_admissions: ", hadm_id_adm)

                for row in records_prcd_icd:
                    # print("Hadm_id: ", row[0], "Procedure_icd: ", row[1])
                    hadm_id_pro = str(row[0])
                    #print("Hadm_id_procedures: ", hadm_id_pro)
                    # Step 1: Extract first two chars of ICD9 Code (01-99)
                    icd9_key = row[1][0:2]
                    # print("Hadm_id: ", row[0], "Procedure_icd Key: ", icd9_key)
                    # Step 2: Alter ICD9 Code to Category (0-16)
                    icd9_val_cat = dict_icd9_cat[icd9_key]
                    #print("Procedure_icd Category: ", icd9_val_cat)

                    if hadm_id_adm == hadm_id_pro:
                        if (icd9_val_cat == '0' or icd9_val_cat == '1' or icd9_val_cat == '2' or icd9_val_cat == '3'):
                            #print('0-3')
                            #print(rowcsv[4 + int(icd9_val_cat)])
                            rowcsv[4 + int(icd9_val_cat)] = '1'
                            #print(rowcsv[4 + int(icd9_val_cat)])
                        elif (icd9_val_cat == '3A'):
                            #print('3A')
                            #print(rowcsv[8])
                            rowcsv[8] = '1'
                            #print(rowcsv[8])
                        else:
                            #print('4-16')
                            #print(rowcsv[5 + int(icd9_val_cat)])
                            rowcsv[5 + int(icd9_val_cat)] = '1'
                            #print(rowcsv[5 + int(icd9_val_cat)])
                csv.writer(csv_out).writerow(rowcsv)
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def add_class_labels_diagnoses(cur):
    try:
        query_selectAll_diagnosesIcds = "select hadm_id, icd9_code from mimiciiidev.diagnoses_icd;"
        cur.execute(query_selectAll_diagnosesIcds)
        records_diag_icd = cur.fetchall()
        with open('procedures.csv', 'r') as csv_in, open('classLabelsDiagnoses.csv', 'w') as csv_out:
            # -for each admission, check if diagnosis icd code related to ADE occured
            for rowcsv in csv.reader(csv_in):
                hadm_id_adm = str(rowcsv[0])
                #print(hadm_id_adm)
                for row in records_diag_icd:
                    hadm_id_diag = str(row[0])
                    if (hadm_id_adm == hadm_id_diag):
                        #print(str(row[1]))
                        if (str(row[1]) == '29281' or str(row[1]) == '6930' or
                                str(row[1]) == 'E9320' or str(row[1]) == 'E9331' or str(row[1]) == 'E9352'):
                            #print('Records diagnosis: ' + str(records_diag_icd[1]))
                            rowcsv[22] = '1'
                csv.writer(csv_out).writerow(rowcsv)
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

# def map_add_labEvents_LOINC(cur):
#     # JOIN labevents & d_labitems ON itemid:

      # Select loinc_codes:
#     query_selectAll_labEvenmtsLoinc = "select hadm_id, loinc_code from mimiciiidev.;"
#     cur.execute(query_selectAll_labEvenmtsLoinc)
#     records_diag_icd = cur.fetchall()
#     query_select
#     cur.execute(query_selectAll_diagnosesicds)
#     records_diag_icd = cur.fetchall()
#     query_seelct
#     cur.execute(query_selectAll_diagnosesicds)
#     records_diag_icd = cur.fetchall()
#     try:
#         with open('classLabelsDiagnoses.csv', 'r') as csv_in, open('prescriptions.csv', 'w') as csv_out:
#             for rowcsv in csv.reader(csv_in):
#                 # Add rows!!! --> Should be done in first function append_rows() later on!!!
#                 # 22 LOINC parent groups
#                 for x in range(22):
#                     rowcsv.insert(23 + x, '0')
#                 csv.writer(csv_out).writerow(rowcsv)
#
#                 hadm_id_adm = str(rowcsv[0])
#
#
#     except (Exception, psycopg2.Error) as error:
#         print("Error while connecting to PostgreSQL", error)

# def mapping_diagnoses_icd(cur):
#     try:
#         query_selectAll_diagnosesicd = "select icd9_code from mimiciiidev.diagnoses_icd;"
#         cur.execute(query_selectAll_diagnosesicd)
#         records_diagnoses_icd = cur.fetchall()
#         for row in records_diagnoses_icd:
#             print("Diagnoses_icd: ", row[0])
#             icd9_diagnosis = row[0]
#     except (Exception, psycopg2.Error) as error:
#         print ("Error while fetching data from PostgreSQL", error)



def main():
    #### "Main"
    ## Database connection:
    try:
        # API which opens connection to a PostgreSQL database instance
        connection = psycopg2.connect(user="lenamondrejevski",
                                      password="",
                                      host="localhost",
                                      database="mimicdev")
        # Create a cursor object (allows to execute PostgreSQL command through Python source code)
        cursor = connection.cursor()
        # Print PostgreSQL version
        cursor.execute("SELECT version();")
    except (Exception, psycopg2.Error) as error:
        print ("Error while connecting to PostgreSQL", error)

    # Joining Admissions and Patients table --> .csv file:
    #write_csv_adm_JOIN_pat(cursor)
    # Append .csv file with rows (filled with 0) for procedure categories:
    #append_cols()
    # Group procedure icd9 codes into categories
    # and add category (alter 0 to 1) if procedure within category has been done during an admission:
    #map_add_procedures_icd(cursor)
    # Add class labels (0: no ADE, 1: one or more ADE(s) during admission):
    add_class_labels_diagnoses(cursor)
    # Group labevents LOINC codes into parent groups
    # and add group if event within group occured during an admission:
    #map_add_prescription_LOINC(cursor)


    ## Alter Database:
    # Procedures_ICD - Grouping of Procedures ICD9 Codes
    #mapping_procedures_icd(cursor)
    # Diagnoses_ICD - Grouping of Diagnoses ICD9 Codes
    #mapping_diagnoses_icd(cursor)



    ## Close database connection:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")




if __name__ == '__main__':
    main();