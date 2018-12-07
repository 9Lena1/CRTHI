import csv
import datetime

import psycopg2
from psycopg2.sql import NULL


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

#TODO
def calc_age(cur):
    # Age = dob - day of first admission
    try:
        query1 = "SELECT hadm_id, subject_id, admittime FROM mimiciiidev.admissions;"
        cur.execute(query1)
        records_adm = cur.fetchall()
        query2 = "SELECT DISTINCT patients.subject_id, patients.dob FROM mimiciiidev.patients;"
        cur.execute(query2)
        records_pat = cur.fetchall()
        #Create csv file with patient ids and age
        # with open('age.csv', 'w') as csv_pat_age:
        #     for row_pat in records_pat:
        #         subject_id_pat = str(row_pat[0])
        #         #print(subject_id_pat)
        #         dob = row_pat[1]
        #         first_admission = NULL
        #         for row_adm in records_adm:
        #             print(row_adm)
        #             subject_id_adm = str(row_adm[1])
        #             if (subject_id_pat == subject_id_adm):
        #                 adm_time = row_adm[2]
        #                 if (first_admission == NULL):
        #                     first_admission = adm_time
        #                 elif (adm_time < first_admission):
        #                     first_admission = adm_time
        #                #print(adm_time)
        #         #print(first_admission)
        #         age_diff = (first_admission - dob)
        #         age = int(age_diff.days/356)
        #         csv.writer(csv_pat_age).writerow((subject_id_pat, age))
        #print("entering section 2")
        # Write age
        with open('admissionsLEFTJOINpatients.csv', 'r') as csv_in, open('age.csv', 'r') as csv_age, open('patientAge.csv', 'w') as csv_out:
            i = 0
            for rowcsv in csv.reader(csv_in):
                hadm_id_in = str(rowcsv[0])
                #print('In: ' + hadm_id_in)
                for rows_adm in records_adm:
                    hadm_id_adm = str(rows_adm[0])

                    if (hadm_id_in == hadm_id_adm):
                    #     print("Comparing shit")
                    #     print(hadm_id_in)
                    #     print(hadm_id_adm)
                    #     print("Printing row in IF statement")
                    #     print(rows_adm)
                        print('Hadm_id_Adm: ' + hadm_id_adm)
                        subject_id_adm = str(rows_adm[1])

                        print('Subject_id_adm: ' + subject_id_adm)
                        for rows_age in csv_age:
                            print("rows_age: " + rows_age)
                            #TODO split when comma subject_id_age and pat_age
                            subject_id_age = str(rows_age.split(',')[0])
                            print("subject_id_age: " + rows_age.split(',')[0])
                            if (subject_id_adm == subject_id_age):
                                print('Id_Age: ' + subject_id_age)
                                pat_age = str(rows_age.split(',')[1])
                                print(rows_age.split(',')[1])
                                print(pat_age)
                                rowcsv[3] = pat_age
                            print('hi')
                csv.writer(csv_out).writerow(rowcsv)
                i = i + 1
                #print(i)


        # query = "SELECT p.subject_id, p.dob, p.gender, MIN(a.admittime) AS first_admittime, \
        #         MIN(ROUND((cast(admittime as date) - cast(dob as date)) / 365.242, 2) ) AS first_admit_age \
        #         FROM mimiciiidev.patients p INNER JOIN mimiciiidev.admissions a ON p.subject_id = a.subject_id \
        #         GROUP BY p.subject_id, p.dob, p.gender ORDER BY p.subject_id;"
        # cur.execute(query)
        # records_pat_age = cur.fetchall()
        #
        # query1 = "SELECT hadm_id, subject_id FROM mimiciiidev.admissions "
        # cur.execute(query1)
        # records_adm = cur.fetchall()
        #
        # with open('admissionsLEFTJOINpatients.csv', 'r') as csv_in, open('patientAge.csv', 'w') as csv_out:
        #     for rowcsv in csv.reader(csv_in):
        #         hadm_id_in = rowcsv[0]
        #         for rows_adm in records_adm:
        #             hadm_id = records_adm[0]
        #             subject_id_adm = NULL
        #             if (hadm_id_in == hadm_id):
        #                 subject_id_adm = records_adm[1]
        #             for rows_age in records_pat_age:
        #                 subject_id_pat = rows_age[0]
        #                 if (subject_id_adm == subject_id_pat):
        #                     pat_age = rows_age[4]
        #                     rowcsv[3] = pat_age
        #                     print(pat_age)
        #                     return
        #         csv.writer(csv_out).writerow(rowcsv)

    except (Exception, psycopg2.Error) as error:
        print ("Error while fetching data from PostgreSQL", error)

def append_cols():
    with open('admissionsLEFTJOINpatients.csv', 'r') as csv_in, open('appendedCols.csv', 'w') as csv_out:
        for row in csv.reader(csv_in):
            # 18 Procedures, 1 class label
            # 37 LOINC parent groups
            # 93 medication groups
            for x in range(139):
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
                #print("Hadm_id_admissions: ", hadm_id_adm)

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
                        if (str(row[1]) == 'E9308' or str(row[1]) == 'E9300' or
                                str(row[1]) == 'E9320' or str(row[1]) == 'E9331' or str(row[1]) == 'E9305'
                                or str(row[1]) == 'E9342'or str(row[1]) == 'E9305'):
                            #print('Records diagnosis: ' + str(records_diag_icd[1]))
                            rowcsv[22] = '1'
                csv.writer(csv_out).writerow(rowcsv)
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def map_add_labEvents_LOINC(cur):
    dict_icd9_cat = {'LG100-4': '0', 'LG103-8': '1', 'LG105-3': '2', 'LG106-1': '3', 'LG27-5': '4', 'LG41751-5': '5',
                     'LG41762-2': '6', 'LG41808-3': '7', 'LG41809-1': '8', 'LG41811-7': '9', 'LG41812-5': '10',
                     'LG41813-3': '11', 'LG41814-1': '12', 'LG41816-6': '13', 'LG41817-4': '14', 'LG41818-2': '15',
                     'LG41820-8': '16', 'LG41821-6': '17', 'LG41822-4': '18', 'LG41855-4': '19', 'LG47-3': '20',
                     'LG50067-4': '21', 'LG55-6': '22', 'LG66-3': '23', 'LG68-9': '24', 'LG70-5': '25', 'LG74-7': '26',
                     'LG78-8': '27', 'LG80-4': '28', 'LG85-3': '29', 'LG88-7': '30', 'LG89-5': '31', 'LG90-3': '32',
                     'LG92-9': '33', 'LG96-0': '34', 'LG97-8': '35', 'LG99-4': '36'}
    try:
        query = "SELECT  labevents.hadm_id, group_loinc.parentGroupId  \
                    FROM mimiciiidev.labevents INNER JOIN mimiciiidev.d_labitems ON labevents.itemid = d_labitems.itemid \
                    INNER JOIN mimiciiidev.group_loinc_terms ON group_loinc_terms.loincnumber = d_labitems.loinc_code \
                    INNER JOIN mimiciiidev.group_loinc ON group_loinc.groupId =  group_loinc_terms.groupId;"
        cur.execute(query)
        records = cur.fetchall()
        with open('classLabelsDiagnoses.csv', 'r') as csv_in, open('labEvents.csv', 'w') as csv_out:
            i = 0
            for rowcsv in csv.reader(csv_in):
                # Add rows!!! --> Should be done in first function append_rows() later on!!!
                # 37 LOINC parent groups
                for x in range(37):
                    rowcsv.insert(23 + x, '0')
                hadm_id = str(rowcsv[0])
                for rowlabEv in records:
                    hadm_id_labEv = str(rowlabEv[0])
                    if (hadm_id == hadm_id_labEv):
                        parent_group = str(rowlabEv[1])
                        rowcsv[23 + int(dict_icd9_cat[parent_group])] = '1'
                csv.writer(csv_out).writerow(rowcsv)
                i = i + 1
                print(i)
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

#TODO
# 93 medication groups (ATC)
def mapping_prescriptions_ATC(cur):
    x=1

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
    #add_class_labels_diagnoses(cursor)
    # Group labevents LOINC codes into parent groups
    # and add group if event within group occured during an admission:
    calc_age(cursor)
    #map_add_labEvents_LOINC(cursor)
    #mapping_prescriptions_ATC(cursor)

    ## Close database connection:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")




if __name__ == '__main__':
    main();