import csv
import psycopg2
from psycopg2.sql import NULL


def write_csv_adm_JOIN_pat(cur):
    query = "SELECT admissions.hadm_id, admissions.admission_type, patients.gender, patients.dob FROM mimiciiidev.admissions INNER JOIN mimiciiidev.patients ON admissions.subject_id = patients.subject_id;"
    cur.execute(query)
    records = cur.fetchall()
    with open('admissionsLEFTJOINpatients.csv', 'w', newline='') as csvfile:
        a = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_ALL, quotechar='"', doublequote=True,
                       lineterminator='\n')
        a.writerows(records)


def pre_calc_age(cur):
    query1 = "SELECT hadm_id, subject_id, admittime FROM mimiciiidev.admissions;"
    cur.execute(query1)
    records_adm = cur.fetchall()
    query2 = "SELECT DISTINCT patients.subject_id, patients.dob FROM mimiciiidev.patients;"
    cur.execute(query2)
    records_pat = cur.fetchall()
    # Create csv file with patient ids and age
    with open('age.csv', 'w') as csv_pat_age:
        for row_pat in records_pat:
            subject_id_pat = str(row_pat[0])
            # print(subject_id_pat)
            dob = row_pat[1]
            first_admission = NULL
            for row_adm in records_adm:
                subject_id_adm = str(row_adm[1])
                if (subject_id_pat == subject_id_adm):
                    adm_time = row_adm[2]
                    if (first_admission == NULL):
                        first_admission = adm_time
                    elif (adm_time < first_admission):
                        first_admission = adm_time
            age_diff = (first_admission - dob)
            age = int(round(age_diff.days / 365.242, 2))
            csv.writer(csv_pat_age).writerow((subject_id_pat, age))
    return records_adm

def calc_age(rowcsv, records_adm):
    # Age = dob - day of first admission
    hadm_id_in = str(rowcsv[0])
    for rows_adm in records_adm:
        hadm_id_adm = str(rows_adm[0])
        if (hadm_id_in == hadm_id_adm):
            subject_id_adm = str(rows_adm[1])
            with open('age.csv', 'r') as csv_age:
                for rows_age in csv_age:
                    subject_id_age = str(rows_age.split(',')[0])
                    if (subject_id_adm == subject_id_age):
                        pat_age = str(rows_age.split(',')[1])
                        rowcsv[3] = pat_age


def append_cols(row):
    # 18 Procedures, 1 class label
    # 37 LOINC parent groups
    # 93 medication groups
    for x in range(149):
        row.insert(4 + x, '0')


def pre_map_add_procedures_icd(cur):
    # insert_pro_categories
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

    query_selectAll_procedureicds = "select hadm_id, icd9_code from mimiciiidev.procedures_icd;"
    cur.execute(query_selectAll_procedureicds)
    records_prcd_icd = cur.fetchall()
    return records_prcd_icd, dict_icd9_cat

def map_add_procedures_icd(records_prcd_icd, dict_icd9_cat, rowcsv):
    hadm_id_adm = str(rowcsv[0])
    for row in records_prcd_icd:
        hadm_id_pro = str(row[0])
        # Step 1: Extract first two chars of ICD9 Code (01-99)
        icd9_key = row[1][0:2]
        # Step 2: Alter ICD9 Code to Category (0-16)
        icd9_val_cat = dict_icd9_cat[icd9_key]
        if hadm_id_adm == hadm_id_pro:
            if (icd9_val_cat == '0' or icd9_val_cat == '1' or icd9_val_cat == '2' or icd9_val_cat == '3'):
                rowcsv[4 + int(icd9_val_cat)] = '1'
            elif (icd9_val_cat == '3A'):
                rowcsv[8] = '1'
            else:
                rowcsv[5 + int(icd9_val_cat)] = '1'


def pre_add_class_labels_diagnoses(cur):
    query_selectAll_diagnosesIcds = "select hadm_id, icd9_code from mimiciiidev.diagnoses_icd;"
    cur.execute(query_selectAll_diagnosesIcds)
    records_diag_icd = cur.fetchall()
    return records_diag_icd

def add_class_labels_diagnoses(records_diag_icd, rowcsv):
    # -for each admission, check if diagnosis icd code related to ADE occured
    hadm_id_adm = str(rowcsv[0])
    #print(hadm_id_adm)
    for row in records_diag_icd:
        hadm_id_diag = str(row[0])
        if (hadm_id_adm == hadm_id_diag):
            #print(str(row[1]))
            if (str(row[1]) == '9308' or str(row[1]) == '9300' or
                    str(row[1]) == '9320' or str(row[1]) == '9331' or str(row[1]) == '9305'
                    or str(row[1]) == '9342' or str(row[1]) == '9305'):
                #print('Records diagnosis: ' + str(records_diag_icd[1]))
                rowcsv[22] = '1'


def pre_map_add_labEvents_LOINC(cur):
    dict_icd9_cat = {'LG100-4': '0', 'LG103-8': '1', 'LG105-3': '2', 'LG106-1': '3', 'LG27-5': '4', 'LG41751-5': '5',
                     'LG41762-2': '6', 'LG41808-3': '7', 'LG41809-1': '8', 'LG41811-7': '9', 'LG41812-5': '10',
                     'LG41813-3': '11', 'LG41814-1': '12', 'LG41816-6': '13', 'LG41817-4': '14', 'LG41818-2': '15',
                     'LG41820-8': '16', 'LG41821-6': '17', 'LG41822-4': '18', 'LG41855-4': '19', 'LG47-3': '20',
                     'LG50067-4': '21', 'LG55-6': '22', 'LG66-3': '23', 'LG68-9': '24', 'LG70-5': '25', 'LG74-7': '26',
                     'LG78-8': '27', 'LG80-4': '28', 'LG85-3': '29', 'LG88-7': '30', 'LG89-5': '31', 'LG90-3': '32',
                     'LG92-9': '33', 'LG96-0': '34', 'LG97-8': '35', 'LG99-4': '36'}

    query = "SELECT  labevents.hadm_id, group_loinc.parentGroupId  \
                FROM mimiciiidev.labevents INNER JOIN mimiciiidev.d_labitems ON labevents.itemid = d_labitems.itemid \
                INNER JOIN mimiciiidev.group_loinc_terms ON group_loinc_terms.loincnumber = d_labitems.loinc_code \
                INNER JOIN mimiciiidev.group_loinc ON group_loinc.groupId =  group_loinc_terms.groupId;"
    cur.execute(query)
    records = cur.fetchall()

    return records, dict_icd9_cat

def map_add_labEvents_LOINC(records, dict_icd9_cat, rowcsv):
    hadm_id = str(rowcsv[0])
    for rowlabEv in records:
        hadm_id_labEv = str(rowlabEv[0])
        if (hadm_id == hadm_id_labEv):
            parent_group = str(rowlabEv[1])
            rowcsv[23 + int(dict_icd9_cat[parent_group])] = '1'


def pre_map_prescriptions_ATC(cur):
    dict_atc2_cat = {'A01': '0', 'A02': '1', 'A03': '2', 'A04': '3', 'A05': '4', 'A06': '5', 'A07': '6', 'A08': '7',
                     'A09': '8', 'A10': '9', 'A11': '10', 'A12': '11', 'A13': '12', 'A14': '13', 'A15': '14',
                     'A16': '15',
                     'B01': '16', 'B02': '17', 'B03': '18', 'B05': '19', 'B06': '20',
                     'C01': '21', 'C02': '22', 'C03': '23', 'C04': '24', 'C05': '25', 'C07': '26', 'C08': '27',
                     'C09': '28', 'C10': '29',
                     'D01': '30', 'D02': '31', 'D03': '32', 'D04': '33', 'D05': '34', 'D06': '35', 'D07': '36',
                     'D08': '37', 'D09': '38', 'D10': '39', 'D11': '40',
                     'G01': '41', 'G02': '42', 'G03': '43', 'G04': '44',
                     'H01': '45', 'H02': '46', 'H03': '47', 'H04': '48', 'H05': '49',
                     'J01': '50', 'J02': '51', 'J04': '52', 'J05': '53', 'J06': '54', 'J07': '55',
                     'L01': '56', 'L02': '57', 'L03': '58', 'L04': '59',
                     'M01': '60', 'M02': '61', 'M03': '62', 'M04': '63', 'M05': '64', 'M09': '65',
                     'N01': '66', 'N02': '67', 'N03': '68', 'N04': '69', 'N05': '70', 'N06': '71', 'N07': '72',
                     'P01': '73', 'P02': '74', 'P03': '75',
                     'R01': '76', 'R02': '77', 'R03': '78', 'R05': '79', 'R06': '80', 'R07': '81',
                     'S01': '82', 'S02': '83', 'S03': '84',
                     'V01': '85', 'V03': '86', 'V04': '87', 'V06': '88', 'V07': '89', 'V08': '90', 'V09': '91',
                     'V10': '92', 'V20': '93'
                     }
    return dict_atc2_cat


def map_prescriptions_ATC(rowcsv, dict_atc2_cat):
    # 93 medication groups (ATC)
    hadm_id_adm = str(rowcsv[0])
    # open csv file with hamdid codes and matching atc4 codes
    with open('hamd_ndc_atc3.csv', 'r') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        for row in csv_reader:
            hadm_id_med = str(row[1])
            if (hadm_id_adm == hadm_id_med):
                if (row[5] != 'NA'):
                    # Step 1: Extract first two chars of ICD9 Code (01-99)
                    atc2_key = str(row[5][0:3])
                    # Step 2: Alter ICD9 Code to Category (0-16)
                    atc2_cat = str(dict_atc2_cat[atc2_key])
                    rowcsv[60 + int(atc2_cat)] = '1'



def main():
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

    try:
        ## Pre steps; Querying, dictionaries
        write_csv_adm_JOIN_pat(cursor)

        records_adm = pre_calc_age(cursor)

        records_prcd_icd = pre_map_add_procedures_icd(cursor)[0]
        dict_icd9_cat = pre_map_add_procedures_icd(cursor)[1]

        records_diag_icd = pre_add_class_labels_diagnoses(cursor)

        records_loinc_groups = pre_map_add_labEvents_LOINC(cursor)[0]
        dict_icd9_cat_2 = pre_map_add_labEvents_LOINC(cursor)[1]

        dict_atc2_cat = pre_map_prescriptions_ATC(cursor)

        with open('admissionsLEFTJOINpatients.csv', 'r') as csv_in, open('preProcessed.csv', 'w') as csv_out:
            #Joining Admissions and Patients table --> .csv file:
            i = 0
            for row in csv.reader(csv_in):
                # Set the age (in field of dob)
                calc_age(row, records_adm)
                # Append .csv file with rows (filled with 0) for procedure categories:
                append_cols(row)
                # Group procedure icd9 codes into categories
                # and add category (alter 0 to 1) if procedure within category has been done during an admission:
                map_add_procedures_icd(records_prcd_icd, dict_icd9_cat, row)
                # Add class labels (0: no ADE, 1: one or more ADE(s) during admission):
                add_class_labels_diagnoses(records_diag_icd, row)
                # Group labevents LOINC codes into parent groups
                # and add group if event within group occured during an admission:
                map_add_labEvents_LOINC(records_loinc_groups, dict_icd9_cat_2, row)
                # Group Medication in Prescriptions to ATC
                map_prescriptions_ATC(row, dict_atc2_cat)
                # Write rows
                csv.writer(csv_out).writerow(row)
                i = i + 1
                print(i)

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    ## Close database connection:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


if __name__ == '__main__':
    main();