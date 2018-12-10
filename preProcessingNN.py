# -*- coding: utf-8 -*-
import csv
import psycopg2


def select_hamd_id():
    with open('hamd_ndc_atc3.csv', 'r') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        array_selected_hadm_id = []
        for row in csv_reader:
            if (row[5] != 'NA'):
                # Step 1: extract first 3 chars of the ATC4 code
                atc2_key = str(row[5][0:3])
                if (atc2_key == 'B01'
                        or atc2_key == 'L01'
                        or atc2_key == 'L04'
                        or atc2_key == 'H02'
                        or atc2_key == 'J01'):
                    hadm_id = row[1]
                    # print (hadm_id)
                    if (hadm_id not in array_selected_hadm_id):
                        array_selected_hadm_id.append(str(hadm_id))
    return array_selected_hadm_id


def write_hadmid_admissiontype_gender_calc_age(cur, array_selected_hadm_id):
    # query all admission, type, gender, dob and admissiontime
    cur.execute(
        "SELECT  a.hadm_id, a.admission_type, p.gender, p.dob, MIN (a.admittime) OVER (PARTITION BY p.subject_id) AS first_admittime  FROM mimiciiidev.admissions a INNER JOIN mimiciiidev.patients p ON p.subject_id = a.subject_id ORDER BY a.hadm_id;")
    records_adm = cur.fetchall()
    # write file with admission Id, type, gender and age
    with open('admissionsLEFTJOINpatients.csv', 'w', newline='') as csvfile:
        filewriter = csv.writer(csvfile)
        for row in records_adm:
            # only write rows with selected admission IDs
            if str(row[0]) in array_selected_hadm_id:
                first_admission = row[4]
                dob = row[3]
                # calculate age
                age_diff = (first_admission - dob)
                age = int(round(age_diff.days / 365.242, 2))
                # set all 90+ ages to 90
                if (age == 300):
                    age = 90
                # TODO Change: include all admission types (+3) and male/female (+1 row)
                filewriter.writerow([row[0], row[1], row[2], age])


def shorten_atc_file(array_selected_hadm_id):
    with open('hamd_ndc_atc3.csv', 'r') as csvfile, open('hamd_atc_new.csv', 'w', newline='') as csvout:
        csv_reader = csv.reader(csvfile, delimiter=',')
        filewriter = csv.writer(csvout)
        # remove rows from atc file that are not selected admission IDs
        for row in csv_reader:
            # print(i)
            # skip non-defined atc codes
            if str(row[5]) != 'NA':
                if (str(row[1]) in array_selected_hadm_id):
                    filewriter.writerow([row[1], row[5]])


def append_cols(row):
    # 18 Procedures, 1 class label
    # 37 LOINC parent groups
    # 93 medication groups
    for x in range(149):
        ## TODO Change index to 8 (+4 rows; binary) ##
        row.insert(8 + x, '0')

# TODO Binarization
def binarisation(rowcsv):
    admission_type = str(rowcsv[1])
    gender = str(rowcsv[2])
    age = str(rowcsv[3])
    if (admission_type == 'URGENT'):
        rowcsv[1] = '1'
        rowcsv[2] = '0'
        rowcsv[3] = '0'
        rowcsv[4] = '0'
    elif (admission_type == 'NEWBORN'):
        rowcsv[1] = '0'
        rowcsv[2] = '1'
        rowcsv[3] = '0'
        rowcsv[4] = '0'
    elif (admission_type == 'ELECTIVE'):
        rowcsv[1] = '0'
        rowcsv[2] = '0'
        rowcsv[3] = '1'
        rowcsv[4] = '0'
    elif (admission_type == 'EMERGENCY'):
        rowcsv[1] = '0'
        rowcsv[2] = '0'
        rowcsv[3] = '0'
        rowcsv[4] = '1'

    if (gender == 'male'):
        rowcsv[5] = '1'
        rowcsv[6] = '0'
    elif (gender == 'female'):
        rowcsv[5] = '0'
        rowcsv[6] = '1'
    rowcsv[7] = age

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

    # query admission IDs and icd9 procedure cods
    query_selectAll_procedureicds = "SELECT hadm_id, icd9_code FROM mimiciiidev.procedures_icd;"
    cur.execute(query_selectAll_procedureicds)
    records_prcd_icd = cur.fetchall()
    # only keep rows from selected admission IDs
    # for row in records_prcd_icd:
    #   if str(row[0]) not in array_selected_hadm_id:
    #       print ("deleted" + str(row[0]) )
    #      del row
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
                ## TODO Change index --> 8 ##
                rowcsv[8 + int(icd9_val_cat)] = '1'
            elif (icd9_val_cat == '3A'):
                ## TODO Change index --> 12 ##
                rowcsv[12] = '1'
            else:
                ## TODO Change index --> 9 ##
                rowcsv[9 + int(icd9_val_cat)] = '1'


def pre_add_class_labels_diagnoses(cur):
    # query admission IDs with ADEs diagnosed
    query_selectAll_diagnosesIcds = "SELECT hadm_id, icd9_code FROM mimiciiidev.diagnoses_icd WHERE icd9_code = 'E9308' or icd9_code = 'E9300' or icd9_code= 'E9320' or icd9_code= 'E9331' or icd9_code= 'E9305' or icd9_code= 'E9342' or icd9_code= 'E9305';"
    cur.execute(query_selectAll_diagnosesIcds)
    records_diag_icd = cur.fetchall()
    return records_diag_icd


def add_class_labels_diagnoses(records_diag_icd, rowcsv):
    hadm_id_adm = str(rowcsv[0])
    # print(hadm_id_adm)
    for row in records_diag_icd:
        hadm_id_diag = str(row[0])
        if (hadm_id_adm == hadm_id_diag):
            # print(row[0])
            # print('Records diagnosis: ' + str(records_diag_icd[1]))
            ## TODO Change index --> 26 ##
            rowcsv[26] = '1'


def pre_map_add_labEvents_LOINC(cur):
    dict_LOINC_cat = {'LG100-4': '0', 'LG103-8': '1', 'LG105-3': '2', 'LG106-1': '3', 'LG27-5': '4', 'LG41751-5': '5',
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

    return records, dict_LOINC_cat


def map_add_labEvents_LOINC(records, dict_icd9_cat, rowcsv):
    hadm_id = str(rowcsv[0])
    for rowlabEv in records:
        hadm_id_labEv = str(rowlabEv[0])
        if (hadm_id == hadm_id_labEv):
            parent_group = str(rowlabEv[1])
            ## TODO Change index --> 27 ##
            rowcsv[27 + int(dict_icd9_cat[parent_group])] = '1'


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
    hadm_id_adm = str(rowcsv[0])
    # open csv file with hamdid codes and matching atc4 codes
    with open('hamd_atc_new.csv', 'r') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        for row in csv_reader:
            hadm_id_med = str(row[0])
            # check if admission IDs match
            if (hadm_id_adm == hadm_id_med):
                # Step 1: Extract first 3 chars of atc code
                atc2_key = str(row[1][0:3])
                # Step 2: Map to atc group dictionary
                atc2_cat = str(dict_atc2_cat[atc2_key])
                ## TODO Change: Change index --> 64 ##
                rowcsv[64 + int(atc2_cat)] = '1'


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
        print("Error while connecting to PostgreSQL", error)

    try:
        ## Pre steps; Querying, dictionaries
        ##TODO Comment in when run for first time!!!
        array_selected_hadm_id = select_hamd_id()
        write_hadmid_admissiontype_gender_calc_age(cursor, array_selected_hadm_id)
        shorten_atc_file(array_selected_hadm_id)

        records_diag_icd = pre_add_class_labels_diagnoses(cursor)

        records_prcd_icd = pre_map_add_procedures_icd(cursor)[0]
        dict_icd9_cat = pre_map_add_procedures_icd(cursor)[1]

        records_loinc_groups = pre_map_add_labEvents_LOINC(cursor)[0]
        dict_icd9_cat_2 = pre_map_add_labEvents_LOINC(cursor)[1]

        dict_atc2_cat = pre_map_prescriptions_ATC(cursor)

        with open('admissionsLEFTJOINpatients.csv', 'r') as csv_in, open('preProcessedNN.csv', 'w',
                                                                         newline='') as csv_out:
            i = 0
            for row in csv.reader(csv_in):
                # Append .csv file with rows (filled with 0) for procedure categories:
                append_cols(row)
                ## TODO Binarization ##
                # Binarise admission_type and gender for NN
                binarisation(row)
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