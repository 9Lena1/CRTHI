# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 14:53:34 2018

@author: Corinne
"""


import csv
import psycopg2


def select_hamd_id(cur):
        array_selected_hadm_id = []
        query = "SELECT hadm_id, icd9_code FROM diagnoses_icd WHERE icd9_code = '2930' or icd9_code = '29281';"
        cur.execute(query)
        records= cur.fetchall()
        for row in records:
                    hadm_id = str(row[0])
                    if (hadm_id not in array_selected_hadm_id):
                        array_selected_hadm_id.append(str(hadm_id))
                        
        return array_selected_hadm_id


def write_hadmid_admissiontype_gender_calc_age(cur, array_selected_hadm_id):
    # query all admission, type, gender, dob, first admissiontime, discharge time and admissiontime
    cur.execute(
        "SELECT  a.hadm_id, a.admission_type, p.gender, p.dob, MIN (a.admittime) OVER (PARTITION BY p.subject_id) AS first_admittime, a.dischtime, a.admittime  FROM admissions a INNER JOIN patients p ON p.subject_id = a.subject_id ORDER BY a.hadm_id;")
    records_adm = cur.fetchall()
    # write file with admission Id, type, gender and age
    with open('delirium_preprocessing_intermediate1.csv', 'w', newline='') as csvfile:
        filewriter = csv.writer(csvfile)
        #create dictionary to save row number of every hadm_id
        dict_selected_hadm_id={}
        i=0
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
                dischtime = row[5]
                admittime = row[6]
                #calculate length of hospital stay in days
                hospital_stay = (dischtime - admittime)
                hospital_stay = hospital_stay.days
                #add hadm_id and row number to dictionary
                dict_selected_hadm_id.update([(str(row[0]), int(i))])
                i=i+1
                filewriter.writerow([row[0], row[1], row[2], age, hospital_stay])
   
    return  dict_selected_hadm_id


def append_cols(row):
    # 18 Procedures, 1 class label
    # 8 LOINC parent groups
    # 2 Loinc groups
    # 94 medication groups
    # 1 kidney failure
    for x in range(124):
        # 5 rows from previous query 
        row.insert(5 + x, '0')


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
    query_selectAll_procedureicds = "SELECT hadm_id, icd9_code FROM procedures_icd;"
    cur.execute(query_selectAll_procedureicds)
    records_prcd_icd = cur.fetchall()
    return records_prcd_icd, dict_icd9_cat


def pre_add_class_labels_diagnoses(cur):
    # query admission IDs with ADEs diagnosed
    query_selectAll_diagnosesIcds = "SELECT hadm_id, icd9_code FROM diagnoses_icd WHERE icd9_code = '29281';"
    cur.execute(query_selectAll_diagnosesIcds)
    records_diag_icd = cur.fetchall()
    return records_diag_icd


def pre_map_add_labEvents_LOINC(cur):
    dict_LOINC_cat = {'LG100-4': '0', 'LG103-8': '1',  'LG27-5' : '2', 'LG55-6': '3', 'LG74-7': '4',
                      'LG78-8': '5', 'LG80-4': '6', 'LG97-8': '7', }

    #query all parent group id for hadm where the result of the labevents was flagged as abnormal
    query = "SELECT  labevents.hadm_id, group_loinc.parentGroupId  \
                FROM labevents INNER JOIN d_labitems ON labevents.itemid = d_labitems.itemid \
                INNER JOIN group_loinc_terms ON group_loinc_terms.loincnumber = d_labitems.loinc_code \
                INNER JOIN group_loinc ON group_loinc.groupId =  group_loinc_terms.groupId WHERE labevents.flag = 'abnormal';"
    cur.execute(query)
    records = cur.fetchall()

    return records, dict_LOINC_cat

def map_alb_and_platelets (cur):
    query = "SELECT  labevents.hadm_id, group_loinc_terms.GroupId  \
                FROM labevents INNER JOIN d_labitems ON labevents.itemid = d_labitems.itemid \
                INNER JOIN group_loinc_terms ON group_loinc_terms.loincnumber = d_labitems.loinc_code \
                WHERE labevents.flag = 'abnormal' AND group_loinc_terms.GroupId = 'LG5465-2' OR group_loinc_terms.GroupId = 'LG49829-1' OR group_loinc_terms.GroupId = 'LG32892-8'; "
    cur.execute(query)
    records = cur.fetchall()
   
    dict_LOINC_group= { 'LG5465-2': '0', 'LG49829-1': '0', 
                       'LG32892-8': '1' 
                       }

    return records, dict_LOINC_group

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

def diagnosis_kidney_failure(cur):
    query= " SELECT hadm_id, icd9_code FROM diagnoses_icd WHERE icd9_code LIKE '58%';"
    cur.execute(query)
    kidney_failure_records= cur.fetchall()
    return kidney_failure_records


def main():
    ## Database connection:
    try:
        # API which opens connection to a PostgreSQL database instance
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="localhost",
                                      database="mimic")
        # Create a cursor object (allows to execute PostgreSQL command through Python source code)
        cursor = connection.cursor()
        # Print PostgreSQL version
        cursor.execute("set search_path to mimiciii;")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    try:
        ## Pre steps; Querying, dictionaries
        ##TODO Comment in when run for first time!!!
        
       # 
        #only has to be run once to create csv file with array
        array_selected_hadm_id = select_hamd_id(cursor)
        
        #run this if you need to rerun but already have the csv file with array hadm id
        #array_selected_hadm_id = []
        #with open('selected_hamd.csv', 'r') as  csvfile:
         #   csv_reader = csv.reader(csvfile, delimiter=',')
          #  for row in csv_reader:
           #     array_selected_hadm_id.append(str(row[0]))
        
        dict_selected_hadm_id = write_hadmid_admissiontype_gender_calc_age(cursor, array_selected_hadm_id)

        records_diag_icd = pre_add_class_labels_diagnoses(cursor)

        records_prcd_icd = pre_map_add_procedures_icd(cursor)[0]
        dict_icd9_cat = pre_map_add_procedures_icd(cursor)[1]

        records_loinc_groups = pre_map_add_labEvents_LOINC(cursor)[0]
        dict_icd9_cat_2 = pre_map_add_labEvents_LOINC(cursor)[1]

        records_loinc_alb_plat= map_alb_and_platelets (cursor)[0]
        dict_loinc_groups = map_alb_and_platelets(cursor)[1]

        dict_atc2_cat = pre_map_prescriptions_ATC(cursor)
    
        kidney_failure_records = diagnosis_kidney_failure(cursor)
        
        with open('delirium_preprocessing_intermediate1.csv', 'r') as csv_in, open('delirium_preprocessing_intermediate2.csv', 'w', newline='') as csv_out:
            i = 0
            for row in csv.reader(csv_in):
                # Append .csv file with rows:
                append_cols(row)
                csv.writer(csv_out).writerow(row)
                
        #add lab events to table
        with open('delirium_preprocessing_intermediate2.csv', 'r') as csv_in, open('delirium_preprocessing.csv', 'w', newline='') as csv_out:       
            csv_reader = csv.reader(csv_in, delimiter=',')
            lines = list(csv_reader)  
            i=0
            
            
             # Go through query for class labels    
            for row in records_diag_icd:
                #check if hadm id is in array of selected hadm ids
                if str(row[0]) in array_selected_hadm_id:
                    # make corresponding row, column positive
                    lines[dict_selected_hadm_id[str(row[0])]][23]= '1'
                    i=i+1
                    print("label"+ str(i))
            
            for row in records_loinc_alb_plat:
                if str(row[0]) in array_selected_hadm_id:
                    # make corresponding row, column positive
                    lines[dict_selected_hadm_id[str(row[0])]][127+int(dict_loinc_groups[str(row[1])])]= int(lines[dict_selected_hadm_id[str(row[0])]][127+int(dict_loinc_groups[str(row[1])])]) +1
                    i=i+1
                    print("loinc" + str(i))
            
            
            #go through lab events query
            for row in records_loinc_groups:
                #check if hadm id is in array of selected hadm ids
                if str(row[0]) in array_selected_hadm_id:
                    parent_group = str(row[1])
                    lines[dict_selected_hadm_id[str(row[0])]][24 + int(dict_icd9_cat_2[parent_group])] = int(lines[dict_selected_hadm_id[str(row[0])]][24 + int(dict_icd9_cat_2[parent_group])]) +1
                    print("loinc2" + str(i))
                    i=i+1
                    
        #add procedures, class labels, medications, kidney failure
            
            #Go through query for procedures
            for row in records_prcd_icd:
                # Extract first two chars of ICD9 Code (01-99)
                icd9_key = row[1][0:2]
                # Alter ICD9 Code to Category (0-16)
                icd9_val_cat = dict_icd9_cat[str(icd9_key)]
                #check if hadm id is in array of selected hadm ids
                if str(row[0]) in array_selected_hadm_id:
                    # add one to corresponding row, column
                    if (icd9_val_cat == '0' or icd9_val_cat == '1' or icd9_val_cat == '2' or icd9_val_cat == '3'):
                        lines[dict_selected_hadm_id[str(row[0])]][5 + int(icd9_val_cat)] = int(lines[dict_selected_hadm_id[str(row[0])]][5 + int(icd9_val_cat)]) + 1
                    elif (icd9_val_cat == '3A'):
                        lines[dict_selected_hadm_id[str(row[0])]][9] = int(lines[dict_selected_hadm_id[str(row[0])]][9]) +1
                    else:
                        lines[dict_selected_hadm_id[str(row[0])]][6 + int(icd9_val_cat)] = int(lines[dict_selected_hadm_id[str(row[0])]][6 + int(icd9_val_cat)]) +1
               
                    i=i+1
                    print("Proc" + str(i))
              
           
                
           
            
            with open ('hamd_ndc_atc3.csv' , 'r', newline = '') as csv_atc:
                csv_atc_reader = csv.reader(csv_atc, delimiter=',')
                # go through hadm_atc file, already shortened, so all hadm are in selected
                for row in csv_atc_reader:
                    if str(row[5]) != 'NA':
                        if str(row[1]) in array_selected_hadm_id:
                    #Step 1: select first three chars for atc2 code
                            atc2_key = str(row[5][0:3])
                    # Step 2: Map to atc group dictionary
                            atc2_cat = str(dict_atc2_cat[atc2_key])
                    # add one to corresponding row, column
                            lines[dict_selected_hadm_id[str(row[1])]][32 + int(atc2_cat)] = int(lines[dict_selected_hadm_id[row[1]]][32 + int(atc2_cat)]) + 1
                            print ("med" + str(i))
                            i= i + 1
                    
            for row in kidney_failure_records:
                #check if hadm id is in array of selected hadm ids
                if str(row[0]) in array_selected_hadm_id:
                    # make corresponding row, column positive
                    lines[dict_selected_hadm_id[str(row[0])]][126]= '1'
                    i=i+1
                    print("kidney" + str(i))
            
            
                
            
            filewriter = csv.writer(csv_out)
            filewriter.writerows(lines)
    
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    ## Close database connection:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


if __name__ == '__main__':
    main();