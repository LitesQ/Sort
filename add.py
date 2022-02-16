# -*- coding: utf-8 -*-

import psycopg2
from psycopg2 import Error

try:
    # Подключение к существующей базе данных
    connection = psycopg2.connect(user="", password="", host="", port="", database="")

    # Курсор для выполнения операций с базой данных
    cursor = connection.cursor()

    query = "SELECT MAX(id) FROM civilians"
    cursor.execute(query)
    incriment = cursor.fetchone()[0]+1

    # Выполнение SQL-запроса
    query = "SELECT * FROM reg"
    cursor.execute(query)

    # Массив с данными 
    result = cursor.fetchall()

    count = 0

    for i in range(len(result)):
        #Разделение ФИО
        fio = result[i][1].split()
        last_name = fio[0].strip()
        first_name = fio[1].strip()
        patronymic = "-"
        if len(fio) > 2:
            patronymic = fio[2].strip()
        if len(fio) == 4 and fio[3]!= "-":   
            patronymic += " " + fio[3].strip()

        #Получение даты рождения
        birth_date = str(result[i][2]).replace(" ","")

        #Получение места рождения
        birth_region_var = result[i][3].split()
        birth_region = ""
        for row in range(len(birth_region_var)):
            birth_region += birth_region_var[row] + " "
        birth_region = birth_region.strip()

        #Получение пола
        sex = str(result[i][4]).replace(" ","")
        if sex == "муж":
            sex = "М"
        elif sex == "жен":
            sex = "Ж"
        
        #Получение гражданства
        citizenship = str(result[i][5]).strip()

        #Получение адреса
        adress_full = str(result[i][6]).strip()
        street = adress_full.split(".")[0] + "."
        home = adress_full[adress_full.index("д."):].split(" ")[0].split(".")[1]
        try:
            korpus = adress_full[adress_full.index("к."):].split(" ")[0].split(".")[1].replace("кв","")
        except ValueError:
            korpus = ""  

        kvartira = adress_full[adress_full.index("кв."):].split(".")[1]    

        #Полуение типа документа
        doc_type = str(result[i][7]).strip()
        if doc_type.find("рожд") != -1 and doc_type.find("иност"):
            doc_type = "Свидетельство о рождении"
        elif doc_type.find("иност") != -1 and doc_type.find("рож") != -1:
            doc_type = "Иностанное свидетельство о рождении"
        elif doc_type.find("паспорт РФ") != -1:
            doc_type = "Паспорт гражданина Российской Федерации"
        elif doc_type.find("заграничный паспорт") != -1:
            doc_type = "Загранпаспорт"


        #Получение номера документа
        doc_full_raw = str(result[i][8]).replace(" ","")

        try:
            if doc_full_raw.find("сер.") != -1:
                serial = doc_full_raw.split("сер.")[1].split("№")[0]
                numb = doc_full_raw.split("сер.")[1].split("№")[1]
            else:
                serial = doc_full_raw[0:4]
                numb = doc_full_raw[4:]
        except:
            numb = doc_full_raw
            serial = ""

        #Получение органа выдавшего документ
        doc_org = str(result[i][9]).strip().replace("   "," ").replace("  "," ").replace("  "," ")

        #Получение кода органа
        doc_code = str(result[i][10]).replace(" ","")

        #Получение даты выдачи
        doc_date = str(result[i][11]).replace(" ","")

        #Получение нахуй не нужного описания
        description = str(result[i][12]).replace(" ","")

        query = "INSERT INTO civilians (id,last_name,first_name,patronymic,fio,date_of_birth,birth_region,street,dom,korpus,stroenie,kvartira,tel,sex,citizenship,doc_type,doc_serial,doc_number,doc_full,doc_org,doc_date,registration_type,registration_start,registration_end,to_del) VALUES ({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{})".format(incriment,"'"+last_name+"'","'"+first_name+"'","'"+patronymic+"'","'"+last_name + " " + first_name + " " + patronymic+"'","'"+birth_date+"'","'"+birth_region+"'","'"+street+"'","'"+home+"'","'"+korpus+"'","'"+""+"'","'"+kvartira+"'","'"+""+"'","'"+sex+"'","'"+citizenship+"'","'"+doc_type+"'","'"+serial+"'","'"+numb+"'","'"+serial+numb+"'","'"+doc_org+"'","'"+doc_date+"'","'"+"Постоянная"+"'","'"+description+"'","'"+""+"'","'"+""+"'")
        cursor.execute(query)
        connection.commit()
        print(str(i))
        incriment+=1
        count = count + cursor.rowcount

    print (count, "записей успешно добавлено")

except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", Exception, error.pgerror)
finally:
    if connection:
        cursor.close()
        connection.close()
        #print("Соединение с PostgreSQL закрыто")
