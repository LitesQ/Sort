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
    query = "SELECT * FROM riur_14"
    cursor.execute(query)

    # Массив с данными 
    result = cursor.fetchall()

    for i in range(len(result)):
        #Разделение ФИО
        fio = str(result[i][1]).replace("'","`").split()
        try:
            last_name = fio[0].strip().title()
        except IndexError:
            continue

        first_name = fio[1].strip().title()
        patronymic = "-"
        if len(fio) > 2:
            patronymic = fio[2].strip().title()
        if len(fio) == 4 and fio[3]!= "-":   
            patronymic += " " + fio[3].strip().title()

        #Получение даты рождения
        birth_date = str(result[i][2]).replace(" ","")

        #Получение места рождения
        birth_region_var = result[i][3].split()
        birth_region = ""
        for row in range(len(birth_region_var)):
            birth_region += birth_region_var[row] + " "
        birth_region = birth_region.strip()

        #Получение пола
        sex = str(result[i][4]).replace(" ","").title()
        if sex == "Мужской":
            sex = "М"
        elif sex == "Женский":
            sex = "Ж"
        
        #Получение гражданства
        citizenship = str(result[i][5]).strip()

        #Получение адреса
        adress_full = str(result[i][6]).strip()
        street = adress_full.split(",")[1].strip()
        if street.find("атайский") != -1:
            street = "Батайский пр."
        elif street.find("елореченская") != -1:
            street = "Белореченская ул."
        elif street.find("ратиславская") != -1:
            street = "Братиславская ул."
        elif street.find("ерхние") != -1:
            street = "Верхние поля ул."
        elif street.find("олованова") != -1:
            street = "Голованова маршала ул."
        elif street.find("онецкая") != -1:
            street = "Донецкая ул."
        elif street.find("уговой") != -1:
            street = "Луговой пр."
        elif street.find("юблинская") != -1:
            street = "Люблинская ул."
        elif street.find("арьинский бульв") != -1:
            street = "Марьинский бульв."
        elif street.find("арьинский Парк") != -1 or street.find("арьинский парк") != -1:
            street = "Марьинский Парк ул."
        elif street.find("ячковский") != -1:
            street = "Мячковский бульв."
        elif street.find("овомарьинская") != -1:
            street = "Новомарьинская ул."
        elif street.find("овочеркасский") != -1:
            street = "Новочеркасский бульв."
        elif street.find("ерерва") != -1:
            street = "Перерва ул."
        elif street.find("ерервинский") != -1:
            street = "Перервинский бульв."
        elif street.find("одольская") != -1:
            street = "Подольская ул."
        elif street.find("оречная") != -1:
            street = "Поречная ул."
            
        if adress_full.find(" д ")!=-1 and adress_full.find("д.")==-1:
            home = adress_full[adress_full.index(" д "):].split(" ")[2].replace(" ","").replace(",","")
        elif adress_full.find(" д ")==-1 and adress_full.find("д.")!=-1:
            home = adress_full[adress_full.index("д."):].split(".")[1].split(",")[0].replace(" ","").replace(",","")
        else:
            home = adress_full
             

        if adress_full.find(" к ")!=-1 and adress_full.find("к.")==-1:
            korpus = adress_full[adress_full.index(" к "):].split(" ")[2].replace(" ","").replace(",","")
        elif adress_full.find(" к ")==-1 and adress_full.find("к.")!=-1:
            korpus = adress_full[adress_full.index("к."):].split(".")[1].split(",")[0].replace(" ","").replace(",","")
        else: korpus = ""
        if adress_full.find(" кв ")!=-1 and adress_full.find("кв.")==-1:
            kvartira = adress_full[adress_full.index(" кв "):].replace(" кв ","").strip()         
        elif adress_full.find(" кв ")==-1 and adress_full.find("кв.")!=-1:
            kvartira = adress_full[adress_full.index("кв."):].replace("кв. ","").strip()
        else: kvartira = ""

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
        description = str(result[i][12]).strip()
        description = description[0:28].replace("Дата регистрации: ","")
        
        #print("'"+last_name+"'","'"+first_name+"'","'"+patronymic+"'","'"+last_name + " " + first_name + " " + patronymic+"'","'"+birth_date+"'","'"+birth_region+"'","'"+street+"'","'"+home+"'","'"+korpus+"'","'"+""+"'","'"+kvartira+"'","'"+""+"'","'"+sex+"'","'"+citizenship+"'","'"+doc_type+"'","'"+serial+"'","'"+numb+"'","'"+serial+numb+"'","'"+doc_org+"'","'"+doc_date+"'","'"+"Постоянная"+"'","'"+description+"'","'"+""+"'","'"+""+"'")

        #query = "".format(incriment,)
        query = "UPDATE civilians SET last_name = {},first_name = {},patronymic = {},fio = {},date_of_birth = {},birth_region = {},street = {},dom = {},korpus = {},stroenie = {},kvartira = {},tel = {},sex = {},citizenship = {},doc_type = {},doc_serial = {},doc_number = {},doc_full = {},doc_org = {},doc_date = {},registration_type = {},registration_start = {},registration_end = {},to_del = {} WHERE fio = {} and date_of_birth = {}".format("'"+last_name+"'","'"+first_name+"'","'"+patronymic+"'","'"+last_name + " " + first_name + " " + patronymic+"'","'"+birth_date+"'","'"+birth_region+"'","'"+street+"'","'"+home+"'","'"+korpus+"'","'"+""+"'","'"+kvartira+"'","'"+""+"'","'"+sex+"'","'"+citizenship+"'","'"+doc_type+"'","'"+serial+"'","'"+numb+"'","'"+serial+numb+"'","'"+doc_org+"'","'"+doc_date+"'","'"+"Постоянная"+"'","'"+description+"'","'"+""+"'","'"+""+"'","'"+last_name + " " + first_name + " " + patronymic+"'","'"+birth_date+"'")
        cursor.execute(query)
        connection.commit()
        print(str(i))
        count = cursor.rowcount
        print(count, "Record updated successfully")
        if count == 0:
            query = "INSERT INTO civilians (id,last_name,first_name,patronymic,fio,date_of_birth,birth_region,street,dom,korpus,stroenie,kvartira,tel,sex,citizenship,doc_type,doc_serial,doc_number,doc_full,doc_org,doc_date,registration_type,registration_start,registration_end,to_del) VALUES ({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{})".format(incriment,"'"+last_name+"'","'"+first_name+"'","'"+patronymic+"'","'"+last_name + " " + first_name + " " + patronymic+"'","'"+birth_date+"'","'"+birth_region+"'","'"+street+"'","'"+home+"'","'"+korpus+"'","'"+""+"'","'"+kvartira+"'","'"+""+"'","'"+sex+"'","'"+citizenship+"'","'"+doc_type+"'","'"+serial+"'","'"+numb+"'","'"+serial+numb+"'","'"+doc_org+"'","'"+doc_date+"'","'"+"Постоянная"+"'","'"+""+"'","'"+""+"'","'"+""+"'")
            cursor.execute(query)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record INSERTED successfully")
            incriment+=1


except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error.pgerror)
finally:
    if connection:
        cursor.close()
        connection.close()
        #print("Соединение с PostgreSQL закрыто")
