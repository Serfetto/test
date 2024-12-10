import datetime
import sqlite3
import xml.etree.ElementTree as ET
from typing import Optional
from xml.etree.ElementTree import Element
from pydantic import BaseModel, Field, ValidationError
from sqlite3 import Cursor, Connection

class xmlfile(BaseModel):
    ogrn: str = Field(max_length=13, min_length=13)
    inn: str = Field(max_length=10, min_length=10)
    name_company: Optional[str] = None
    phone: Optional[str] = None
    date_obn: datetime.date

def take_data_from_xml(child: Element):
    ogrn = child.findall("ОГРН")[0].text.strip()
    inn = child.findall("ИНН")[0].text.strip()
    name_company = child.findall("НазваниеКомпании")[0].text.strip()
    phone = ", ".join(phone.text.strip() for phone in child.findall("Телефон") if phone.text is not None)
    date_obn = datetime.datetime.strptime(child.findall("ДатаОбн")[0].text.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")
    return ogrn, inn, name_company, phone, date_obn

def setup_database():
    conn = sqlite3.connect('companies.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ogrn TEXT UNIQUE,
            inn TEXT,
            company_name TEXT,
            phone TEXT,
            update_date DATE
        )
    ''')
    conn.commit()
    return conn, cursor

def preproccesing(root):
    companies = {}
    for child in root:
        ogrn, inn, name_company, phone, date_obn = take_data_from_xml(child = child)

        try:
            val = xmlfile(ogrn=ogrn, inn=inn, name_company=name_company, phone=phone, date_obn=date_obn)
        except ValidationError as e:
            print(f"""\n{"-"*50}\nКомпания не прошла валидацию
{{
    ОГРН: {ogrn},
    ИНН: {inn},
    Название: {name_company},
    Телефон: {phone if len(phone) != 0 else "-"},
    Дата: {date_obn}
}}\n
Причина:
{e}
{"-"*50}""")
            continue
        
        if val.ogrn not in companies or companies[val.ogrn]['Дата'] < val.date_obn:
            companies[val.ogrn] = {
                'ОГРН': val.ogrn,
                'ИНН': val.inn,
                'Название': val.name_company,
                'Телефон': val.phone,
                'Дата': val.date_obn
            }
    return companies

def save_to_database(conn: Connection, cursor: Cursor, companies: dict):
    for company in companies.values():
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO companies (ogrn, inn, company_name, phone, update_date)
                VALUES (?, ?, ?, ?, ?)
            ''', (company['ОГРН'], company['ИНН'], company['Название'], company['Телефон'], company['Дата']))
        except Exception as e:
            print(f"Ошибка записи компании {company['ОГРН']}: {e}")
    conn.commit()  # Фиксация транзакции
    print("Cохранение компаний в базу данных произошло успешно")

if __name__ == '__main__':
    tree = ET.parse('companies.xml')
    root = tree.getroot()
    conn, cursor = setup_database()
    companies = preproccesing(root)
    save_to_database(conn, cursor, companies)
    conn.close()  # Закрытие соединения