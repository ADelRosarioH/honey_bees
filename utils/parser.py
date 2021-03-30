import os
import re
import uuid
import datetime
import pdfplumber
import urllib
import pandas as pd
from io import StringIO
from price_parser import Price
from dateparser import parse

months_regex = re.compile(
    r'(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)', re.I)
days_regex = re.compile(r'[^0-9][0-9]{2}[^0-9]')
years_regex = re.compile(r'[0-9]{4}')


def get_months(text):
    return months_regex.findall(text)


def get_days(text):
    return [digits.replace('-', '').replace('_', '') for digits in days_regex.findall(text.split('.')[0])]


def get_years(text):
    return years_regex.findall(text)


def get_from_date(text):
    months = get_months(text)
    days = get_days(text)
    years = get_years(text)
    month = _get_parsed_month(months[0] if len(months) >= 1 else 1)
    day = days[0] if len(days) >= 1 else 1
    year = years[0] if len(years) >= 1 else 2017

    date = datetime.datetime(year=int(year), month=int(month), day=int(day))
    return date


def get_to_date(text):
    from_date = get_from_date(text)

    months = get_months(text)
    days = get_days(text)
    years = get_years(text)

    month = _get_parsed_month(months[1]) if len(
        months) > 1 else from_date.month
    day = days[1] if len(days) > 1 else from_date.day
    year = years[1] if len(years) > 1 else from_date.year

    date = datetime.datetime(year=int(year), month=int(month), day=int(day))
    return date


def _get_parsed_month(month):
    return parse(str(month), locales=['es-DO']).month


def get_date_range_from_text(text):
    unquote = urllib.parse.unquote
    text = unquote(unquote(text))
    filename = os.path.basename(text)
    return [get_from_date(filename), get_to_date(filename)]


def pdf_to_csv_a(file_path):
    excluded_columns_regex = re.compile(
    r'(CASA EDITORIAL|n0|NO|Orden|Resumen General Media y/o|Promedio Global|Precios|Mínimo|Precios|Máximo|Moda|Mediana|Desviación|Estándar)', re.I)

    path, ext = os.path.splitext(file_path)
    name = os.path.basename(path)
    file_output = '{}{}'.format(path, '.csv')

    if os.path.exists(file_output):
        return file_output

    start_date, end_date = get_date_range_from_text(file_path)

    pages = []

    with pdfplumber.open(file_path) as pdf:
        
        total_pages = len(pdf.pages)
        page_count = 0


        for page in pdf.pages:
            page_count += 1

            print('Parsing {} of {}'.format(page_count, total_pages))

            # extract table from every page individually.
            # pdf can have fixed headers and this messes up with
            # the implementation.
            tables = page.extract_tables()
            # in case of hardware products, last page find a table with no cells
            if len(tables) <= 0:
                continue

            # pdfplumber cloud return multiple tables for the same
            # table instance in the pdf
            df = pd.concat([pd.DataFrame(table)
                            for table in tables], axis=1)

            # export and read csv to merge header rows.
            df = df.replace('\n', '', regex=True)
            df.fillna('--', inplace=True)

            if len(df.index) < 3:
                continue

            output = StringIO(df.to_csv(header=False, index=False))
            df = pd.read_csv(output, header=[0, 1, 2])
            
            df.columns = df.columns.map(lambda h: '{} {} {}'.format(
                h[0], h[1], h[2]).replace('\n', '|'))

            df.rename_axis('id')
            
            # excludes irrelevant columns
            excluded_columns = [df.columns.get_loc(
                c) for c in df.columns if excluded_columns_regex.search(c)]
            df.drop(df.columns[excluded_columns], axis=1, inplace=True)

            items = pd.DataFrame(data=None, columns=['id', 'file_id', 'description', 'unit',
                                                        'vendor', 'price', 'currency', 'start_date',
                                                        'end_date'])

            # read pivoted (product x vendor) data
            for index, row in df.iterrows():
                if len(row) < 2:
                    continue

                description = str(row[0]).strip()
                unit = str(row[1]).strip()

                if (not description or not unit):
                    continue

                for (label, contents) in df[df.columns[2:]].iteritems():
                    value = contents.at[index]
                    price = Price.fromstring(str(value))
                    if not price.amount:
                        continue

                    vendor = re.sub(
                        r'Unnamed:\s\d{1,2}_level_\d{1,2}', '', label)

                    item = {
                        'id': uuid.uuid4(),
                        'file_id': name,
                        'description': description,
                        'unit': unit,
                        'vendor': vendor,
                        'price': price.amount,
                        'currency': 'DOP',
                        'start_date': start_date,
                        'end_date': end_date
                    }

                    items = items.append(item, ignore_index=True)

            page.close()
            pages.append(items)

        pdf.close()

    pd.concat(pages).to_csv(file_output, index=False)

    return file_output


def pdf_to_csv_b(file_path):
    excluded_columns_regex = re.compile(
    r'(n0|no|Orden|Resumen|General|Media|Promedio|Global|Mínimo|Máximo|Moda|Mediana|Desviación|Estándar)', re.I)

    generic_columns_regex = re.compile(r'Genérico|Principio|Activo|Concentrac', re.I)

    commercial_columns_regex = re.compile(r'Marca|Concentrac', re.I)

    clean_columns_regex = re.compile(r'Genérico|Principio|Activo')

    def parse_report(df, name, start_date, end_date):
        items = pd.DataFrame(data=None, columns=['id', 'file_id', 'medication_name', 'dosage',
                                                    'maker', 'unit', 'vendor', 'price', 'currency',
                                                    'start_date', 'end_date'])

        # read pivoted (product x vendor) data
        for index, row in df.iterrows():
            medication_name = None if len(row) <= 1 else str(row[0]).strip()
            dosage = None if len(row) <= 2 else str(row[1]).strip()
            maker = None if len(row) <= 3 else str(row[2]).strip()
            unit = None if len(row) <= 4 else str(row[3]).strip()

            if (not medication_name and not dosage and not maker and not unit):
                continue

            for (label, contents) in df[df.columns[4:]].iteritems():
                value = contents.at[index]
                price = Price.fromstring(str(value))
                if not price.amount:
                    continue

                vendor = re.sub(
                    r'Unnamed:\s\d{1,2}_level_\d{1,2}', '', label)

                item = {
                    'id': uuid.uuid4(),
                    'file_id': name,
                    'medication_name': medication_name,
                    'dosage': dosage,
                    'maker': maker,
                    'unit': unit,
                    'vendor': vendor,
                    'price': price.amount,
                    'currency': 'DOP',
                    'start_date': start_date,
                    'end_date': end_date
                }

                items = items.append(item, ignore_index=True)

        return items

    path, ext = os.path.splitext(file_path)
    name = os.path.basename(path)

    file_output = '{}{}'.format(path, '.csv')
    generic_file_output = '{}-{}{}'.format(path, 'generic', '.csv')
    commercial_file_output = '{}-{}{}'.format(path, 'commercial', '.csv')
    
    existing_outputs = []

    if os.path.exists(file_output):
        existing_outputs.append(file_output)

    if os.path.exists(generic_file_output):
        existing_outputs.append(generic_file_output)

    if os.path.exists(commercial_file_output):
        existing_outputs.append(commercial_file_output)

    if (len(existing_outputs) > 0):
        return existing_outputs

    start_date, end_date = get_date_range_from_text(file_path)
    
    generic_report_pages = []
    commercial_report_pages = []
    single_report_pages = []

    with pdfplumber.open(file_path) as pdf:
        total_pages = len(pdf.pages)
        page_count = 0
        
        for page in pdf.pages:
            page_count += 1

            print('Parsing {} of {}'.format(page_count, total_pages))

            # extract table from every page individually.
            # pdf can have fixed headers and this messes up with
            # the implementation.
            tables = page.extract_tables()

            # pdfplumber cloud return multiple tables for the same
            # table instance in the pdf
            df = pd.concat([pd.DataFrame(table)
                            for table in tables], axis=1)

            output = StringIO(df.to_csv(header=False, index=False))
            df = pd.read_csv(output, header=[0, 1, 2])
            df.columns = df.columns.map(lambda h: '{} {} {}'.format(
                h[0], h[1], h[2]).replace('\n', '|'))

            # excludes irrelevant columns
            excluded_columns = [df.columns.get_loc(
                c) for c in df.columns if excluded_columns_regex.search(c)]

            df.drop(df.columns[excluded_columns], axis=1, inplace=True)

            # df = df.dropna(how='all', axis=1)

            generic_columns = [df.columns.get_loc(
                c) for c in df.columns if generic_columns_regex.search(c)]

            commercial_columns = [df.columns.get_loc(
                c) for c in df.columns if commercial_columns_regex.search(c)]

            if (len(generic_columns) > 1 and len(commercial_columns) > 1):
                generic_df = df.copy()
                generic_df.drop(
                    generic_df.columns[commercial_columns], axis=1, inplace=True)

                cleaned_columns = [
                    re.sub(clean_columns_regex, '', c) for c in generic_df.columns]

                generic_df.columns = cleaned_columns

                generic_report_page = parse_report(
                    generic_df, name, start_date, end_date)
                generic_report_pages.append(generic_report_page)

                commercial_df = df.copy()
                commercial_df.drop(
                    commercial_df.columns[generic_columns], axis=1, inplace=True)
                
                if (len(commercial_df.columns) == len(cleaned_columns)):
                    commercial_df.columns = cleaned_columns

                commercial_report_page = parse_report(
                    commercial_df, name, start_date, end_date)
                commercial_report_pages.append(
                    commercial_report_page)

            else:
                single_report_page = parse_report(
                    df, name, start_date, end_date)
                single_report_pages.append(single_report_page)
            
            page.close()

        pdf.close()

    if (len(generic_report_pages) >= 1):
        pd.concat(generic_report_pages).to_csv(
            generic_file_output, index=False)

    if (len(commercial_report_pages) >= 1):
        pd.concat(commercial_report_pages).to_csv(
            commercial_file_output, index=False)

    if (len(single_report_pages) >= 1):
        pd.concat(single_report_pages).to_csv(file_output, index=False)

    return [file_output, generic_file_output, commercial_file_output]