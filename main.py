import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas
from google.oauth2 import service_account
from utils import *
from datetime import timedelta

# Credentials
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('gsheetcred.json', scope)
credentials = service_account.Credentials.from_service_account_file(
    'gsheetcred.json'
)

# Project ID
projectid = "cedar-freedom-138023"

# Client
client = gspread.authorize(creds)
client_bq = bigquery.Client(project=projectid, credentials=credentials)

# Nb of KY
nbKy = 1

# range
cell_range = 'B2:I15'
range_to_delete = 'C3:I15'
date_range = 'C2:I2'

# radical
root = 'Copie de Creteil {}KY'

# Table name and dataset name
table_name = 'work_done'
dataset_name = 'data_state'
header_gsheet = ["date", "morning_schedule", "subae", "ntf_ntc", "ttagui", "mannam", "nbj", "volontier", "education",
                 "tm", "leaf", "bs", "wen_service", "tue_service"]


def export_data(worksheet_list):
    sheet_main = worksheet_list[0]
    nb = len(worksheet_list) - 1

    large_df = pandas.DataFrame()

    for k in range(nb):
        sheet = worksheet_list[k + 1]

        sheet_range = sheet.range(cell_range)
        subae_data = [item.value for item in sheet_range]

        data_dict_tobq = dict()

        i = 0

        for n in range(0, len(subae_data), 8):
            l = subae_data[n:n + 8]

            cols = header_gsheet[i]
            if cols == 'morning_schedule' or cols == 'tue_service' or cols == 'wen_service' or cols == 'leaf':
                data_dict_tobq[cols] = list(map(attendance, l[1:]))
            elif cols == 'date':
                data_dict_tobq[cols] = list(map(format_date, l[1:]))
            elif cols == 'subae' or cols == 'ntf_ntc' or cols == 'ttagui' \
                    or cols == 'mannam' or cols == 'education' or cols == 'tm':
                data_dict_tobq[cols] = list(map(format_int, l[1:]))
            else:
                data_dict_tobq[cols] = l[1:]
            i = i + 1

        data_dict_tobq['name'] = [sheet.title for i in range(7)]
        data_dict_tobq['ky'] = [sheet_main.title for i in range(7)]

        df = pandas.DataFrame(data_dict_tobq)
        if large_df.empty:
            large_df = df
        else:
            large_df = large_df.append(df)

        clear(sheet)

    load_job(large_df, client_bq, projectid, dataset_name, table_name)


def main():
    for i in range(nbKy):
        n = i + 1
        spreadsheet = client.open(root.format(n))
        worksheet_list = spreadsheet.worksheets()
        export_data(worksheet_list)


def clear(worksheet):
    cell_list = worksheet.range(range_to_delete)
    for cell in cell_list:
        cell.value = ''

    date_cell_list = worksheet.range(date_range)
    date_cell_list_cvt = ['{}/{}'.format(item.value, str(datetime.now().year)) for item in date_cell_list]

    init = 1
    for cell in date_cell_list:
        cell.value = (datetime.strptime(date_cell_list_cvt[-1], '%d/%m/%Y').date() + timedelta(days=init)).strftime("%d/%m")
        init = init + 1
    # Update in batch
    worksheet.update_cells(cell_list)
    worksheet.update_cells(date_cell_list)


main()
