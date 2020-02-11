import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pprint
import pandas
from google.cloud import bigquery
from google.oauth2 import service_account
from utils import *

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
nbKy = 11

# range
cell_range = 'B2:I14'

# radical
root = 'Creteil {}KY'

# Table name and dataset name
table_name = 'work_done'
dataset_name = 'data_state'


def export_data(worksheet_list):
    sheet_main = worksheet_list[0]
    nb = len(worksheet_list) - 1

    for k in range(nb):
        sheet = worksheet_list[k + 1]

        sheet_range = sheet.range(cell_range)
        subae_data = [item.value for item in sheet_range]

        data_dict_tobq = dict()
        header_gsheet = ["date", "morning_schedule", "subae", "ntf_ntc", "ttagui", "mannam", "nbj",
                         "education", "tm", "leaf", "bs", "wen_service", "tue_service"]
        i = 0

        for n in range(0, 104, 8):
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

        job_config = bigquery.LoadJobConfig(
            write_disposition='WRITE_TRUNCATE',
            schema=[
                bigquery.SchemaField('name', bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField('ky', bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField('date', bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField('subae', bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField('ttagui', bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField('ntf_ntc', bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField('mannam', bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField('education', bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField('tm', bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField('morning_schedule', bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField('nbj', bigquery.enums.SqlTypeNames.STRING),
                # bigquery.SchemaField('volontier', bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField('leaf', bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField('bs', bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField('wen_service', bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField('tue_service', bigquery.enums.SqlTypeNames.STRING),
            ]
        )

        full_name = '{}.{}.{}'.format(projectid, dataset_name, table_name)
        job = client_bq.load_table_from_dataframe(df, full_name, job_config=job_config)

        job.result()


def main():
    for i in range(nbKy):
        n = i + 1
        spreadsheet = client.open(root.format(n))
        worksheet_list = spreadsheet.worksheets()
        export_data(worksheet_list)

def clear(worksheet):


main()
