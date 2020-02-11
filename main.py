import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pprint
import pandas
import pandas_gbq
from google.oauth2 import service_account
from datetime import datetime

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('gsheetcred.json', scope)
credentials = service_account.Credentials.from_service_account_file(
    'gsheetcred.json',
)

client = gspread.authorize(creds)

projectid = "cedar-freedom-138023"
spreadsheet = client.open('Creteil 1KY')
worksheet_list = spreadsheet.worksheets()

sheet_main = worksheet_list[0]

sheet = worksheet_list[2]
sheet_range = sheet.range('B2:I14')

pp = pprint.PrettyPrinter()
subae_data = [item.value for item in sheet_range]

data_dict_tobq = dict()
header_gsheet = ["date", "morning_schedule", "subae", "ntf_ntc", "ttagui", "mannam", "nbj", "education", "tm", "leaf",
                 "bs", "wen_service", "tue_service"]
i = 0


def attendance(value):
    if value == '○':
        return 'P'
    elif value == '●':
        return 'Abs'
    elif value == '◐':
        return 'L'
    else:
        return ''


def format_date(value):
    value = value + str(datetime.now().year)
    return str(datetime.strptime(value, '%d/%m%Y').date())


for n in range(0, 104, 8):
    l = subae_data[n:n + 8]

    cols = header_gsheet[i]
    if cols == 'morning_schedule':
        data_dict_tobq[cols] = list(map(attendance, l[1:]))
    elif cols == 'date':
        data_dict_tobq[cols] = list(map(format_date, l[1:]))
    else:
        data_dict_tobq[cols] = l[1:]
    i = i + 1

data_dict_tobq['name'] = [sheet.title for i in range(7)]
data_dict_tobq['ky'] = [sheet_main.title for i in range(7)]

df = pandas.DataFrame(data_dict_tobq)

print(df)
pandas_gbq.to_gbq(
    df, 'data_state.work_done', project_id=projectid,
    if_exists='append', credentials=credentials
)
