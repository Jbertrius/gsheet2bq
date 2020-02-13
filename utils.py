from datetime import datetime
from google.cloud import bigquery


def attendance(value):
    if value == '○':
        return 'Present'
    elif value == '●':
        return 'Absent'
    elif value == '◐':
        return 'Late'
    elif value == '□':
        return 'Evening Present'
    elif value == '▣':
        return 'Evening Late'
    else:
        return ''


def format_int(val):
    if val != '':
        return int(val)
    else:
        return None


def format_date(value):
    value = value + str(datetime.now().year)
    return datetime.strptime(value, '%d/%m%Y').date()


def load_job(df, client_bq, project_id, dataset, table):
    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_APPEND',
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
            bigquery.SchemaField('volontier', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('leaf', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('bs', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('wen_service', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('tue_service', bigquery.enums.SqlTypeNames.STRING),
        ]
    )

    full_name = '{}.{}.{}'.format(project_id, dataset, table)
    job = client_bq.load_table_from_dataframe(df, full_name, job_config=job_config)

    job.result()
