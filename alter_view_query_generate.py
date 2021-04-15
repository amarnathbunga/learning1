import sys
from impala.dbapi import connect

IMPALA_HOST = 'es-estimation-n11.internal.ads.dailyhunt.in'
IMPALA_PORT = '21050'

PARTITION_COLUMNS = ['event_year', 'event_month', 'event_day', 'event_hour']

conn = connect(host='es-estimation-n11.internal.ads.dailyhunt.in', port=21050)


def get_table_schema(table_name):
    global conn
    cursor = conn.cursor()
    return cursor.get_table_schema(table_name)


def generate_alter_query(table_1, table_2, view_name, table_1_selection_criteria, table_2_selection_criteria):
    table_1_schema = get_table_schema(table_1)
    # Keep non-partition columns first
    table_columns = [column[0] for column in table_1_schema if column[0] not in PARTITION_COLUMNS]
    all_table_columns = table_columns + PARTITION_COLUMNS
    all_table_columns_str = ', '.join(all_table_columns)
    query = (
        f'''ALTER VIEW {view_name} AS SELECT {all_table_columns_str} '''
        f'''FROM {table_1} WHERE {table_1_selection_criteria} '''
        f'''UNION ALL SELECT {all_table_columns_str} '''
        f'''FROM {table_2} WHERE {table_2_selection_criteria} '''
    )
    return query


if __name__ == "__main__":
    kudu_table_name = sys.argv[1]
    hive_table_name = sys.argv[2]
    view_name = sys.argv[3]
    kudu_table_selection_criteria = sys.argv[4]
    hive_table_selection_criteria = sys.argv[5]
    query = generate_alter_query(kudu_table_name, hive_table_name, view_name,
                                 kudu_table_selection_criteria, hive_table_selection_criteria)
    # print to stdout
    print(query)

