def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'

    local_filename = date.replace('-', '') + '_' + filename

    response = requests.get(s3_filename)
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df = df.drop_duplicates(subset=['id'])
    df = df.drop(['id'], axis = 1) #Новая строка

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)