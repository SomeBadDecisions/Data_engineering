update_f_retention_table = PostgresOperator(
        task_id='update_f_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql"