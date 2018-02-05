from config import get_postgres_config
from psycopg2 import connect as psycopg_connect
from psycopg2 import DatabaseError


def create_tables():
    """ Create tables in the Postgres database necessary
        for the file parser to run
    """
    commands = (
        """
        CREATE TABLE specification_formats (
            spec_id SERIAL PRIMARY KEY,
            spec_name VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE specification_format_columns (
                column_id SERIAL PRIMARY KEY,
                spec_id INTEGER NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                column_width INTEGER NOT NULL,
                column_data_type VARCHAR(255) NOT NULL,
                FOREIGN KEY (spec_id)
                REFERENCES specification_formats (spec_id)
                ON DELETE CASCADE
                )
        """)
    conn = None
    try:
        params = get_postgres_config()
        conn = psycopg_connect(**params)
        cur = conn.cursor()

        for command in commands:
            cur.execute(command)

        cur.close()
        conn.commit()
    except DatabaseError as error:
        print error
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()
