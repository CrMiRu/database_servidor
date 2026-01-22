# CRUD operations (Create, Read, Update and Delete) for database records

from .connection import get_db_connection


def initialize_table():
    """Creates the table"""
    conn, cur = None, None
    try:
        conn, cur = get_db_connection()
        query = """
        CREATE TABLE IF NOT EXISTS demo_series (
            id SERIAL PRIMARY KEY,
            title VARCHAR(100) NOT NULL UNIQUE,
            genre VARCHAR(50),
            seasons INTEGER,
            rating NUMERIC(3, 1),
            release_year INTEGER
        );
        """
        cur.execute(query)
        conn.commit()
        print("Table 'demo_series' initialized successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error initializing table: {e}")
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_all_series():
    """Returns all rows from the database."""
    conn, cur = None, None
    try:
        conn, cur = get_db_connection()
        cur.execute("SELECT * FROM demo_series ORDER BY id ASC;")
        return cur.fetchall()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def insert_series(series_list):
    """Inserts rows"""
    conn, cur = None, None
    try:
        conn, cur = get_db_connection()
        query = """
        INSERT INTO demo_series (title, genre, seasons, rating, release_year)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (title) DO NOTHING;
        """
        cur.executemany(query, series_list)
        conn.commit()
        print(f"Successfully processed {len(series_list)} records.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error during insertion: {e}")
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def update_table(title, new_rating):
    """Updates a specific cell from the table"""
    conn, cur = None, None
    try:
        conn, cur = get_db_connection()
        query = "UPDATE demo_series SET rating = %s WHERE title = %s;"
        cur.execute(query, (new_rating, title))
        conn.commit()
        print(f"Updated {title} to rating {new_rating}")
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def delete_series(title):
    """Removes a row from the database."""
    conn, cur = None, None
    try:
        conn, cur = get_db_connection()
        query = "DELETE FROM demo_series WHERE title = %s;"
        cur.execute(query, (title,))
        conn.commit()
        print(f"Deleted {title} from database.")
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
