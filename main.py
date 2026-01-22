from database.operations import (
    initialize_table,
    insert_series,
    get_all_series,
    update_table,
)


def main():
    # 1. Create the table schema
    initialize_table()

    # 2. Insert values to the table
    series_data = [
        ("Doctor Who", "Sci-Fi", 12, 8.6, 2005),
    ]
    insert_series(series_data)

    # 4. Print the records added to the terminal
    rows = get_all_series()
    print("\n--- Current Records ---")
    for row in rows:
        print(row)

    # 5. Modify a record of the table, for example rating
    update_table("Stranger Things", 9.2)


if __name__ == "__main__":
    main()
