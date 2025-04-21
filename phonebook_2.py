import psycopg2 
import csv

db_params = {
    'dbname': 'phonebook_2',
    'user': 'postgres',
    'password': '12345678',
    'host': 'localhost',
    'port': '5432'
}

def connect_db():
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        print(f"Connection error: {e}")
        return None
    
def create_phonebook2_table():
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS phonebook_2 (
                        id SERIAL PRIMARY KEY,
                        first_name VARCHAR(50) UNIQUE NOT NULL,
                        last_name VARCHAR(50),
                        phone VARCHAR(20) NOT NULL
                    );
                    """)
        conn.commit()
        print(f"Table phonebook_2 created or already exists.")
    except Exception as e:
        print(f"Table creation error: {e}")
    finally:
        cur.close()
        conn.close()

def create_search_function():
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE OR REPLACE FUNCTION search_phonebook(pattern TEXT)
            RETURNS TABLE (
                id INTEGER,
                first_name VARCHAR,
                last_name VARCHAR,
                phone VARCHAR
            )
            AS $$
            BEGIN
                RETURN QUERY
                SELECT p.id, p.first_name, p.last_name, p.phone
                FROM phonebook_2 p
                WHERE p.first_name ILIKE '%' || pattern || '%'
                   OR p.last_name ILIKE '%' || pattern || '%'
                   OR p.phone ILIKE '%' || pattern || '%';
            END;
            $$ LANGUAGE plpgsql;
        """)
        conn.commit()
        print("search_phonebook function created or updated.")
    except Exception as e:
        print(f"Function creation error: {e}")
    finally:
        cur.close()
        conn.close()

       
def insert_or_update_user():
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE OR REPLACE FUNCTION insert_or_update_user(p_first_name VARCHAR, p_last_name VARCHAR, p_phone VARCHAR)
            RETURNS VOID
            AS $$
            BEGIN
                INSERT INTO phonebook_2 (first_name, last_name, phone)
                VALUES (p_first_name, p_last_name, p_phone)
                ON CONFLICT (first_name)
                DO UPDATE SET last_name = EXCLUDED.last_name, phone = EXCLUDED.phone;
            END;
            $$ LANGUAGE plpgsql;
            """)
        conn.commit()
        print("insert_or_update_user function created or updated.")
    except Exception as e:
        print(f"Function creation error: {e}")
    finally:
        cur.close()
        conn.close()      

# CREATE OR REPLACE PROCEDURE insert_or_update_user(
#     p_first_name VARCHAR, 
#     p_last_name VARCHAR, 
#     p_phone VARCHAR
# )
# LANGUAGE plpgsql
# AS $$
# BEGIN
#     INSERT INTO phonebook_2 (first_name, last_name, phone)
#     VALUES (p_first_name, p_last_name, p_phone)
#     ON CONFLICT (first_name)
#     DO UPDATE 
#     SET last_name = EXCLUDED.last_name, phone = EXCLUDED.phone;
# END;
# $$;

def insert_many_users():
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE OR REPLACE PROCEDURE insert_many_users(
                IN names TEXT[],
                IN phones TEXT[],
                OUT bad_data TEXT[]
            )
            LANGUAGE plpgsql
            AS $$
            DECLARE
                i INTEGER := 1;
                entry TEXT;
            BEGIN
                bad_data := ARRAY[]::TEXT[];
                WHILE i <= array_length(names, 1) LOOP
                    IF phones[i] ~ '^\+?\d{10,15}$' THEN
                        PERFORM insert_or_update_user(names[i], NULL, phones[i]);
                    ELSE
                        entry := names[i] || ' — ' || phones[i];
                        bad_data := array_append(bad_data, entry);
                    END IF;
                    i := i + 1;
                END LOOP;
            END;
            $$;
            """)
        conn.commit()
        print("insert_many_users procedure created or updated.")
    except Exception as e:
        print(f"Function creation error: {e}")
    finally:
        cur.close()
        conn.close() 
       
def get_phonebook_paginated():
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE OR REPLACE FUNCTION get_phonebook_paginated(
                p_limit INTEGER,
                p_offset INTEGER
            )
            RETURNS TABLE (
                id INTEGER,
                first_name VARCHAR,
                last_name VARCHAR,
                phone VARCHAR
            )
            AS $$
            BEGIN
                RETURN QUERY
                SELECT p.id, p.first_name, p.last_name, p.phone
                FROM phonebook_2 p
                ORDER BY p.id
                LIMIT p_limit OFFSET p_offset;
            END;
            $$ LANGUAGE plpgsql;
            """)
        conn.commit()
        print("get_phonebook_paginated function created or updated.")
    except Exception as e:
        print(f"Function creation error: {e}")
    finally:
        cur.close()
        conn.close() 

def delete_from_phonebook_procedure():
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE OR REPLACE PROCEDURE delete_from_phonebook(
                p_username VARCHAR,
                p_phone VARCHAR
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF p_username IS NOT NULL THEN
                    DELETE FROM phonebook_2 WHERE first_name = p_username;
                    RAISE NOTICE 'Deleted records with first_name: %', p_username;
                END IF;

                IF p_phone IS NOT NULL THEN
                    DELETE FROM phonebook_2 WHERE phone = p_phone;
                    RAISE NOTICE 'Deleted records with phone: %', p_phone;
                END IF;
                
                IF NOT FOUND THEN
                    RAISE NOTICE 'No records found for deletion.';
                END IF;
            END;
            $$;
            """)
        conn.commit()
        print("delete_from_phonebook procedure created or updated.")
    except Exception as e:
        print(f"Function creation error: {e}")
    finally:
        cur.close()
        conn.close() 

def insert_from_csv(file_path):
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cur.execute("SELECT insert_or_update_user(%s, %s, %s)", (row[0], row[1], row[2]))
        conn.commit()
        print("Data from CSV loaded successfully.")
    except Exception as e:
        print(f"CSV loading error: {e}")
    finally:
        cur.close()
        conn.close()


# INSERT INTO phonebook_2 (first_name, last_name, phone)
#                     VALUES (%s, %s, %s)
#                     ON CONFLICT (first_name)
#                     DO UPDATE SET phone = EXCLUDED.phone, last_name = EXCLUDED.last_name
#                     """, (row[0], row[1], row[2]))
def insert_from_console():
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        first_name = input("Enter first name: ")
        last_name = input("Enter last name (or leave empty): ")
        phone = input("Enter phone number: ")
        cur.execute("SELECT insert_or_update_user(%s, %s, %s)", (first_name, last_name, phone))
        conn.commit()
        print("Data added successfully.")
    except Exception as e:
        print(f"Data insertion error: {e}")
    finally:
        cur.close()
        conn.close()
        
def update_phonebook():
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        print("Update: 1 - first name, 2 - phone")
        choice = input("Choose (1 or 2): ")
        phone_id = input("Enter record ID to update: ")
        
        if choice == "1":
            new_name = input("Enter new first name: ")
            cur.execute("""
                        UPDATE phonebook_2 SET first_name = %s WHERE id = %s
                        """, (new_name, phone_id))
        elif choice == '2':
            new_phone = input("Enter new phone number: ")
            cur.execute("""
                        UPDATE phonebook_2 SET phone = %s WHERE id = %s
                        """, (new_phone, phone_id))
        else:
            print("Invalid choice")
            return
        conn.commit()
        print("Record updated successfully.")
    except Exception as e:
        print(f"Update error: {e}")
    finally:
        cur.close()
        conn.close()
        
def query_phonebook():
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        print("Filters: 1 - by pattern (first name, last name, phone), 2 - all records.")
        choice = input("Choose (1, 2): ")
        if choice == "1":
            pattern = input("Enter search pattern (part of first name, last name or phone): ")
            cur.execute("SELECT * FROM search_phonebook(%s)", (pattern,))
        elif choice == "2":
            cur.execute("SELECT * FROM phonebook_2")
        else:
            print("Invalid choice")
            return
        rows = cur.fetchall()
        if not rows:
            print("No records found.")
        else:
            for row in rows:
                print(f"ID: {row[0]}, First Name: {row[1]}, Last Name: {row[2] or 'N/A'}, Phone: {row[3]}")
    except Exception as e:
        print(f"Query error: {e}")
    finally:
        cur.close()
        conn.close()

def parse_users_input():
    raw = input("Enter name and phone in turn: ")
    parts = raw.strip().split()
    
    if len(parts) % 2 != 0:
        print("Invalid list")
        return []
    
    user_list = [(parts[i], parts[i+1]) for i in range(0, len(parts), 2)]
    return user_list

def bulk_insert_users(user_list):
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        names = [name for name, _ in user_list]
        phones = [phone for _, phone in user_list]
        cur.execute("""
            CALL insert_many_users(%s, %s, %s)
        """, (names, phones, None))
        conn.commit()
        cur.execute("SELECT * FROM unnest(%s::text[])", (cur.fetchone()[0],)) #разворачивает массив в строку
        bad_entries = cur.fetchall()

        if bad_entries:
            print("Incorrect entries:")
            for entry in bad_entries:
                print(entry[0])
        else:
            print("All users inserted/updated successfully.")
    except Exception as e:
        print(f"Bulk insert error: {e}")
    finally:
        cur.close()
        conn.close()


def paginated_query():
    conn = connect_db()
    if conn is None:
        return
    try:
        limit = int(input("Enter limit (number of records per page): "))
        offset = int(input("Enter offset (start from record #): "))

        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM get_phonebook_paginated(%s, %s);
        """, (limit, offset))

        rows = cur.fetchall()
        if not rows:
            print("No records.")
        else:
            for row in rows:
                print(f"ID: {row[0]}, First Name: {row[1]}, Last Name: {row[2] or 'N/A'}, Phone: {row[3]}")
    except Exception as e:
        print(f"Pagination error: {e}")
    finally:
        cur.close()
        conn.close()

def delete_from_phonebook():
    conn = connect_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        print("Delete by: 1 - first name, 2 - phone")
        choice = input("Choose (1 or 2): ")

        if choice == '1':
            name = input("Enter first name to delete: ")
            cur.execute("CALL delete_from_phonebook(%s, NULL);", (name,))
        elif choice == '2':
            phone = input("Enter phone to delete: ")
            cur.execute("CALL delete_from_phonebook(NULL, %s);", (phone,))
        else:
            print("Invalid choice.")
            return
        
        conn.commit()
        print("Delete operation completed.")
        
    except Exception as e:
        print(f"Deletion error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

        
def main():
    create_phonebook2_table()
    create_search_function()
    insert_or_update_user()
    insert_many_users()
    get_phonebook_paginated()
    delete_from_phonebook_procedure()
    while True:
        print("\nPhoneBook Menu:")
        print("1. Add data from CSV")
        print("2. Add data via console")
        print("3. Add data from list")
        print("4. Update data")
        print("5. Query data")
        print("6. Paginated query")
        print("7. Delete data")
        print("8. Exit")
        choice = input("Choose action (1-8): ")
        
        if choice == '1':
            file_path = input("Enter CSV file path: ")
            insert_from_csv(file_path)
        elif choice == '2':
            insert_from_console()
        elif choice == '3':
            list = parse_users_input()
            bulk_insert_users(list)    
        elif choice == '4':
            update_phonebook()
        elif choice == '5':
            query_phonebook()
        elif choice == '6':
            paginated_query()
        elif choice == '7':
            delete_from_phonebook()
        elif choice == '8':
            print("Exiting program.")
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
