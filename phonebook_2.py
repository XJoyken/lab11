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
                cur.execute("""
                    INSERT INTO phonebook_2 (first_name, last_name, phone)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (first_name)
                    DO UPDATE SET last_name = EXCLUDED.last_name, phone = EXCLUDED.phone
                """, (row[0], row[1], row[2]))
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
        cur.execute("""
            INSERT INTO phonebook_2 (first_name, last_name, phone)
            VALUES (%s, %s, %s)
            ON CONFLICT (first_name)
            DO UPDATE SET phone = EXCLUDED.phone, last_name = EXCLUDED.last_name
            """, (first_name, last_name, phone))
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
        print("Filters: 1 - by first name, 2 - by surname, 3 - by phone, 4 - all records")
        choice = input("Choose (1, 2, 3, 4): ")
        
        if choice == "1":
            name = input("Enter first name to search: ")
            cur.execute("""
                        SELECT * FROM phonebook_2 WHERE first_name ILIKE %s
                        """, (f"%{name}%",))
        elif choice == "2":
            last_name = input("Enter surname to search: ")
            cur.execute("""
                        SELECT * FROM phonebook_2 WHERE last_name ILIKE %s
                        """, (f"%{last_name}%",))    
        elif choice == "3":
            phone = input("Enter phone to search: ")
            cur.execute("""
                        SELECT * FROM phonebook_2 WHERE phone ILIKE %s
                        """, (f"%{phone}%",))
        elif choice == "4":
            cur.execute("""
                        SELECT * FROM phonebook_2
                        """)
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
        bad_entries = []

        for first_name, phone in user_list:
            if not (phone.isdigit() or (phone.startswith('+') and phone[1:].isdigit())) or not (10 <= len(phone) <= 15):
                bad_entries.append((first_name, phone))
                continue

            cur.execute("SELECT * FROM phonebook_2 WHERE phone = %s", (phone,))
            if cur.fetchone():
                continue

            cur.execute("SELECT * FROM phonebook_2 WHERE first_name = %s", (first_name,))
            if cur.fetchone():
                cur.execute("UPDATE phonebook_2 SET phone = %s WHERE first_name = %s", (phone, first_name))
            else:
                cur.execute("INSERT INTO phonebook_2 (first_name, phone) VALUES (%s, %s)", (first_name, phone))
        
        conn.commit()
        if bad_entries:
            print("Incorrect entries:")
            for name, phone in bad_entries:
                print(f"{name} — {phone}")
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
            SELECT * FROM phonebook_2 ORDER BY id LIMIT %s OFFSET %s
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
            cur.execute(
                "DELETE FROM phonebook_2 WHERE first_name = %s",
                (name,)
            )
        elif choice == '2':
            phone = input("Enter phone to delete: ")
            cur.execute(
                "DELETE FROM phonebook_2 WHERE phone = %s",
                (phone,)
            )
        else:
            print("Invalid choice.")
            return
        
        if cur.rowcount == 0:
            print("No records found.")
        else:
            conn.commit()
            print(f"Deleted records: {cur.rowcount}")
    except Exception as e:
        print(f"Deletion error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        
def main():
    create_phonebook2_table()
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