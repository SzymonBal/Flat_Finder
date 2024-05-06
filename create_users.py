import psycopg2
import bcrypt

def initialize_database():
    conn = psycopg2.connect(
        dbname="postgres",
        user="my_user",
        password="123",
        host="localhost",
        port="7777" 
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE my_database_users5")
    cursor.close()

    
    conn.close()
    conn = psycopg2.connect(
        dbname="my_database_users5",
        user="my_user",
        password="123",
        host="localhost",
        port="7777"  
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(500) UNIQUE NOT NULL,
            password VARCHAR(1000) NOT NULL,
            account_type VARCHAR(50) NOT NULL
        )
    """)
    
    
    hashed_password = bcrypt.hashpw("123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    cursor.execute("INSERT INTO users (username, password, account_type) VALUES (%s, %s, %s)", ("123", hashed_password, "basic"))
    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    initialize_database()
    print("Baza danych została zainicjowana.")
