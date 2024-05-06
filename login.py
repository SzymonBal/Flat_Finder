import streamlit as st
import psycopg2
import hashlib

def hash_password(password):
    # Użycie funkcji SHA-256 do haszowania hasła
    hashed_password = hashlib.sha256(password.encode()).hexdigest()
    return hashed_password

def login(input_username, input_password):
    conn = psycopg2.connect(
        dbname="my_database_users5",
        user="my_user",
        password="123",
        host="my_database_users",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = %s AND password = %s", (input_username, hash_password(input_password)))
    user = cursor.fetchone()  # Zwraca pierwszy pasujący rekord
    cursor.close()
    conn.close()
    if user:
        account_type = user[3]
        return True, user, account_type
    return False, None, None

def signup(input_username, input_password, account_type):
    conn = psycopg2.connect(
        dbname="my_database_users5",
        user="my_user",
        password="123",
        host="my_database_users",
        port="5432"
    )
    cursor = conn.cursor()

    hashed_password = hash_password(input_password)
    cursor.execute("INSERT INTO users (username, password, account_type) VALUES (%s, %s, %s)", (input_username, hashed_password, account_type))
    conn.commit()
    cursor.close()
    conn.close()
    return True

def main():
    st.sidebar.title("Panel logowania")
    choice = st.sidebar.radio("Wybierz akcję", ("Logowanie", "Rejestracja"))

    if choice == "Logowanie":
        st.title("Logowanie")
        username = st.text_input("Nazwa użytkownika")
        password = st.text_input("Hasło", type="password")
        if st.button("Zaloguj"):
            authenticated, user, account_type = login(username, password)
            if authenticated:
                st.success("Zalogowano pomyślnie!")
                st.write("Witaj, ", user[1])
                st.markdown('[Przejdź do aplikacji](http://localhost:8501/?is_logged_in=True&account_type=' + account_type + ')')
            else:
                st.error("Nieprawidłowa nazwa użytkownika lub hasło")
                st.write("Sprawdź, czy dane logowania są poprawne:", username, password)

    elif choice == "Rejestracja":
        st.title("Rejestracja")
        username = st.text_input("Nazwa użytkownika")
        password = st.text_input("Hasło", type="password")
        account_type = st.selectbox('Account type',('basic', 'premium'))
        if st.button("Zarejestruj"):
            if signup(username, password, account_type):
                st.success("Rejestracja udana!")
                st.write("Możesz teraz się zalogować.")
            else:
                st.error("Nazwa użytkownika jest już zajęta")

if __name__ == "__main__":
    main()
