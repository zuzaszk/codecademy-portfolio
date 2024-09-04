import sqlite3
import pandas as pd
import logging



logger = logging.getLogger()
logger.setLevel(logging.INFO)

changelog_handler = logging.FileHandler('dev/changelog.log')
error_handler = logging.FileHandler('dev/error.log')


changelog_handler.setLevel(logging.INFO)
error_handler.setLevel(logging.ERROR)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

changelog_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)

logger.addHandler(changelog_handler)
logger.addHandler(error_handler)

def connect_to_db(db_name):
    """Connect to the database."""
    try:
        con = sqlite3.connect(db_name)
        logging.info(f'Connected to {db_name} successfully')
        return con
    except Exception as e:
        logging.error(f'Error connecting to {db_name}')
        logging.error(e)
        return None

# def fetch_tables(con):
#     """Fetch tables from the database."""
#     try:
#         cur = con.cursor()
#         tables = cur.execute('''SELECT * FROM sqlite_master''').fetchall()
#         logging.info('Fetched tables successfully')
#         return tables
#     except Exception as e:
#         logging.error('Error fetching tables')
#         logging.error(e)
#         return None

# def fetch_columns(con, table_name):
#     """Fetch columns from a table in the database."""
#     try:
#         cur = con.cursor()
#         columns = cur.execute(f'''PRAGMA table_info({table_name})''').fetchall()
#         logging.info(f'Fetched columns from {table_name} successfully')
#         return columns
#     except Exception as e:
#         logging.error(f'Error fetching columns from {table_name}')
#         logging.error(e)
#         return None

def read_table(con, table_name):
    """Read a table from the database."""
    try:
        df = pd.read_sql_query(f'SELECT * FROM {table_name}', con)
        logging.info(f'Read {table_name} successfully')
        return df
    except Exception as e:
        logging.error(f'Error reading {table_name}')
        logging.error(e)
        return None

def clean_dataframes(students_df, courses_df, jobs_df):
    """Clean and process the dataframes."""
    try:
        students_df = students_df.drop_duplicates()
        courses_df = courses_df.drop_duplicates()
        jobs_df = jobs_df.drop_duplicates()

        students_df['mailing_address'] = students_df['contact_info'].apply(lambda x: x.split('", ')[0])
        students_df['email'] = students_df['contact_info'].apply(lambda x: x.split('", ')[1])
        students_df.drop('contact_info', axis=1, inplace=True)

        students_df['mailing_address'] = students_df['mailing_address'].apply(lambda x: x.replace('{', '').replace('}', '').replace('"', '').replace('mailing_address:', ''))
        students_df['email'] = students_df['email'].apply(lambda x: x.replace('{', '').replace('}', '').replace('"', '').replace('email:', ''))

        numeric_columns = ['job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs']
        for col in numeric_columns:
            students_df[col] = pd.to_numeric(students_df[col], errors='coerce')
            if col != 'time_spent_hrs':
                students_df[col] = students_df[col].astype('Int64')

        students_df['dob'] = pd.to_datetime(students_df['dob'], errors='coerce').dt.date

        students_df['first_name'] = students_df['name'].apply(lambda x: x.split()[0])
        students_df['last_name'] = students_df['name'].apply(lambda x: ' '.join(x.split()[1:]))
        students_df.drop('name', axis=1, inplace=True)

        logging.info("Dataframes cleaned successfully.")
        return students_df, courses_df, jobs_df
    except Exception as e:
        logging.error(f"Error cleaning dataframes")
        logging.error(e)
        return None, None, None

def reorganize_student(df):
    """Reorganize the student dataframe."""
    try:
        df = df[['uuid', 'first_name', 'last_name', 'dob', 'sex', 'mailing_address', 'email', 'job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs']]
        logging.info("Student dataframe reorganized successfully.")
        return df
    except Exception as e:
        logging.error(f"Error reorganizing student dataframe")
        logging.error(e)
        return None

def validate_foreign_keys(students_df, courses_df, jobs_df):
    """Validate foreign keys to ensure data integrity."""
    students_jobs_keys = jobs_df['job_id'].unique()
    courses_keys = courses_df['career_path_id'].unique()

    students_jobs_fk = students_df['job_id'].unique()
    courses_fk = students_df['current_career_path_id'].unique()

    # Check if foreign keys are in primary keys
    for key in students_jobs_fk:
        if pd.notna(key) and key not in students_jobs_keys:
            logging.warning(f'Foreign key {key} not in primary keys of jobs table.')
            yield False

    for key in courses_fk:
        if pd.notna(key) and key not in courses_keys:
            logging.warning(f'Foreign key {key} not in primary keys of courses table.')
            yield False


def update_database(con, students_df, courses_df, jobs_df):
    """Update the SQLite database with cleaned data."""
    try:
        cur = con.cursor()

        # Drop existing tables
        cur.execute('''DROP TABLE IF EXISTS cademycode_students''')
        cur.execute('''DROP TABLE IF EXISTS cademycode_courses''')
        cur.execute('''DROP TABLE IF EXISTS cademycode_student_jobs''')

        # Create new tables
        table_creation = '''
        CREATE TABLE cademycode_courses (
            career_path_id INTEGER PRIMARY KEY,
            career_path_name TEXT,
            hours_to_complete INTEGER
        );

        CREATE TABLE cademycode_student_jobs (
            job_id INTEGER PRIMARY KEY,
            job_category TEXT,
            avg_salary INTEGER
        );

        CREATE TABLE cademycode_students (
            uuid INTEGER PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            dob DATE,
            sex VARCHAR,
            mailing_address VARCHAR,
            email TEXT,
            job_id INTEGER REFERENCES cademycode_student_jobs(job_id),
            num_course_taken INTEGER,
            current_career_path_id INTEGER REFERENCES cademycode_courses(career_path_id),
            time_spent_hrs REAL
        );'''

        cur.executescript(table_creation)
        logging.info("Tables created successfully.")

        # Insert data into the new tables
        for _, row in courses_df.iterrows():
            cur.execute('INSERT INTO cademycode_courses VALUES (?, ?, ?)', tuple(row))

        for _, row in jobs_df.iterrows():
            cur.execute('INSERT INTO cademycode_student_jobs VALUES (?, ?, ?)', tuple(row))

        for _, row in students_df.iterrows():
            cur.execute('INSERT INTO cademycode_students VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tuple(row))

        con.commit()
        logging.info("Database updated successfully.")

        return {
            'courses': len(courses_df),
            'student_jobs': len(jobs_df),
            'students': len(students_df)
        }

    except Exception as e:
        logging.error(f"Error updating the database")
        logging.error(e)
        return None
        

def load_to_csv(con, file_name):
    """Load a dataframe to a CSV file."""
    try:
        join_query = '''
        SELECT s.uuid, s.first_name, s.last_name, s.dob, s.sex, s.mailing_address, s.email, 
            s.num_course_taken, s.time_spent_hrs,
            j.job_id, j.job_category, j.avg_salary,
            c.career_path_id, c.career_path_name, c.hours_to_complete
        FROM cademycode_students s
        LEFT JOIN cademycode_student_jobs j ON s.job_id = j.job_id
        LEFT JOIN cademycode_courses c ON s.current_career_path_id = c.career_path_id;
        '''
        df = pd.read_sql_query(join_query, con)
        df.to_csv(file_name, index=False)
        logging.info(f"Dataframe loaded to {file_name} successfully.")
        return len(df)
    except Exception as e:
        logging.error(f"Error loading dataframe to {file_name}")
        logging.error(e)
        return None

def main():
    con1 = connect_to_db('dev/cademycode.db')
    if con1:
        students_df = read_table(con1, 'cademycode_students')
        courses_df = read_table(con1, 'cademycode_courses')
        jobs_df = read_table(con1, 'cademycode_student_jobs')
        if students_df is not None and courses_df is not None and jobs_df is not None:
            students_df, courses_df, jobs_df = clean_dataframes(students_df, courses_df, jobs_df)
            students_df = reorganize_student(students_df)
            validate_foreign_keys(students_df, courses_df, jobs_df)

            con2 = connect_to_db('dev/cademycode_updated.db')
            if con2:
                update_database(con2, students_df, courses_df, jobs_df)
                load_to_csv(con2, 'dev/cademycode_updated.csv')

                con2.commit()
                con2.close()
        con1.close()


if __name__ == '__main__':
    main()