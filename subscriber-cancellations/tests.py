import os
import pandas as pd
import main
import unittest
import sqlite3

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.con = sqlite3.connect(':memory:')
        self.cur = self.con.cursor()
        
        # Create test tables
        self.cur.executescript('''
        CREATE TABLE cademycode_students (
            uuid INTEGER PRIMARY KEY,
            name TEXT,
            dob TEXT,
            sex TEXT,
            contact_info TEXT,
            job_id INTEGER,
            num_course_taken INTEGER,
            current_career_path_id INTEGER,
            time_spent_hrs REAL
        );
        
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
        ''')
        self.con.commit()
        
    def tearDown(self):
        self.con.close()
        if os.path.exists('test.csv'):
            os.remove('test.csv')

    def test_connect_to_db(self):
        con = main.connect_to_db(':memory:')
        self.assertIsInstance(con, sqlite3.Connection)

    def test_read_table(self):
        con_old = main.connect_to_db('dev/cademycode.db')

        students = pd.read_sql_query('SELECT * FROM cademycode_students', con_old)
        students_test = main.read_table(con_old, 'cademycode_students')
        self.assertTrue(students.equals(students_test))

        courses = pd.read_sql_query('SELECT * FROM cademycode_courses', con_old)
        courses_test = main.read_table(con_old, 'cademycode_courses')
        self.assertTrue(courses.equals(courses_test))

        jobs = pd.read_sql_query('SELECT * FROM cademycode_student_jobs', con_old)
        jobs_test = main.read_table(con_old, 'cademycode_student_jobs')
        self.assertTrue(jobs.equals(jobs_test))
    
    def test_clean_dataframes(self):
        students_df = pd.DataFrame({
            'contact_info': ['{"mailing_address:123 Main St", "email:test@example.com"}'],
            'name': ['John Doe'],
            'job_id': ['1'],
            'num_course_taken': ['5'],
            'current_career_path_id': ['2'],
            'time_spent_hrs': ['10.5'],
            'dob': ['2000-01-01']
        })

        courses_df = pd.DataFrame({
            'career_path_id': [2],
            'career_path_name': ['Engineering'],
            'hours_to_complete': [100]
        })

        jobs_df = pd.DataFrame({
            'job_id': [1],
            'job_category': ['Software Development'],
            'avg_salary': [80000]
        })

        cleaned_students_df, _, _ = main.clean_dataframes(students_df, courses_df, jobs_df)

        self.assertIsInstance(cleaned_students_df, pd.DataFrame)
        self.assertEqual(cleaned_students_df['first_name'][0], 'John')
        self.assertEqual(cleaned_students_df['email'][0], 'test@example.com')

    def test_reorganize_student(self):
        df = pd.DataFrame({
            'uuid': [1],
            'first_name': ['John'],
            'last_name': ['Doe'],
            'dob': ['2000-01-01'],
            'sex': ['M'],
            'mailing_address': ['123 Main St'],
            'email': ['test@example.com'],
            'job_id': [1],
            'num_course_taken': [5],
            'current_career_path_id': [2],
            'time_spent_hrs': [10.5]
        })

        df_reorganized = main.reorganize_student(df)
        self.assertEqual(list(df_reorganized.columns), ['uuid', 'first_name', 'last_name', 'dob', 'sex', 'mailing_address', 'email', 'job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs'])

    def test_validate_foreign_keys(self):
        students_df = pd.DataFrame({
            'job_id': [1, 2],  # 2 is invalid
            'current_career_path_id': [2, 3]  # 3 is invalid
        })

        jobs_df = pd.DataFrame({'job_id': [1]})
        courses_df = pd.DataFrame({'career_path_id': [2]})

        test_FK = main.validate_foreign_keys(students_df, courses_df, jobs_df)
        self.assertFalse(next(test_FK))

    def test_update_database(self):
        students_df = pd.DataFrame({
            'uuid': [1],
            'first_name': ['John'],
            'last_name': ['Doe'],
            'dob': ['2000-01-01'],
            'sex': ['M'],
            'mailing_address': ['123 Main St'],
            'email': ['test@example.com'],
            'job_id': [1],
            'num_course_taken': [5],
            'current_career_path_id': [2],
            'time_spent_hrs': [10.5]
        })

        courses_df = pd.DataFrame({
            'career_path_id': [2],
            'career_path_name': ['Engineering'],
            'hours_to_complete': [100]
        })

        jobs_df = pd.DataFrame({
            'job_id': [1],
            'job_category': ['Software Development'],
            'avg_salary': [80000]
        })

        result = main.update_database(self.con, students_df, courses_df, jobs_df)
        self.assertIsInstance(result, dict)
        self.assertEqual(result['students'], 1)
        self.assertEqual(result['courses'], 1)
        self.assertEqual(result['student_jobs'], 1)

    def test_load_csv(self):
        students_df = pd.DataFrame({
            'uuid': [1],
            'first_name': ['John'],
            'last_name': ['Doe'],
            'dob': ['2000-01-01'],
            'sex': ['M'],
            'mailing_address': ['123 Main St'],
            'email': ['test@example.com'],
            'job_id': [1],
            'num_course_taken': [5],
            'current_career_path_id': [2],
            'time_spent_hrs': [10.5]
        })

        courses_df = pd.DataFrame({
            'career_path_id': [2],
            'career_path_name': ['Engineering'],
            'hours_to_complete': [100]
        })

        jobs_df = pd.DataFrame({
            'job_id': [1],
            'job_category': ['Software Development'],
            'avg_salary': [80000]
        })

        main.update_database(self.con, students_df, courses_df, jobs_df)
        result = main.load_to_csv(self.con, 'test.csv')
        self.assertEqual(result, 1)
                    
if __name__ == '__main__':
    unittest.main()