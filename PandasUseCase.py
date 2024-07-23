import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from urllib.parse import quote_plus


class Main:
    def __init__(self):
        self.df_deptDetails = pd.read_json("D:\\GCPEngineer\\Python\\DeptDetails.json")
        self.df_salaryBand = pd.read_excel("D:\\GCPEngineer\\Python\\SalaryBand.xlsx", sheet_name='Sheet1')
    def run(self):
        # creating object to the Usecase
        usecase = UseCase()
        emp_details_df = usecase.GetEmpDetails()
        top_salaried_emps_df,salary_Grade_emps = usecase.GetLocationWiseTopSalariedEmps(emp_details_df, self.df_deptDetails)
        usecase.WriteToCSV(top_salaried_emps_df, "top_salaried_employees.csv")
        usecase.WriteToCSV(salary_Grade_emps, "emp.csv")
        df_emps_withSalaryBands = usecase.GetSalaryBandsForEnps(emp_details_df,self.df_salaryBand)
        usecase.WriteToCSV(df_emps_withSalaryBands, "emp_salary_bands.csv")



class UseCase:
    def GetEmpDetails(self):
        connn_str = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=DESKTOP-TMIQ2ND\\SQLEXPRESS;"
            "Database=UseCase_Task;"
            "Uid=sa;"
            "Pwd=sa123;"
        )
        server="DESKTOP-TMIQ2ND\\SQLEXPRESS"
        database = "UseCase_Task"
        Uid="sa"
        pwd="sa123"
        encoded_pwd = quote_plus(pwd)
        conn_str = f"mssql+pyodbc://{Uid}:{encoded_pwd}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

        engine = create_engine(conn_str)
        query = "SELECT * FROM Emp"
        emp_df = pd.read_sql_query(query,engine)
        print(emp_df)
        return emp_df

    def GetLocationWiseTopSalariedEmps(self, emp_df, dept_df):
        # Merge the dataframes to get the location of each employee
        merged_df = emp_df.merge(dept_df, on="DEPTNO")

        # Group by location and find the top salaried employee for each location
        top_salaried_emps_df = merged_df.loc[merged_df.groupby("dept_loc")["SALARY"].idxmax()]
        selected_columns = top_salaried_emps_df[['ID', 'NAME', 'SALARY', 'DEPTNO']]
        return top_salaried_emps_df, selected_columns

    def WriteToCSV(self, df, file_name):
        df.to_csv(file_name, index=False)
        print(f"Data written to {file_name}")
    def GetSalaryBandsForEnps(self, emp_df, salaryBand_df):
        emp_df['key'] = 1
        salaryBand_df['key'] = 1
        # Merge emp with salary_band based on emp_salary
        merged_df = pd.merge(emp_df, salaryBand_df, on='key').drop('key', axis=1)
        df_filtered = merged_df[(merged_df['SALARY'] >= merged_df['salary_lower_bound']) &
                                (merged_df['SALARY'] <= merged_df['salary_upper_bound'])]
        filtered_df = df_filtered[['ID', 'SALARY', 'DEPTNO','band']]
        print(filtered_df)

        return filtered_df

if __name__ == "__main__":
    main = Main()
    main.run()
