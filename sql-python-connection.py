from sqlalchemy import create_engine,MetaData,Table
engine=create_engine('sqlite:///census_nyc.sqlite')
connection=engine.connect()
metadata=MetaData()
census=Table('census',metadata,autoload=True,autoload_with=engine)
print(repr(census))
stmt='select * from people'
result_proxy=connection.execute(stmt)
results=result_proxy.fetchall()

#where clause implementation
stmt=select([census])
stmt=stmt.where(census.columns.state=='California')
results=connection.execute(stmt).fetchall()
print(results)
for result in results:
    print(result.state,result.age)

#where clause with or statement
from sqlalchemy import or_
stmt=select([census])
stmt=stmt.where(or_(census.columns.state=='California',census.columns.state=='New York'))  
for result in connection.execute(stmt):
    print(result.state,result.sex)
     
from sqlalchemy import func
stmt=select([func.sum(census.columns.pop2008)])
results=connection.execute(stmt).scalar()
print(results)     

# calculating differences
stmt=select([census.columns.age,(census.columns.pop2008-census.columns.pop2008-census.columns.pop2000).label('pop_change')])
stmt=stmt.group_by(census.columns.age)
stmt=stmt.order_by(desc('pop_change'))
stmt=stmt.limit(5)
results=connection.execute(stmt).fetchall()
print(results)

#example for case statement
from sqlalchemy import case
stmt=select([func.sum(case([(census.columns.state=='New York',census.columns.pop2008)],else=0))])
results=connection.execute(stmt).fetchall()
print(results)

#percentage example
from sqlalchemy import cast, case,Float
stmt=select([func.sum(case([(census.columns.state=='New York',census.columns.pop2008)],else_=0))]/cast(func.sum(census.columns.pop2008),Float*100).label("ny_percentage")])
results=connection.execute(stmt).fetchall()
print(results)

#select_from example
stmt=select([func.sum(census.columns.pop2000)])
stmt=stmt.select_from(census.join(state_fact,census.columns.state==state_fact.columns.name))
stmt=stmt.where(state_fact.columns.census_divison_name=='East South Central')
results=connection.execute(stmt).fetchall()
print(results)

#querying hierarchical data
managers=employees.alias()
stmt=select([managers.columns.name.label('manager'),employees.columns.name.label('employee')])
stmt=stmt.select_from(employees.join(managers,managers.columns.id==employees.columns.manager))
stmt=stmt.order_by(managers.columns.name)
print(connection.execute(stmt).fetchall())

#handling large result sets **offbeat
while more_results:
    partial_results=results_proxy.fetchmany(50)
    if partial_results==[]:
        more_results=False
    for row in partial_results:
        state_count[row.state]+=1
results_proxy.close()   
print(state_count)

#creating databases and tables
from sqlalchemy import (Table,Column,String,Integer,Decimal,Boolean)
employees=Table('employees',metadata,Column('id',Integer()),Column('name',String(255)),Column('Salary',Decimal),Column('active',Boolean))
metadata.create_all(engine)
engine.table_names()

#creating databases and tables with constraints
from sqlalchemy import (Table,Column,String,Integer,Decimal,Boolean)
employees=Table('employees',metadata,Column('id',Integer()),Column('name',String(255),unique =True,nullable=False),Column('Salary',Decimal),Column('active',Boolean))
# metadata.create_all(engine)
# engine.table_names()
employees.constraints

#inserting data(one row) into table
from sqlalchemy import insert
stmt=insert(employees).values(id=1,name='Jason',salary=1.00,active=True)
result_proxy=connection.execute(stmt)
print(result_proxy.rowcount)

#inserting multiple rows of data
from sqlalchemy import insert
stmt=insert(employees)
values_list=[{'id':2,'name':'Rebecca','salary':2.00,'active':True},{'id':3,'name':'Bob','salary':0.00,'active':False}]
result_proxy=connection.execute(stmt,values_list)
print(result_proxy.rowcount) #attribute to get the no.of rows

# loading csv data files
import pandas as pd
census_df = pd.read_csv("census.csv", header=None)
census_df.columns = ['state', 'sex', 'age', 'pop2000', 'pop2008']
census_df.to_sql(name="census", con=connection, if_exists="append", index=False)

#updating one row
from  sqlalchemy import update
stmt=update(employees)
stmt=stmt.where(employees.columns.id==3)
stmt=stmt.values(active=True)
result_proxy=connection.execute(stmt)
print(result_proxy.rowcount)

#updating multiple records
stmt=update(employees)
stmt=stmt.where(employees.columns.active==True)
stmt=stmt.values(active=False,salary=0.00)
result_proxy=connection.execute(stmt)
print(result_proxy.rowcount)

#corelated updates
new_salary=select([employees.columns.salary])
new_salary=new_salary.order_by(desc(employees.columns.salary))
new_salary=new_salary.limit(1)
stmt=update(employees)
stmt=stmt.values(salary=new_salary)
result_proxy=connection.execute(stmt)
print(result_proxy.rowcount)

#deleting data from a table
from sqlalchemy import delete
stmt_check=select([func.count(extra_employees.columns.id)])
connection.execute(stmt).scalar()
delete_stmt=delete(extra_employees)
result_proxy=connection.execute(delete_stmt)
result_proxy.rowcount

#deleting specific rows from table
stmt=delete(employees).where(employees.columns.id==3)
result_proxy=connection.execute(stmt)
result_proxy.rowcount

#dropping a table completely
#while dropping a table, data is not erased from the metadata until the python process is restarted!
extra_employees.drop(engine)
print(extra_employees.exists(engine))
#outputs False

#dropping all the tables
metadata.drop_all(engine)
engine.table_names()
#returns an empty list

#census case study
->preparing sqlalchemy and the database
from sqlalchemy import create_engine, MetaData
engine=create_engine('sqlite:///census_nyc.sqlite')
metadata=MetaData()

