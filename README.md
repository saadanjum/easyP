# EasyP

A simple, light weight and easy to use Python wrapper for PostgreSQL client `psycopg2`. Avoid the hassle of writing SQL queries and then executing them. With easyP you can do database operations in a functional manner.

## Usage
Create instance and connect to database
```
from easyP import easyP
db = easyP(HOST='localhost', DB='moviesdb', USERNAME='postgres', PASSWORD='somepassword')
db.connect(encoding='utf-8')
```

### Select
```
response = db.select(table='movies',
                     select=['col1', 'col2'],
                     where={
                         'col1': 'abc',
                         'col2': '>= 10'
                     },
                     orderBy=[col1, col2],
                     limit=100,
                     offset=100)
```
Every operational call returns the following response. Where you can find the actual rows returned in rusults array.
```
{
    'rowcount': rowcount,
    'results': results,
    'status': status,
    'error': error
}
```

You can add distinct on column clause as follows
```
response = db.select(table='movies',
                     distinctOn='col1',
                     select=['col1', 'col2', 'col3']
                     limit=10)
```
Select **count(*)** can be performed by not providing any select columns
```
response = db.select(table='movies')
```

### Where
Select, Update and Delete operations provide ability to add a filter using the where clause. Following are some examples
#### Simple equality comparison
You can pass a key, value pair dictionary as where object where each key represents a column and its value is used for comparison.
```
response = db.select(table='movies',
                     select=['col1', 'col2'],
                     where={
                         'col1': 'abc',
                         'col2': 'xyz'
                     })
```
This would make the following SQL query
```
SELECT col1, col2 FROM movies WHERE col1 = abc AND col2 = xyz;
```

#### Expression based comparison
SQL expressions can be processed directly to value of a column for other types of comparisons. For example
```
response = db.select(table='movies',
                     select=['col1', 'col2'],
                     where={
                         'col1': '>= 1',
                         'col2': '<= 10'
                     })
```
```
response = db.select(table='movies',
                     select=['col1', 'col2'],
                     where={
                         'col1': "LIKE 'abc%'"
                     })
```
```
response = db.select(table='movies',
                     select=['col1', 'col2'],
                     where={
                         'col1': 'IN (1, 2, 3, 4)'
                     })
```
### Group By
Group by can be applied in the query as follows
```
response = db.select(table='movies', 
                    select=[
                        'vote_average', 
                        'count(*)'
                    ],
                    groupBy=[
                        'vote_average'
                    ])
```

### Order By
Order by can be applied by providing the name of column in a list along with the direction.
```
response = db.select(table='movies',
                    select=['*'], 
                    orderBy=['vote_average asc', 'budget desc'],
                    limit=10)
```

