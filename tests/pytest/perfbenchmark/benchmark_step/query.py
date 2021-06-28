import taos
import datetime

host = '192.168.1.86'

conn = taos.connect(host="192.168.1.86", user="root", password="taosdata", config="/etc/taos")
c1 = conn.cursor()

c1.execute('use db')

print('running query: select * from stb')
begin = datetime.now()
c1.execute('select * from stb;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select count(*) , max(col1), avg(col1) from stb;')
begin = datetime.now()
c1.execute('select count(*) , max(col1), avg(col1) from stb')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select * from stb where t1 = xx or t1 = xx 30%')
begin = datetime.now()
c1.execute('select * from stb where t0 = 1 or t0 = 2 or t0 = 3;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select * from stb where t1 = xx or t1 = xx 70%')
begin = datetime.now()
c1.execute('select * from stb where t0 = 1 or t0 = 2 or t0 = 3 or t0 = 4 or t0 = 5 or t0 = 6 or t0 = 7;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select * from stb where t1 = xx or t1 = xx 100%')
begin = datetime.now()
c1.execute('select count(*) from stb where t0 = 1 or t0 = 2 or t0 = 3 or t0 = 4 or t0 = 5 or t0 = 6 or t0 = 7 or t0 = 8 or t0 = 9 or t0 = 10;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select * from stb where t1 = xx and t0 = xx 30%')
begin = datetime.now()
c1.execute('select count(*) from stb where t1 = \'beijing\' and t2 = \'china\';')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select * from stb where t1 = xx and t0 = xx 70%')
begin = datetime.now()
c1.execute('select count(*) from stb where (t1 = \'beijing\' and t2 = \'china\') or (t1 = \'shenzhen\' and t2 = \'china\');')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select * from stb where t1 = xx and t0 = xx 100%')
begin = datetime.now()
c1.execute('select count(*) from stb where (t1 = \'beijing\' and t2 = \'china\') or (t1 = \'shenzhen\' and t2 = \'china\') or (t1 = \'shanghai\' and t2 = \'china\');')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')


print('running query: select count(*) , max(col1), avg(col1) from stb where t1 = xx or t1 = xx 30%')
begin = datetime.now()
c1.execute('select count(*) , max(col1), avg(col1) from stb where t0 = 1 or t0 = 2 or t0 = 3;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select count(*) , max(col1), avg(col1) from stb where t1 = xx or t1 = xx 70%')
begin = datetime.now()
c1.execute('select count(*) , max(col1), avg(col1) from stb where t0 = 1 or t0 = 2 or t0 = 3 or t0 = 4 or t0 = 5 or t0 = 6 or t0 = 7;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select count(*) , max(col1), avg(col1) from stb where t1 = xx or t1 = xx 100%')
begin = datetime.now()
c1.execute('select count(*) , max(col1), avg(col1) from stb where t0 = 1 or t0 = 2 or t0 = 3 or t0 = 4 or t0 = 5 or t0 = 6 or t0 = 7 or t0 = 8 or t0 = 9 or t0 = 10;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select count(*),max(col1),avg(col1) from stb group by tbname')
begin = datetime.now()
c1.execute('select count(*),max(col1),avg(col1) from stb group by tbname;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select count(*),max(col1),avg(col1) from stb group by t1')
begin = datetime.now()
c1.execute('select count(*),max(col1),avg(col1) from stb group by t1;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')


print('running query: select count(*) , max(col1), avg(col1) from stb where t1 = xx or t1 = xx group by tbname 30%')
begin = datetime.now()
c1.execute('select count(*) , max(col1), avg(col1) from stb where t0 = 1 or t0 = 2 or t0 = 3 group by tbname;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select count(*) , max(col1), avg(col1) from stb where t1 = xx or t1 = xx group by tbname 70%')
begin = datetime.now()
c1.execute('select count(*) , max(col1), avg(col1) from stb where t0 = 1 or t0 = 2 or t0 = 3 or t0 = 4 or t0 = 5 or t0 = 6 or t0 = 7 group by tbname;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')

print('running query: select count(*) , max(col1), avg(col1) from stb where t1 = xx or t1 = xx group by tbname 100%')
begin = datetime.now()
c1.execute('select count(*) , max(col1), avg(col1) from stb where t0 = 1 or t0 = 2 or t0 = 3 or t0 = 4 or t0 = 5 or t0 = 6 or t0 = 7 or t0 = 8 or t0 = 9 or t0 = 10 group by tbname;')
result = c1.fetchall()
end = datetime.now()
print(f'time used ={end - begin}')