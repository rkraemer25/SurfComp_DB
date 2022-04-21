import mysql.connector
from time import time

# note:  look at at end of this file - mydb.commit() and mydb.close() -> do not forget
# MUST commit the changes!!!!  (if you did any inserts, deletes, updates, load data.... )

print ("\nHello - starting createAndLoadSurfCompDB.py\n")

mydb = mysql.connector.connect(
  user='root',    # could be root, or a user you created, I created 'testuser2'
  passwd='password',  # the password for that use
  database='SurfCompDB',   # the database to connect to
  host='127.0.0.1',   # localhost
  allow_local_infile=1  # needed so can load local files
)

print(mydb)
myc = mydb.cursor()   # myc name short for "my cursor"

# We need to reset the variable that allows loading of local files
myc.execute('set global local_infile = 1')

# myc.execute ("show databases")  # this returns a list in myc that you can iterate over
# for x in myc:
# 	print(x)

myc.execute ("use SurfCompDB")

# drop procedures
myc.execute("drop procedure if exists add_Surfer ;")

# drop relation tables
myc.execute("drop table if exists PairsWith ;")
myc.execute("drop table if exists WorksFor ;")
myc.execute("drop table if exists Host ;")
myc.execute("drop table if exists Competes ;")

# drop entity tables
myc.execute("drop table if exists Surfer ;")
myc.execute("drop table if exists Surfboard ;")
myc.execute("drop table if exists Shaper ;")
myc.execute("drop table if exists Sponsor ;")
myc.execute("drop table if exists Event ;")


## Creat tables

myc.execute("""
create table Surfer(
  surferID int,
  name varchar(20) NOT NULL,
  age int,
  country varchar(20),
  weight float,
  height float,
  rating int NOT NULL,
  primary key (surferID)
);
""")

myc.execute("""
create table Surfboard(
  modelNum int,
  name varchar(20) NOT NULL,
  type varchar(20),
  length float,
  volume float,
  MaxWaveSize int,
  shaperID int,
  primary key (modelNum, shaperID)
);
""")

myc.execute("""
create table Shaper(
  shaperID int,
  name varchar(20) NOT NULL,
  company varchar(20),
  country varchar(20),
  primary key (shaperID)
);
""")

myc.execute("""
create table Sponsor(
  sponsorID int,
  name varchar(20) NOT NULL,
  company varchar(20),
  primary key (sponsorID)
);
""")

myc.execute("""
create table Event(
  eventNum int,
  title varchar(20) NOT NULL,
  day date NOT NULL,
  startTime time NOT NULL,
  location varchar(20) NOT NULL,
  ratingNeeded int,
  waveSize float,
  winner varchar(20),
  primary key (eventNum)
);
""")

myc.execute("""
create table PairsWith(
  surferID int,
  modelNum int,
  primary key (surferID, modelNum),
  foreign key (surferID) references Surfer(surferID),
  foreign key (modelNum) references Surfboard(modelNum)
);
""")

myc.execute("""
create table WorksFor(
  shaperID int,
  sponsorID int,
  salary float,
  primary key (shaperID, sponsorID),
  foreign key (shaperID) references Shaper(shaperID),
  foreign key (sponsorID) references Sponsor(sponsorID)
);
""")

myc.execute("""
create table Host(
  sponsorID int,
  eventNum int,
  primary key (sponsorID, eventNum),
  foreign key (sponsorID) references Sponsor(sponsorID),
  foreign key (eventNum) references Event(eventNum)
);
""")

myc.execute("""
create table Competes(
  surferID int,
  eventNum int,
  modelNum int,
  primary key (surferID, eventNum, modelNum),
  foreign key (surferID) references Surfer(surferID),
  foreign key (eventNum) references Event(eventNum),
  foreign key (modelNum) references Surfboard(modelNum)
);
""")

## load data into tables
print("\nLoading data into tables...\n")
# load surfer
myc.execute("""
  load data local infile '/Users/rbkraemer/Documents/DU/Winter_2022/comp_3421/SurfCompProj/data_surfer' into table Surfer
  fields terminated by ','
  lines terminated by '\n' ;
""")
# load surfboard
myc.execute("""
  load data local infile '/Users/rbkraemer/Documents/DU/Winter_2022/comp_3421/SurfCompProj/data_surfboard' into table Surfboard
  fields terminated by ','
  lines terminated by '\n' ;
""")
# load shaper
myc.execute("""
  load data local infile '/Users/rbkraemer/Documents/DU/Winter_2022/comp_3421/SurfCompProj/data_shaper' into table Shaper
  fields terminated by ','
  lines terminated by '\n' ;
""")
# load sponsor
# load event
# load PairsWith
myc.execute("""
  load data local infile '/Users/rbkraemer/Documents/DU/Winter_2022/comp_3421/SurfCompProj/data_PairsWith' into table PairsWith
  fields terminated by ','
  lines terminated by '\n' ;
""")
# load WorksFor
# load Host
# load Competes

myc.execute("show databases;")
for x in myc:
    print(x)



    

# # # # # # # # # #
# Assignment 7    #
# # # # # # # # # #
# print("""\n------------------ Assignment 7 ------------------\n
# 1) foreign keys are added in the create table statements above. Adding
# update that violates the foreign key constraint.\n
# insert into PairsWith values (1000001, 1);\n
# """)
# try:
#     myc.execute("""
#     insert into PairsWith values (1000001, 1);
#     """)
#     for x in myc:
#         print(x)
# except:
#     print("""Cannot add or update a child row: a foreign key constraint
#             fails (`surfcompdb`.`pairswith`, CONSTRAINT `pairswith_ibfk_1`
#             FOREIGN KEY (`surferID`) REFERENCES `surfer` (`surferID`))
#             """)
#
# print("""\n
# 2) create procedure that takes in attributes for surfer and adds a surfer to
# the Surfer Table if the surferID is not already in use and returns the total
# number of surfers.\n
# delimiter //
# create procedure add_Surfer(IN surferID int, IN name varchar(20), IN age int,
# IN country varchar(20), IN weight float, IN height float, IN rating int,
# OUT total int)
# begin
#   IF surferID not in (select S.surferID from Surfer S) THEN
#      insert into Surfer values(surferID, name, age, country, weight, height, rating);
#      select 'SURFER ADDED!' as '';
#      select count(*) from Surfer into total;
#   ELSE
#      select 'SURFER ID ALREADY EXISTS, CANNOT BE ADDED!' as '';
#      select count(*) from Surfer into total;
#   END IF;
# end //
# delimiter ;\n
# """)
# myc.execute("""
# create procedure add_Surfer(IN surferID int, IN name varchar(20), IN age int,
# IN country varchar(20), IN weight float, IN height float, IN rating int,
# OUT total int)
# begin
#   IF surferID not in (select S.surferID from Surfer S) THEN
#      insert into Surfer values(surferID, name, age, country, weight, height, rating);
#      select 'SURFER ADDED!' as '';
#      select count(*) from Surfer into total;
#   ELSE
#      select 'SURFER ID ALREADY EXISTS, CANNOT BE ADDED!' as '';
#      select count(*) from Surfer into total;
#   END IF;
# end
# """)
# for x in myc:
#     print(x)
#
# print("\nCount:\n")
# myc.execute("""select count(*) from Surfer;""")
# for x in myc:
#     print(x)
#
# myc.execute("""
# show tables;
# """)
# for x in myc:
#     print(x)
#
# print("""\n
# Running IF in add_Surfer procedure.\n
# call add_Surfer(10001, 'Rob', 25, 'USA', 140, 67, 4, @T);\n
# """)
# myc.execute("""
# call add_Surfer(10001, 'Rob', 25, 'USA', 140, 67, 4, @T);
# """)
# myc.execute("""
# show tables;
# """)
# for x in myc.fetchall():
#     print(x)
#
#
#
#
# print("""\n
# Running ELSE in add_Surfer procedure.\n
# call add_Surfer(10001, 'Rob', 25, 'USA', 140, 67, 4, @T);\n
# """)
# myc.execute("""
# call add_Surfer(10001, 'Rob', 25, 'USA', 140, 67, 4, @T);
# select @T as 'total';
# """)
# for x in myc:
#     print(x)
#
#
# #
# # QUESTION 3
# #
# print("""\n
# 3) load large data and check run times for indexed and non indexed tables\n
# """)
#
# myc.execute("drop table if exists PairsWith ;")
# myc.execute("drop table if exists Surfer ;")
# myc.execute("""
# create table Surfer(
#   surferID int,
#   name varchar(20) NOT NULL,
#   age int,
#   country varchar(20),
#   weight float,
#   height float,
#   rating int NOT NULL,
#   primary key (surferID)
# );
# """)
# myc.execute("""
# create table PairsWith(
#   surferID int,
#   modelNum int,
#   primary key (surferID, modelNum),
#   foreign key (surferID) references Surfer(surferID),
#   foreign key (modelNum) references Surfboard(modelNum)
# );
# """)
# myc.execute("""
#   load data local infile '/Users/rbkraemer/Documents/DU/Winter_2022/comp_3421/SurfCompProj/data_surfer_lg' into table Surfer
#   fields terminated by ','
#   lines terminated by '\n' ;
# """)
# myc.execute("""
#   load data local infile '/Users/rbkraemer/Documents/DU/Winter_2022/comp_3421/SurfCompProj/data_PairsWith' into table PairsWith
#   fields terminated by ','
#   lines terminated by '\n' ;
# """)
#
# print("\nselect count(*) from Surfer S where S.age < 50\n")
# tic = time()
# myc.execute("""select count(*) from Surfer S where S.age < 50""")
# toc = time()
# for x in myc:
#     print(x)
# print(f"Run time: {toc - tic}")
#
# print("\nselect count(*) from Surfer S where S.age < 50\n")
# tic = time()
# myc.execute("""select count(*) from Surfer S where S.age < 50""")
# toc = time()
# for x in myc:
#     print(x)
# print(f"Run time: {toc - tic}")
#
# myc.execute("drop table if exists PairsWith ;")
# myc.execute("drop table if exists Surfer ;")
# myc.execute("""
# create table Surfer(
#   surferID int,
#   name varchar(20) NOT NULL,
#   age int,
#   country varchar(20),
#   weight float,
#   height float,
#   rating int NOT NULL,
#   index(age)
#   primary key (surferID)
# );
# """)
# myc.execute("""
# create table PairsWith(
#   surferID int,
#   modelNum int,
#   primary key (surferID, modelNum),
#   foreign key (surferID) references Surfer(surferID),
#   foreign key (modelNum) references Surfboard(modelNum)
# );
# """)
#
# myc.execute("""
#   load data local infile '/Users/rbkraemer/Documents/DU/Winter_2022/comp_3421/SurfCompProj/data_surfer_lg' into table Surfer
#   fields terminated by ','
#   lines terminated by '\n' ;
# """)
# myc.execute("""
#   load data local infile '/Users/rbkraemer/Documents/DU/Winter_2022/comp_3421/SurfCompProj/data_PairsWith' into table PairsWith
#   fields terminated by ','
#   lines terminated by '\n' ;
# """)
#
# print("\nselect count(*) from Surfer_lg, PairWith P where S.age < 50 and P.modelNum < 200")
# tic = time()
# myc.execute("""select count(*) from Surfer, PairWith P where S.age < 50 and P.modelNum < 200""")
# toc = time()
# for x in myc:
#     print(x)
# print(f"Run time: {toc - tic}")
#
# print("\nselect count(*) from Surfer S, PairsWith P where S.age < 50 and P.modelNum < 200\n")
# tic = time()
# myc.execute("""select count(*) from Surfer, PairWith P where S.age < 50 and P.modelNum < 200""")
# toc = time()
# for x in myc:
#     print(x)
# print(f"Run time: {toc - tic}")
#




# # # # # # # # # # #
# OLDER ASSIGNMENTS #
# # # # # # # # # # #
# ### Assignment 5
#
# # 1) Write three queries on your PDA database, using the select-from-where
# # construct of SQL.
# # # One must involve a two-way or three-way join with a where clause that
# # # limits the results to 20 of fewer tuples.
# print("""\nGet name and age of all the Surfers that use a surfboard shaped
# by shaperID = 1\n""")
# myc.execute("""select S.name, S.age
#     from Surfer S
#     join PairsWith P on S.surferID = P.surferID
#     join Surfboard B on P.modelNum = B.modelNum
#     where B.shaperID = 1;
# """)
# for x in myc:
#     print(x)
#
# # # One must be an aggregate using a group by clause.
# print("""\nFor each rating group, get average age of Surfers that PairWith
# with a gun\n""")
# myc.execute("""select S.rating, AVG(S.age)
#     from Surfer S
#     join PairsWith P on S.surferID = P.surferID
#     join Surfboard B on P.modelNum = B.modelNum
#     where B.type = 'gun'
#     group by S.rating
#     order by S.rating;
# """)
# for x in myc:
#     print(x)
#
# # # One must be an aggregate using a group by clause and a having clause
# print("""\nFor each rating group that has 2 or more Surfers, get minimum and
# average age for surfers over the age of 18\n""")
# myc.execute("""select S.rating, MIN(S.age), AVG(S.age)
#     from Surfer S
#     where S.age >= 18
#     group by S.rating
#     having count(*) >= 2
#     order by S.rating;
# """)
# for x in myc:
#     print(x)
#
# # 2) Write three data-modification commands on your PDA database.
# # # simple insert
# print("\nInsert into Surfer values (10001, 'Rob', 25, 'USA', 140, 67, 8)")
# myc.execute("""insert into Surfer values (10001, 'Rob', 25, 'USA', 140, 67, 4);""")
# print("\nPrint Surfer with id 10001\n")
# myc.execute("""select * from Surfer S where S.surferID = 10001;""")
# for x in myc:
#     print(x)
#
# # # simple update
# print("\nUpdate surferID 10001's rating to 8")
# myc.execute("""update Surfer set rating = 8 where surferID = 10001;""")
# print("\nPrint Surfer with id 10001\n")
# myc.execute("""select * from Surfer S where S.surferID = 10001;""")
# for x in myc:
#     print(x)
#
# # # update several tuples at once
# print("\nCount the number of longboards with a length less than 96 inches\n")
# myc.execute("""select count(*) from Surfboard B
#     where B.type = 'longboard' and B.length < 96;
# """)
# for x in myc:
#     print(x)
# print("\nUpdate length of longboards with a length less than 96 inches to 96\n")
# myc.execute("""update Surfboard set length = 96
#     where type = 'longboard' and length < 96;
# """)
# print("Count the number of longboards with a length less than 96 inches\n")
# myc.execute("""select count(*) from Surfboard B
#     where B.type = 'longboard' and B.length < 96;
# """)
# for x in myc:
#     print(x)


# ### Assignment 4
#
# # 1) Show tables
# ## show tables
# # print out which tables are in SurfCompDB
# print("\n1) Printing Tables in SurfCompDB...\n")
# myc.execute ("show tables")
# for x in myc:
# 	print(x)
#
# # 2) Describe Surfer, Surfboard, Shaper
# print("\n2) Describe Surfer, Surfboard, Shaper\n")
# print("\nDescribe Surfer...\n")
# myc.execute ("describe Surfer ;")
# for x in myc:
# 	print(x)
# print("\nDescribe Surfbaord...\n")
# myc.execute ("describe Surfboard ;")
# for x in myc:
# 	print(x)
# print("\nDescribe Shaper...\n")
# myc.execute ("describe Shaper ;")
# for x in myc:
# 	print(x)
#
# # 3) get count from Surfer, Surfboard, Shaper
# print("\n3) get count from Surfer, Surfboard, Shaper\n")
# print("\nCount Surfer...\n")
# myc.execute ("select count(*) from Surfer ;")
# for x in myc:
# 	print(x)
# print("\nCount Surfboard...\n")
# myc.execute ("select count(*) from Surfboard ;")
# for x in myc:
# 	print(x)
# print("\nCount Shaper...\n")
# myc.execute ("select count(*) from Shaper ;")
# for x in myc:
# 	print(x)
#
# # 4) the output of a select command on each table that returns less than 10 tuples
# print("\n4) the output of a select command on each table that returns less than 10 tuples\n")
# print("\nAfter loading Surfer:  select * from Surfer where surferID < 10 ...\n")
# myc.execute ("select * from Surfer where surferID < 10") ;
# for x in myc:
# 	print(x)
#
# print("\nAfter loading Surfbaord:  select * from Surfboard where modelNum < 10 ...\n")
# myc.execute ("select * from Surfboard where modelNum < 10") ;
# for x in myc:
# 	print(x)
#
# print("\nAfter loading Shaper:  select * from Shaper where shaperID < 10 ...\n")
# myc.execute ("select * from Shaper where shaperID < 10") ;
# for x in myc:
# 	print(x)

# MUST commit the changes!!!!  (if you did any inserts, deletes, updates, load data.... )
mydb.commit()
mydb.close()
