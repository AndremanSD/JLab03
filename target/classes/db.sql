CREATE TABLE hdd (
                         id serial primary key not null ,
                         name varchar(30),
                         capacity int,
                         inch varchar(30),
                         rpm varchar(10),
                         connface varchar(100),
                         buffer int
);

CREATE TABLE ssd (
                         id serial primary key not null ,
                         name varchar(30),
                         capacity int,
                         inch varchar(30),
                         controller varchar(50),
                         memtype varchar(10),
                         readSpeed int,
                         writeSpeed int
);