CREATE TABLE films (
    code        char(5) CONSTRAINT firstkey PRIMARY KEY,
    title       varchar(40) NOT NULL,
    did         integer NOT NULL,
    date_prod   date,
    kind        varchar(10),
    len         interval hour to minute
);

CREATE SEQUENCE serial_did START 1;

CREATE TABLE distributors (
     did    integer PRIMARY KEY DEFAULT nextval('serial_did'),
     name   varchar(40) NOT NULL CHECK (name <> '')
);