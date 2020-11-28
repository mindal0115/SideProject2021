create table if not exists SideProject2021.company(
	ticker char(50),
    simfinid int,
    companyname char(250),
    industryid int
);

create table if not exists SideProject2021.industry(
	industryid int,
    sector char(100),
    industry char(100)
);

create table if not exists SideProject2021.income(
	ticker char(50),
    reportdate datetime,
    simfinid int,
    currency char(10),
    fiscalyear int,
    fiscalperiod char(10),
    publishdate datetime,
    restateddate datetime,
    share_basic double,
    share_diluted double,
    url char(255),
    variable char(100),
    value double 
);

create table if not exists SideProject2021.priceratio(
	ticker char(50),
    d_date datetime,
    simfinid int,
    variable char(100),
    value double 
);

create table if not exists SideProject2021.price(
	ticker char(50),
    d_date datetime,
    simfinid int,
    open float,
    low float,
    high float,
    close float,
    adj_close float,
    divd float,
    volume float,
    shares double
);
