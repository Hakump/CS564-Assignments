select Title, Authors
from Book
ORDER BY Title collate NOCASE;

select count(*) Book;

select Title, count(*), max(Due)
from Book, Records
where Book.ID == Records.BID
group by Book.ID;

create table temp (
    ID int primary key,
    NAME text not null,
    KU int unique,
    BID foreign key references Book
    check ( ID > 0 & ID < 1)
)