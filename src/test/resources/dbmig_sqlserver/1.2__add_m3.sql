create table m3
(
    id   integer,
    acol varchar(20),
    bcol timestamp
);

insert into m3 (id, acol)
VALUES (1, 'text with ; sign'); -- plus some comment
