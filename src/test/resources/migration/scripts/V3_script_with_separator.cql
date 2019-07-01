INSERT INTO table VALUES(1, '2,45 and ;', 4);
INSERT INTO table VALUES(1, '2,45 and', 4)   ;
INSERT INTO table
VALUES(1, '2,45 and', 4)  ;
-- last insert
INSERT INTO
table
(c1, c2, c3)
values
('qwe; & kdij',
 1,
 'othe;era');

SELECT * FROM t1
JOIN t2 ON t1.a == t2.b
WHERE ;