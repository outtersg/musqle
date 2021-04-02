#include ../graphe.sql

#format delim \t

select graphe from graphe('{{4,4,4,5.25,6.5,7,8,7.5,7},{3,3.5,4,3,2,2.2,2.5,3.2,4}}', '{0,ANSI}');

create temporary table t_graphe as
	select *
	from
	(values
		('oui', 0, 4),
		('oui', 1, 6),
		('oui', 2, 8),
		('oui', 3, 5),
		('oui', 4, 2),
		('non', 0, 4),
		('non', 1, 3),
		('non', 2, 2),
		('non', 3, 3.5),
		('non', 4, 2)
	) t(groupe, x, y);
select graphe from graphe('t_graphe', 'groupe', 'x', 'y', '{0,ANSI}');
