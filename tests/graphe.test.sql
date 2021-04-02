#include ../graphe.sql

#format delim \t

select graphe from graphe('{{4,4,4,5.25,6.5,7,8,7.5,7},{3,3.5,4,3,2,2.2,2.5,3.2,4}}', '{0,ANSI}');
