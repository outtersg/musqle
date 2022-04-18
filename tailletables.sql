-- À FAIRE: gérer d'autres BdD, définir une requête vide pour celles qui ne savent pas remontées nos infos.

#if defined(TT_VUE)
#define @TT_VUE create or replace temporary view tailletables as
#else
#define @TT_VUE
#endif

#include tailletables.pg.sql
