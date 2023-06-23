#if not defined(DEDE_ID)
#define DEDE_ID id
#endif
#if not defined(DEDE_ID_TYPE)
#define DEDE_ID_TYPE bigint
#endif

#if defined(:pilote) and :pilote == "pgsql"
#set :SCHEMA `select current_schema`
#endif
