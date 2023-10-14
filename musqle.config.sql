#if not defined(DEDE_ID)
#define DEDE_ID id
#endif
#if not defined(DEDE_ID_TYPE)
#if defined(:pilote) and :pilote == "pgsql"
#define DEDE_ID_TYPE bigint
#else
#define DEDE_ID_TYPE integer
#endif
#endif

#if defined(DEDE_SCHEMA) and not defined(:SCHEMA)
#define :SCHEMA DEDE_SCHEMA
#endif
#if defined(:pilote) and :pilote == "pgsql" and not defined(:SCHEMA)
#set :SCHEMA `select current_schema`
#endif
#if defined(:SCHEMA) and not defined(DEDE_SCHEMA)
#define DEDE_SCHEMA :SCHEMA
#endif

#if defined(DEDE_SCHEMA)
#define LOCAL(t) DEDE_SCHEMA.t
#else
#define LOCAL(t) t
#endif
