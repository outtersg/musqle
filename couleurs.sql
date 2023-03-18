-- Les séquences ANSI sont définies sous forme d'une concaténation, pour que la définition elle-même ne comporte pas la séquence d'échappement;
-- ainsi les requêtes contenant ces séquences ne sont pas éligibles au regard du terminal (si pour diagnostic on affiche les requêtes),
-- seul le résultat du select sera pris en compte.

#define BLANC  ''||'[0m'

#define GRAS   ''||'[1m'

#define ROUGE  ''||'[31m'
#define VERT   ''||'[32m'
#define JAUNE  ''||'[33m'
#define BLEU   ''||'[34m'
#define VIOLET ''||'[35m'
#define CYAN   ''||'[36m'
#define GRIS   ''||'[90m'
#define ROSE   ''||'[95m'
