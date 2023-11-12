# Copyright (c) 2023 Guillaume Outters
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

case "$_UTIL_MUSQLE_SH_" in "")

#- Environnement ---------------------------------------------------------------

configBdd()
{
	# Historiquement oraParams a été montée avant configBdd, on l'intègre donc.
	if command -v oraParams > /dev/null 2>&1
	then
		oraParams "$@" && BDD_TYPE=oracle && return 0 || true
	fi
	
	echo "# Configuration introuvable pour la base '$1'" >&2
	return 1
}

#- Utilitaires -----------------------------------------------------------------

miamParam()
{
	case "$2" in "") return 1 ;; esac
	eval "$2=\$1" ; shift 2
	params="$*"
}

commande()
{
	command -v "$*" 2> /dev/null >&2
}

# Utilisation: dernier <var> 1 2 3 … n
# la variable <var> aura en sortie la valeur "n"
dernier()
{
	unset IFS
	eval $1'="$'$#\"
}

# Utilisation: paire <var1> <var2> 1 2 3 … n
# <var1> aura en sortie la valeur "1", <var2> vaudra "2 3 … n"
# N.B.: "paire" signifie Premier Argument à Isoler du Reste par Extraction
paire()
{
	unset IFS
	eval $1='"$3"'
	local var2="$2"
	shift 3 || true
	eval $var2='"$*"'
}

_UTIL_MUSQLE_SH_=1 ;; esac
