#!/bin/sh

# @see https://gist.github.com/cdown/1163649

urlencode() {
  # urlencode <string>
  old_lc_collate=$LC_COLLATE
  LC_COLLATE=C
    
  local length="${#1}"
  for (( i = 0; i < length; i++ )); do
    local c="${1:i:1}"
    case $c in
      [a-zA-Z0-9.~_-]) printf "$c" ;;
      #*) printf '%%%02X' "'$c" ;;
      *) printf "$c" | xxd -p -c1 | while read x;do printf "%%%s" "$x"; done;
    esac
  done
    
  LC_COLLATE=$old_lc_collate
}

urldecode() {
  # urldecode <string>

  local url_encoded="${1//+/ }"
  printf '%b' "${url_encoded//%/\\x}"
}

echo 10

while true
do
  read line
  line=`urldecode "${line}"`

  # Do stuff

  output="é èûœ¨bœå"
  echo `urlencode "${output}"`
done
