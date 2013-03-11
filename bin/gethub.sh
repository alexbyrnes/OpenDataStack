


# Parse Parameters #
for ARG in $*; do
  case $ARG in
   -a=*|--api=*)
      api=${ARG#*=}
      ;;
   *)
      infile=$ARG
      ;;
  esac
done

  
echo "["

while read p; do
    curl ${api}${p} | python filter_open_json.py
    echo -ne ",\n"  
done < $infile

echo "]"


