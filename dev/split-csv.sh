# $1: file to split
# $2: number of lines per part
# $3: directory to write parts to
if [[ -z $1 || -z $2 || -z $3 ]]; then
  echo 'You need to pass 3 variables.'
  exit 1
fi
echo "splitting csv in parts by $2 lines each, to '$3/'..."

set -e
# see https://stackoverflow.com/questions/1411713/how-to-split-a-file-and-keep-the-first-line-in-each-of-the-pieces
mkdir -p "$3"
tail -n +2 $1 | split -l $2 - part-
for file in part-*
do
    head -n 1 $1 > tmp_file
    cat "$file" >> tmp_file
    mv -f tmp_file "$3/$file.csv"
    rm "$file"
done
ls -l "$3/"
echo "splitting completed."
