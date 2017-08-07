ls *.log *.final|while read file
do
zip review_log.zip  $file
rm $file
done
