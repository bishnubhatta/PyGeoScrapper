for i in `ls -lart .*.py`
do
log_file_base=`echo $i|sed 's/\.py//g'`
log_file=$log_file_base+'.log'
pylint $i > $log_file
done

for j in `ls -lart *.log`
do
grep ^C: $j > $log_file_base+'.final'
done

for FILE in `ls -lart *.final`
do
if [[ -s $FILE ]] ; then
echo "There are coding standards issue with the code for source ${log_file_base}.py"
else
echo "No issues found for source ${log_file_base}.py"
fi ;

