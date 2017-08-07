>code_rating.txt
for i in `ls -1 *.py`
do
log_file_base=`echo $i|sed 's/\.py//g'`
log_file=$log_file_base'.log'
>$log_file
pylint $i >> $log_file
>$log_file_base'.final'
grep ^C: $log_file >> $log_file_base'.final'
grep ^W: $log_file >> $log_file_base'.final'
grep ^R: $log_file >> $log_file_base'.final'
rating=`grep "Your code has been rated at" $log_file|tr -s ' ' '~'|awk -F~ '{print $7}'` 
echo "Rating for file ${log_file_base}.py is : ${rating}" >> code_rating.txt
if [[ -s $log_file_base'.final' ]] ; then
echo "There are coding standards issue with the code for source ${log_file_base}.py"
else
echo "No issues found for source ${log_file_base}.py"
fi ;
done;
