rm -rf batch_test_tmp
mkdir batch_test_tmp

function dotest() {
  go test -run TestPersist12C >batch_test_tmp/batch_run_figure8_testlog_$1_$2_$$
}

export -f dotest
# 再开一个sh启动函数

for ((i = 0; i < $1; i++)); do
  echo "start Test "$i"/"$1
  for ((j = 0; j < $2; j++)); do
    sh -c "dotest "$i" "$j &
  done
  wait
  echo "end Test "$i"/"$1
  grep -rn "FAIL" -B 2 batch_test_tmp/* >../failed.log
  echo "Fail Count:"$(grep -o "FAIL: Test" batch_test_tmp/* | wc -l)"/"$j
done





# ps -ef | grep TestFigure82C | awk '{print $2}' | xargs kill -9
