rm -rf batch_test_tmp
mkdir batch_test_tmp
for i in {1..2}
  do go test -run TestFigure82C > batch_test_tmp/batch_run_figure8_testlog_$i &
  done
wait
cat batch_test_tmp/batch_run_figure8_testlog_* |grep "FAIL:" -A 3
