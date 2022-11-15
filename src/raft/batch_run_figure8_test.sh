rm -rf batch_test_tmp
mkdir batch_test_tmp
for i in {1..30}
  do go test -run TestFigure82C > batch_test_tmp/batch_run_figure8_testlog_$i
  done
cat batch_test_tmp/batch_run_figure8_testlog_* |grep "FAIL:" -A 3 > failed.log

# ps -ef | grep TestFigure82C | awk '{print $2}' | xargs kill -9

