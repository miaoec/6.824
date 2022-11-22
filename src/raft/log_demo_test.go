package raft

import (
	"os"
	"testing"
)

func TestTestLog(t *testing.T) {
	os.Args = []string{"say", "hello"}
	TestLog1SingleSpan()
}

func TestTestLog2(t *testing.T) {
	os.Args = []string{"say", "hello"}
	TestLog2MultiSpan()
}

func TestTestLog3(t *testing.T) {
	os.Args = []string{"say", "hello"}
	//TestLog3MultiService()
}
