package raft

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/yurishkuro/opentracing-tutorial/go/lib/tracing"
	"log"
	"net/http"
	"os"
)

func TestLog1SingleSpan() {
	if len(os.Args) != 2 {
		panic("ERROR: Expecting one argument")
	}

	tracer, closer := tracing.Init("hello-world")
	defer closer.Close()

	helloTo := os.Args[1]

	span := tracer.StartSpan("say-hello")
	span.SetTag("hello-to", helloTo)

	helloStr := fmt.Sprintf("Hello, %s!", helloTo)
	span.LogFields(
		otlog.String("event", "string-format"),
		otlog.String("value", helloStr),
	)

	println(helloStr)
	span.LogKV("event", "println")

	span.Finish()
}

func TestLog2MultiSpan() {

	formatString := func(ctx context.Context, helloTo string) string {
		span, _ := opentracing.StartSpanFromContext(ctx, "formatString")
		defer span.Finish()

		helloStr := fmt.Sprintf("Hello, %s!", helloTo)
		span.LogFields(
			otlog.String("event", "string-format"),
			otlog.String("value", helloStr),
		)

		return helloStr
	}

	printHello := func(ctx context.Context, helloStr string) {
		span, _ := opentracing.StartSpanFromContext(ctx, "printHello")
		defer span.Finish()

		println(helloStr)
		span.LogKV("event", "println")
	}

	if len(os.Args) != 2 {
		panic("ERROR: Expecting one argument")
	}

	tracer, closer := tracing.Init("hello-world")
	defer closer.Close()
	opentracing.SetGlobalTracer(tracer)

	helloTo := os.Args[1]

	span := tracer.StartSpan("say-hello")
	span.SetTag("hello-to", helloTo)
	defer span.Finish()

	ctx := opentracing.ContextWithSpan(context.Background(), span)

	helloStr := formatString(ctx, helloTo)
	printHello(ctx, helloStr)
}

func makePublish(tracer opentracing.Tracer) func(http.ResponseWriter, *http.Request) {
	//tracer, closer := tracing.Init("publisher")
	return func(w http.ResponseWriter, r *http.Request) {
		spanCtx, err := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
		if err != nil {
			log.Fatal(err)
		}
		span := tracer.StartSpan("publish", ext.RPCServerOption(spanCtx))
		defer span.Finish()
		log.Print(span)
		helloStr := r.FormValue("helloStr")
		log.Print("l:" + helloStr)
		println(helloStr)
	}
	//http.HandleFunc(
	//	"/publish",
	//)
	//log.Fatal(http.ListenAndServe(":1222", nil))
}
func makeFormatter(tracer opentracing.Tracer) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		spanCtx, err := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
		if err != nil {
			log.Fatal(err)
		}
		span := tracer.StartSpan("format", ext.RPCServerOption(spanCtx))
		//defer
		log.Print(span)
		helloTo := r.FormValue("helloTo")
		helloStr := fmt.Sprintf("Hello, %s!", helloTo)
		span.LogFields(
			otlog.String("event", "string-format"),
			otlog.String("value", helloStr),
		)
		log.Print("l:" + helloStr)
		w.Write([]byte(helloStr))
		span.Finish()
	}
}
