package main

import (
	"flag"
	"log"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/executor"
	"github.com/myuon/mylite/server"
	"github.com/myuon/mylite/storage"
)

func main() {
	addr := flag.String("addr", ":3307", "listen address")
	flag.Parse()

	cat := catalog.New()
	store := storage.NewEngine()
	exec := executor.New(cat, store)
	srv := server.New(exec, *addr)

	log.Printf("Starting mylite on %s", *addr)
	if err := srv.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
