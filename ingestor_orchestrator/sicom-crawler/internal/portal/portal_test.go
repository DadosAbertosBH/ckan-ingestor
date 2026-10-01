// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package portal

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestParseSICOMYearsRatherThanCAPMG(t *testing.T) {
	got, e := ParseYears(`this.listaExercicioCAPMG=[{descExercicio:"2018",id:1}];this.listaExercicio=[{descExercicio:"2026",id:13},{descExercicio:"2014",id:11},{descExercicio:"2026",id:1}]`)
	if e != nil || !reflect.DeepEqual(got, []int{2026, 2014}) {
		t.Fatalf("years=%v err=%v", got, e)
	}
	if _, e := ParseYears("changed bundle"); e == nil {
		t.Fatal("expected changed portal diagnostic")
	}
	if got := ParseBearer(`captchaPublicUrl:"url",bearer:"public-app-token"`); got != "public-app-token" {
		t.Fatalf("bearer=%s", got)
	}
}
func TestAuthorizedListingRenewsOnceAndIncludesOrigin(t *testing.T) {
	calls, auth := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/buscarCategoriaDownload" || r.URL.Query().Get("origem") != "SICOM" || r.URL.Query().Get("exercicio") != "2026" {
			t.Errorf("request=%s", r.URL)
		}
		if r.Header.Get("Authorization") != "Bearer app" || r.Header.Get("Origin") != "https://dadosabertos.tce.mg.gov.br" {
			t.Errorf("headers=%v", r.Header)
		}
		if calls == 1 {
			w.WriteHeader(401)
			return
		}
		if r.Header.Get("AuthorizationProxy") != "token renewed" {
			t.Errorf("renewed header=%s", r.Header.Get("AuthorizationProxy"))
		}
		fmt.Fprint(w, `[{"seqZip":109822,"categoria":"Despesas com pessoal","qtdArquivos":853,"tamanho":"1 MB"}]`)
	}))
	defer server.Close()
	c := &Client{HTTP: server.Client(), BaseURL: server.URL + "/", Authenticate: func(context.Context) (Session, error) {
		auth++
		token := "old"
		if auth == 2 {
			token = "renewed"
		}
		return Session{ProxyToken: token, Bearer: "app", Years: []int{2026}}, nil
	}}
	if _, e := c.Years(context.Background()); e != nil {
		t.Fatal(e)
	}
	got, e := c.List(context.Background(), 2026)
	if e != nil || len(got) != 1 || got[0].Year != 2026 || got[0].SeqZip != 109822 {
		t.Fatalf("categories=%v error=%v", got, e)
	}
	if auth != 2 || calls != 2 {
		t.Fatalf("auth=%d calls=%d", auth, calls)
	}
}
func TestOriginErrorsRedactTokens(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403); fmt.Fprint(w, "secret-token") }))
	defer server.Close()
	c := &Client{HTTP: server.Client(), BaseURL: server.URL + "/", Authenticate: func(context.Context) (Session, error) {
		return Session{ProxyToken: "secret-token", Bearer: "app", Years: []int{2026}}, nil
	}}
	_, err := c.List(context.Background(), 2026)
	if err == nil || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("error=%v", err)
	}
}
