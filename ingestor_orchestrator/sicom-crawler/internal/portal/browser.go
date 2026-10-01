// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package portal

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	sicomidentity "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity"
)

const LoginURL = "https://arabiasaudita.tce.mg.gov.br:8443/TCEMG-proxy-web/login/captcha.jsf?sistema=DADOS_ABERTOS&url=https://dadosabertos.tce.mg.gov.br/"
const DownloadPage = sicomidentity.DownloadPageURL

type Browser struct {
	CDPURL  string
	HTTP    *http.Client
	Timeout time.Duration
}

func (b *Browser) Authenticate(ctx context.Context) (Session, error) {
	timeout := b.Timeout
	if timeout == 0 {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	allocator, closeAllocator := chromedp.NewRemoteAllocator(ctx, b.CDPURL)
	defer closeAllocator()
	tab, closeTab := chromedp.NewContext(allocator, chromedp.WithLogf(func(string, ...any) {}), chromedp.WithErrorf(func(string, ...any) {}))
	defer closeTab()
	var mu sync.Mutex
	scripts := map[string]bool{}
	chromedp.ListenTarget(tab, func(event any) {
		if e, ok := event.(*network.EventResponseReceived); ok && e.Type == network.ResourceTypeScript {
			u, err := url.Parse(e.Response.URL)
			if err == nil && u.Hostname() == "dadosabertos.tce.mg.gov.br" {
				mu.Lock()
				scripts[e.Response.URL] = true
				mu.Unlock()
			}
		}
	})
	if err := chromedp.Run(tab, network.Enable(), chromedp.Navigate(sicomidentity.PortalURL)); err != nil {
		return Session{}, errors.New("CDP portal navigation failed")
	}
	if err := chromedp.Run(tab, chromedp.Evaluate(`localStorage.clear()`, nil), chromedp.Navigate(LoginURL)); err != nil {
		return Session{}, errors.New("CDP authentication navigation failed")
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	token := ""
	for token == "" {
		select {
		case <-ctx.Done():
			return Session{}, errors.New("portal authentication needs intervention or exceeded timeout; complete CAPTCHA in an interactive CDP browser and retry")
		case <-ticker.C:
			// Navigations may temporarily remove the execution context; keep waiting for the portal redirect.
			_ = chromedp.Run(tab, chromedp.Evaluate(`location.hostname === 'dadosabertos.tce.mg.gov.br' ? (localStorage.getItem('tokenRequestAuthorizationProxy') || localStorage.getItem('tokenAuthorizationProxy') || '') : ''`, &token))
		}
	}
	// Load the SICOM lazy bundle even when Angular animations are unsupported by Lightpanda.
	_ = chromedp.Run(tab, chromedp.Evaluate(`localStorage.setItem('filtroCategorias',JSON.stringify({origem:'SICOM',exercicio:new Date().getFullYear()}))`, nil), chromedp.Navigate(DownloadPage))
	session := Session{ProxyToken: token}
	fetched := map[string]bool{}
	for len(session.Years) == 0 || session.Bearer == "" {
		select {
		case <-ctx.Done():
			return Session{}, errors.New("portal scripts changed: could not discover SICOM years and application credentials")
		case <-ticker.C:
			mu.Lock()
			urls := []string{}
			for u := range scripts {
				if !fetched[u] {
					urls = append(urls, u)
				}
			}
			mu.Unlock()
			sort.Strings(urls)
			for _, u := range urls {
				req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
				if err != nil {
					continue
				}
				res, err := b.HTTP.Do(req)
				if err != nil {
					continue
				}
				data, readErr := io.ReadAll(io.LimitReader(res.Body, 16<<20))
				_ = res.Body.Close()
				if readErr != nil || res.StatusCode != 200 {
					continue
				}
				fetched[u] = true
				script := string(data)
				if session.Bearer == "" {
					session.Bearer = ParseBearer(script)
				}
				if len(session.Years) == 0 {
					session.Years, _ = ParseYears(script)
				}
			}
		}
	}
	// Proxy authorization may have rotated while loading the download page.
	var current string
	if err := chromedp.Run(tab, chromedp.Evaluate(`localStorage.getItem('tokenRequestAuthorizationProxy') || ''`, &current)); err == nil && strings.TrimSpace(current) != "" {
		session.ProxyToken = current
	}
	return session, nil
}
