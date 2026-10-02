// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package portal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
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

var ErrCAPTCHAIntervention = errors.New("portal displayed an interactive image CAPTCHA; complete it in a visual CDP browser using --interactive and retry")

// Redirect URLs may contain authorization tokens. Preserve the cause while
// keeping URLs and JWTs out of the error displayed by the worker.
var browserErrorCredentials = regexp.MustCompile(`(?:https?|wss?)://\S+|eyJ[A-Za-z0-9_.-]+`)

type browserNavigationError struct {
	stage string
	cause error
}

func (e *browserNavigationError) Error() string {
	return fmt.Sprintf("CDP %s navigation failed: %s", e.stage, browserErrorCredentials.ReplaceAllString(e.cause.Error(), "[redacted]"))
}

func (e *browserNavigationError) Unwrap() error { return e.cause }

type Browser struct {
	CDPURL      string
	HTTP        *http.Client
	Timeout     time.Duration
	Interactive bool
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
		return Session{}, &browserNavigationError{stage: "portal", cause: err}
	}
	if err := chromedp.Run(tab, chromedp.Evaluate(`localStorage.clear()`, nil), chromedp.Navigate(LoginURL)); err != nil {
		return Session{}, &browserNavigationError{stage: "authentication", cause: err}
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	token := ""
	for token == "" {
		select {
		case <-ctx.Done():
			return Session{}, fmt.Errorf("portal authorization token was not received: %w", ctx.Err())
		case <-ticker.C:
			// Navigations may temporarily remove the execution context; keep waiting for the portal redirect.
			var state struct {
				Token         string `json:"token"`
				ManualCAPTCHA bool   `json:"manualCaptcha"`
			}
			if err := chromedp.Run(tab, chromedp.Evaluate(`(() => {
				if (location.hostname === 'dadosabertos.tce.mg.gov.br') {
					return {token: localStorage.getItem('tokenRequestAuthorizationProxy') || localStorage.getItem('tokenAuthorizationProxy') || '', manualCaptcha: false};
				}
				const panel = document.getElementById('painelCaptchaImagem');
				return {token: '', manualCaptcha: !!panel && !panel.classList.contains('ui-helper-hidden') && !!panel.querySelector('input[type="text"]')};
			})()`, &state)); err != nil {
				continue
			}
			if state.ManualCAPTCHA && !b.Interactive {
				return Session{}, ErrCAPTCHAIntervention
			}
			token = state.Token
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
